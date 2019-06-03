/*
 * LFS.c
 *
 *  Created on: 8 abr. 2019
 *      Author: USUARIO
 */

#include "LFS.h"

#define PUERTOLFS "7879"
#define DirMontaje "../FS_LISSANDRA/"
#define DirTablas "../FS_LISSANDRA/Tablas/"
#define DirBloques "../FS_LISSANDRA/Bloques/"
#define DirMetadata "../FS_LISSANDRA/Metadata/"

int socket_sv;
int socket_cli;
int cantidadDeDumpeos = 0;
t_log* logger;
t_list* memtable;
int main(void) {

	int errorHandler;
	signal(SIGINT, finalizarEjecutcion);
	//signal(SIGSEGV, finalizarEjecutcion);

	socket_sv = levantarServidor(PUERTOLFS);
	socket_cli = aceptarCliente(socket_sv);
	type header;
	memtable = inicializarMemtable();
	logger = iniciar_logger();
	while (1) {
		header = leerHeader(socket_cli);
		tSelect* packSelect = malloc(sizeof(tSelect));
		tInsert* packInsert = malloc(sizeof(tInsert));
		tCreate* packCreate = malloc(sizeof(tCreate));

		switch (header) {
		case SELECT:
			desSerializarSelect(packSelect, socket_cli);
			packSelect->type = header;
			registro* reg = Select(packSelect->nombre_tabla, packSelect->key);

			free(packSelect);
			break;
		case INSERT:
			desSerializarInsert(packInsert, socket_cli);
			packInsert->type = header;
			printf(
					"recibi un una consulta INSERT de la tabla %s con le key %d y el value %s \n",
					packInsert->nombre_tabla, packInsert->key,
					packInsert->value);
			errorHandler = insertarEnMemtable(packInsert);

			if (errorHandler == todoJoya) {
				log_debug(logger, "se inserto bien");
			}
			dumpeoMemoria();
			free(packSelect);
			break;
		case CREATE:
			log_debug(logger, "LLEGO EL CREATE");
			desSerializarCreate(packCreate, socket_cli);
			packCreate->type = header;

			errorHandler = Create(packCreate->nombre_tabla,
					packCreate->consistencia, packCreate->particiones,
					packCreate->compaction_time);

			if (errorHandler == todoJoya) {
				log_debug(logger, "se creo bien");
			}

			free(packSelect);
			break;
		}
	}

//	t_log* logger = iniciar_logger();
//	t_config* metadata = config_create("metadata");

//	PRUEBA COMANDO CREATE

//	errorHandler = Create("TABLA1", 1, 4, 1);

//	PRUEBA COMANDO INSERT
//	errorHandler = INSERT("tables/TABLA1", 3, "Mi nombre es Lissandra", 1548421507);

//	PRUEBA COMANDO DROP
//	errorHandler = DROP("tables/TABLA1");

//	PRUEBA COMANDO SELECT
//	errorHandler = SELECT("tables/TABLA1", 6);

	if (errorHandler) {
		logeoDeErrores(errorHandler, logger);
	}

	log_destroy(logger);
	close(socket_cli);
	close(socket_sv);

	return 0;
}

//memtable
t_list* inicializarMemtable() {
	return list_create();
}
void imprimir_registro(registro* unreg) {
	printf("Encontre un %s", unreg->value);
	//log_debug(logger,unreg->value);
}
int insertarEnMemtable(tInsert *packinsert) {

	char* mi_ruta = string_new();
	char* tables = "/home/utnso/tp-2019-1c-UbUTNu/Operativos/LFS/tables/";
	string_append(&mi_ruta, tables);
	string_append(&mi_ruta, packinsert->nombre_tabla);
	log_debug(logger, mi_ruta);
	if (!verificadorDeTabla(mi_ruta)) {
		return noExisteTabla;
	}
	if (!existe_tabla_en_memtable(packinsert->nombre_tabla)) { //chequeo si existe la tabla en la memtable
		if (!agregar_tabla_a_memtable(packinsert->nombre_tabla)) { //si no existe la agrego
			return noSeAgregoTabla;
			log_debug(logger, "NO Existe tabla en memtable");
		}
	}
	log_debug(logger, "Existe tabla en memtable");

	registro* registro_insert = malloc(sizeof(registro));

	registro_insert->key = packinsert->key;
	log_debug(logger, string_itoa(registro_insert->key));

	registro_insert->timestamp = time(NULL); //argregar

	log_debug(logger, (packinsert->value));
	char* value = malloc(packinsert->value_long);
	strcpy(value, packinsert->value);
	registro_insert->value = malloc(strlen(value) + 1);
	strcpy(registro_insert->value, value);
	free(mi_ruta);
	return insertarRegistro(registro_insert, packinsert->nombre_tabla);
}
int insertarRegistro(registro* registro, char* nombre_tabla) {

	int es_tabla(t_tabla* tabla) {
		if (strcmp(tabla->nombreTabla, nombre_tabla) == 0) {
			return 1;
		}
		return 0;
	}
	t_tabla* tabla = (t_tabla*) list_find(memtable, (int) &es_tabla);
	//log_debug(logger,"Encontre esta tabla");
	//log_debug(logger,tabla->nombreTabla);
	list_add(tabla->registros, registro);
	return todoJoya;

}
int agregar_tabla_a_memtable(char* tabla) {
	t_tabla* tabla_nueva = malloc(sizeof(t_tabla));
	tabla_nueva->nombreTabla = string_new();

	strcpy(tabla_nueva->nombreTabla, tabla);

	tabla_nueva->registros = list_create();

	log_debug(logger, "Se Agrego la tabla");
	int cantidad_anterior;
	cantidad_anterior = memtable->elements_count;
	int indice_agregado = list_add(memtable, tabla_nueva);
	return indice_agregado + 1 > cantidad_anterior;

}
int existe_tabla_en_memtable(char* posible_tabla) {
	int es_tabla(t_tabla* UnaTabla) {
		if (strcmp(UnaTabla->nombreTabla, posible_tabla) == 0) {
			return 1;
		}
		return 0;
	}
	t_tabla* tabla_encontrada = (t_tabla*) list_find(memtable, (int) &es_tabla);

	if (tabla_encontrada) {
		log_debug(logger, "Existe  tabla en memtable");
		return 1;
	}
	log_debug(logger, "No existe tabla en memtable");
	return 0;
}

// APIs

int Create(char* NOMBRE_TABLA, char* TIPO_CONSISTENCIA, int NUMERO_PARTICIONES,
		int COMPACTATION_TIME) {
	char* ruta = string_new();
	string_append(&ruta, DirTablas);
	string_append(&ruta, NOMBRE_TABLA);
	// ---- Verifico que la tabla no exista ----
	if (!verificadorDeTabla(ruta) == 0)
		return tablaExistente;

	// ---- Creo directorio de la tabla ----
	if (mkdir(ruta, 0700) == -1)
		return carpetaNoCreada;

	// ---- Creo y grabo Metadata de la tabla ----
	if (crearMetadata(ruta, TIPO_CONSISTENCIA, NUMERO_PARTICIONES,
			COMPACTATION_TIME))
		return metadataNoCreada;

	// ---- Creo los archivos binarios ----
	crearBinarios(ruta, NUMERO_PARTICIONES);
	free(ruta);
	return todoJoya;
}

int Insert(char* NOMBRE_TABLA, int KEY, char* VALUE, int Timestamp) {
	int particiones;
	// ---- Verifico que la tabla exista ----
	if (verificadorDeTabla(NOMBRE_TABLA) != 0)
		return noExisteTabla;

	// ---- Obtengo la metadata ----
	particiones = buscarEnMetadata(NOMBRE_TABLA, "PARTITIONS");
	if (particiones < 0)
		return particiones;

	// ---- Verifico si existe memoria para dumpeo ----

	// ---- Manejo de Timestamp opcional ----

	// ---- Inserto datos en memoria temporal ----

	return todoJoya;
}

int Drop(char* NOMBRE_TABLA) {

	// ---- Verifico que la tabla exista ----
	if (verificadorDeTabla(NOMBRE_TABLA) != 0)
		return noExisteTabla;

	// ---- Elimino el directorio y todos los archivos ----
	if (borrarDirectorio(NOMBRE_TABLA) != 0)
		return tablaNoEliminada;
	return todoJoya;
}
registro* Select(char* NOMBRE_TABLA, int KEY) {
	int es_mayor(registro* unReg, registro* otroReg) {
		if (unReg->timestamp >= otroReg->timestamp) {
			return 1;
		}
		return 0;
	}
	char* ruta = string_new();
	string_append(&ruta, DirTablas);
	string_append(&ruta, NOMBRE_TABLA);
	if (!verificadorDeTabla(ruta))
		return noExisteTabla;
//1) busca en memtable
//2)busca en FS
//3) busca en TMP
// COMPARA
	registro* registro_select = malloc(sizeof(registro));
	t_list* registros = list_create();
	list_add_all(registros, selectEnMemtable(KEY, NOMBRE_TABLA));
	list_add(registros, SelectFS(ruta, KEY));
	//en algun momento agreagar tmp
	list_sort(registros, (int) &es_mayor);
	registro_select = list_get(registros, 0);
	log_debug(logger, registro_select->value);
	return registro_select;

}
t_list* selectEnMemtable(uint16_t key, char* tabla) {
	int es_tabla(t_tabla* UnaTabla) {
		if (strcmp(UnaTabla->nombreTabla, tabla) == 0) {
			return 1;
		}
		return 0;
	}
	int es_registro(registro* unregistro) {
		if (unregistro->key == key) {
			return 1;
		}
		return 0;
	}
	t_tabla* tablaSelect;
	t_list* list_reg = list_create();
	tablaSelect = list_find(memtable, (int) &es_tabla);

	if (tablaSelect != NULL) {

		list_add_all(list_reg,
				list_filter(tablaSelect->registros, (int) &es_registro));
	}
//	log_debug(logger, reg->value);
//	free(tabla);
	return list_reg; //devuelvo valor del select
}

registro* SelectFS(char* ruta, int KEY) {
	int particiones;

	// ---- Verifico que la tabla exista ----

	// ---- Obtengo la metadata ----
	particiones = buscarEnMetadata(ruta, "PARTITIONS");
	if (particiones < 0)
		return particiones;

	// ---- Calculo particion del KEY ----
	particiones = (KEY % particiones);
	string_append(&ruta, ("/"));
	string_append(&ruta, string_itoa(particiones));
	string_append(&ruta, ".bin");
	t_config* particion = config_create(ruta);
	int size = config_get_int_value(particion, "SIZE_BLOCKS");
	char** bloquesABuscar = config_get_array_value(particion, "BLOCKS");
	int i = 0;
	registro* registro = malloc(sizeof(registro));
	while (bloquesABuscar[i] != NULL) {
		char* bloque = string_new();
		string_append(&bloque, DirBloques);
		string_append(&bloque, bloquesABuscar[i]);
		string_append(&bloque, ".bin");
		int fd = open(bloque, O_RDONLY, S_IRUSR | S_IWUSR);
		struct stat s;
		int status = fstat(fd, &s);
		size = s.st_size;

		char* f = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);

		int j = 0;
		char** registros = string_split(f, "\n");
		while (registros[j] != NULL) {

			char** datos_registro = string_split(registros[j], ";");
			if (atoi(datos_registro[1]) == KEY) {
				//if (atol(datos_registro[0]) > registro->timestamp) {
				free(registro->value);
				registro->timestamp = atol(datos_registro[0]);
				registro->key = atoi(datos_registro[1]);
				registro->value = malloc(strlen(datos_registro[2]) + 1);
				strcpy(registro->value, datos_registro[2]);
				//}
			}

			string_iterate_lines(datos_registro, (void*) free);

			free(datos_registro);

			j++;
		}
		string_iterate_lines(registros, (void*) free);
		free(registros);
		close(fd);

		free(bloque);
		i++;
	}

	free(ruta);
	free(bloquesABuscar);

	config_destroy(particion);

	log_debug(logger, "Destrui la config particion");

	// ---- Escaneo particion objetivo ----
	//Escaneo en la memtable
	//Escaneo en archivos temporales

	// ---- Retorno la KEY de mayor Timestamp ----

	return registro;
}
/*
 int DESCRIBE (char* NOMBRE_TABLA,metadata *myMetadata){


 // ---- Verifico que la tabla exista ----
 if (verificadorDeTabla(NOMBRE_TABLA) != 0)
 return noExisteTabla;

 // ---- Obtengo la metadata ----
 myMetadata->particiones = buscarEnMetadata(NOMBRE_TABLA, "PARTITIONS");
 myMetadata->consistencia = buscarEnMetadata(NOMBRE_TABLA, "CONSISTENCY");
 myMetadata->tiempo_compactacion= buscarEnMetadata(NOMBRE_TABLA, "COMPACTION_TIME");

 if (myMetadata->particiones  < 0)
 return myMetadata->particiones;

 return 1;
 }

 int DESCRIBE(){
 struct dirent *dp;
 DIR *dir = opendir("tables");

 // Unable to open directory stream
 if (!dir)
 return -1;

 while ((dp = readdir(dir)) != 0){
 DESCRIBE(dp->d_name);
 }

 // Close directory stream
 closedir(dir);

 return todoJoya;
 }
 */

// Funciones de tabla
int crearMetadata(char* NOMBRE_TABLA, char* TIPO_CONSISTENCIA,
		int NUMERO_PARTICIONES, int COMPACTATION_TIME) {
	char aux[strlen(NOMBRE_TABLA) + 10];
	//Opitimizacion1A: Se puede hace que fp se pase por parámetro para no abrirlo en crearMetadata y crearBIN
	FILE *fp;
	snprintf(aux, sizeof(aux), "%s/metadata", NOMBRE_TABLA);
	fp = fopen(aux, "w+");
	if (fp == NULL)
		return -1;
	fprintf(fp, "CONSISTENCY=%s\n", TIPO_CONSISTENCIA);
	fprintf(fp, "PARTITIONS=%i\n", NUMERO_PARTICIONES);
	fprintf(fp, "COMPACTION_TIME=%i", COMPACTATION_TIME);
	fclose(fp);
	return todoJoya;
}

int crearBinarios(char* NOMBRE_TABLA, int NUMERO_PARTICIONES) {
	//Opitimizacion1B: Se puede hace que fp se pase por parámetro para no abrirlo en crearMetadata y crearBIN
	FILE *fp;
	char aux[strlen(NOMBRE_TABLA) + 10];
	while (NUMERO_PARTICIONES > 0) {
		snprintf(aux, sizeof(aux), "%s/%i.bin", NOMBRE_TABLA,
				NUMERO_PARTICIONES);
		fp = fopen(aux, "w+");
		if (!fp)
			return BINNoCreado;
		NUMERO_PARTICIONES -= 1;
	}
	fclose(fp);
	return todoJoya;
}

int verificadorDeTabla(char* NOMBRE_TABLA) {
	int status = 1;
	DIR *dirp;

	dirp = opendir(NOMBRE_TABLA);

	if (dirp == NULL) {
		status = 0;
	}
	closedir(dirp);

	return status;
}

int borrarDirectorio(const char *dir) {
	FTS* ftsp;
	FTSENT* curr;
	char* archivos[] = { (char *) dir, NULL };

	ftsp = fts_open(archivos, FTS_NOCHDIR | FTS_PHYSICAL | FTS_XDEV, NULL);
	if (!ftsp)
		return -1;
	while ((curr = fts_read(ftsp))) {
		switch (curr->fts_info) {
		case FTS_NS:
		case FTS_DNR:
		case FTS_ERR:
			return -1;
			break;

		case FTS_DP:
		case FTS_F:
		case FTS_SL:
		case FTS_SLNONE:
		case FTS_DEFAULT:
			if (remove(curr->fts_accpath) < 0)
				return -1;
			break;
		}
	}
	fts_close(ftsp);
	return todoJoya;
}

int buscarEnMetadata(char* NOMBRE_TABLA, char* objetivo) {
	char aux[40];
	FILE *fp;
	snprintf(aux, sizeof(aux), "%s/metadata", NOMBRE_TABLA);
	fp = fopen(aux, "r+");
	if (!fp)
		return noAbreMetadata;
	fgets(aux, 40, fp);
	while (strcmp(strtok(aux, "="), objetivo) != 0) {
		if (aux[strlen(aux) - 1] == '\0')
			return noExisteParametro;
		fgets(aux, 40, fp);
	}	// probar return
	return atoi(strtok(NULL, "="));
}

void dumpearTabla(t_tabla* UnaTabla) {
	char* tablaParaDumpeo = string_new();
	string_append(&tablaParaDumpeo, DirTablas);
	string_append(&tablaParaDumpeo, UnaTabla->nombreTabla);
	string_append(&tablaParaDumpeo, "/");
	string_append(&tablaParaDumpeo, string_itoa(cantidadDeDumpeos));
	string_append(&tablaParaDumpeo, ".tmp");
	int fd = open(tablaParaDumpeo, O_RDWR | O_CREAT | O_TRUNC, (mode_t) 0600);
	void dumpearRegistros(registro* UnRegistro) {
		char* registroParaEscribir = string_new();

		string_append(&registroParaEscribir,
				string_itoa(UnRegistro->timestamp));
		string_append(&registroParaEscribir, ";");
		string_append(&registroParaEscribir, string_itoa(UnRegistro->key));
		string_append(&registroParaEscribir, ";");
		string_append(&registroParaEscribir, UnRegistro->value);
		string_append(&registroParaEscribir, "\n");

		size_t textsize = strlen(registroParaEscribir) + 1; // + \0 null character

		lseek(fd, textsize - 1, SEEK_SET);
		/*
		 Something needs to be written at the end of the file to
		 * have the file actually have the new size.
		 * Just writing an empty string at the current file position will do.
		 *
		 * Note:
		 *  - The current position in the file is at the end of the stretched
		 *    file due to the call to lseek().
		 *  - An empty string is actually a single '\0' character, so a zero-byte
		 *    will be written at the last byte of the file.
		 */

		write(fd, "", 1);

		// Now the file is ready to be mmapped.
		char *map = mmap(0, textsize, PROT_READ | PROT_WRITE, MAP_SHARED, fd,
				0);

		size_t i = 0;
		while (i < textsize) {
			map[i] = registroParaEscribir[i];
			i++;
		}

		//memcpy(map, registroParaEscribir, strlen(registroParaEscribir));
		// Write it now to disk
		msync(map, textsize, MS_SYNC);
		munmap(map, textsize);
	}

	list_iterate(UnaTabla->registros, (void*) &dumpearRegistros);
	struct stat s;
	int status = fstat(fd, &s);

	//-----------------

	// Don't forget to free the mmapped memory

	// Un-mmaping doesn't close the file, so we still need to do that.
	close(fd);
	//-----------------

}

int dumpeoMemoria() {
	if (list_is_empty(memtable))
		return 1;	// poner define para memtable vacia
	cantidadDeDumpeos++;

	list_iterate(memtable, &dumpearTabla);
	/*



	 */
	return 0;
}

// Funciones de logger

t_log* iniciar_logger() {
	return log_create("LFS.log", "LFS", 1, LOG_LEVEL_DEBUG);
}

void logeoDeErrores(int errorHandler, t_log* logger) {
	//Optimizacion: ver si hay una forma mejor de manejar los errores
	switch (errorHandler) {
	case tablaExistente:
		log_info(logger, "Se trato de crear una tabla ya existente");
		break;

	case carpetaNoCreada:
		log_info(logger, "No se pudo crear la carpeta de la tabla");
		break;

	case metadataNoCreada:
		log_info(logger, "No se pudo crear el archivo metadata");
		break;

	case BINNoCreado:
		log_info(logger, "No se pudo crear un archivo .bin");
		break;

	case noExisteTabla:
		log_info(logger, "No existe la tabla solicitada");
		break;

	case tablaNoEliminada:
		log_info(logger, "La tabla solicitada no pudo ser eliminada");
		break;

	case noAbreMetadata:
		log_info(logger, "No se pudo abrir el archivo metadata");
		break;

	case noExisteParametro:
		log_info(logger, "El parametro solicitado no existe");
		break;
	}
}

metadata* listarDirectorios(char *dir) {
	FTS* ftsp;
	FTSENT* curr;
	char* directorios[] = { (char *) dir, NULL };

	ftsp = fts_open(directorios, FTS_NOCHDIR | FTS_PHYSICAL | FTS_XDEV, NULL);
	if (!ftsp)
		return -1;
	while ((curr = fts_read(ftsp))) {
		switch (curr->fts_info) {
		case FTS_NS:
		case FTS_DNR:
		case FTS_ERR:
			return -1;
			break;

		case FTS_DP:
		case FTS_F:
		case FTS_SL:
		case FTS_SLNONE:
		case FTS_DEFAULT:
			if (remove(curr->fts_accpath) < 0)
				return -1;
			break;
		}
	}
	fts_close(ftsp);
}

void finalizarEjecutcion() {
	printf("------------------------\n");
	printf("¿chau chau adios?\n");
	printf("------------------------\n");
	log_destroy(logger);
	close(socket_cli);
	close(socket_sv);
	list_iterate(memtable, free);
	raise(SIGTERM);
}
