/*
 * LFS.c
 *
 *  Created on: 8 abr. 2019
 *      Author: USUARIO
 */

#include "LFS.h"

#define PUERTOLFS "7879"
#define DirMontaje "../FS_LISSANDRA/"
#define DirTablas "../../FS_LISSANDRA/Tablas/"
#define DirBloques "../../FS_LISSANDRA/Bloques/"
#define DirMetadata "../../FS_LISSANDRA/Metadata/"

// ---------------VARIABLES GLOBALES-----------------

int socket_sv;
int socket_cli;
int cantidadDeDumpeos = 1;
t_log* logger;
t_list* memtable;
t_config* configLFS;
t_config* configMetadata;
char* dirMontaje;
int cantBloques;

// ---------------MAIN-----------------

int main(void) {
	//Creo variables
	int errorHandler;

	//Levanto config
	logger = iniciar_logger();
	configLFS = config_create("../LFS.config");
	dirMontaje = config_get_string_value(configLFS, "PUNTO_MONTAJE");
	char* puerto = config_get_string_value(configLFS, "PUERTO_ESCUCHA");
	int tamanioValue = config_get_int_value(configLFS, "TAMANIO_VALUE");
	configMetadata = config_create("../../FS_LISSANDRA/Metadata/Metadata.bin");
	cantBloques = config_get_int_value(configMetadata, "BLOCKS");
	signal(SIGINT, finalizarEjecutcion); //Comando de cierre al cortar con Ctrl+C

	// ---------------Pruebas con bitmap propio-----------------
	crearBitmapNuestro();

	//Levanto sockets
	socket_sv = levantarServidor(puerto);
	socket_cli = aceptarCliente(socket_sv);
	send(socket_cli,&tamanioValue,4,0);
	type header;
	memtable = inicializarMemtable();
	while (1) {
		header = leerHeader(socket_cli);
		//TODO: ver si estos paquetes no conviene crearlos en su respectivo case, desperdicio de memoria en cada while
		tSelect* packSelect = malloc(sizeof(tSelect));
		tInsert* packInsert = malloc(sizeof(tInsert));
		tCreate* packCreate = malloc(sizeof(tCreate));
		tDrop* packDrop = malloc(sizeof(tDrop));

		switch (header) {
		case SELECT:
			log_debug(logger, "Comando SELECT recibido");
			desSerializarSelect(packSelect, socket_cli);
			packSelect->type = header;
			registro* reg = Select(packSelect->nombre_tabla, packSelect->key);
			log_debug(logger, "%s", reg->value);
			tRegistroRespuesta* registro = malloc(sizeof(tRegistroRespuesta));
			registro->tipo = REGISTRO;
			registro->timestamp = reg->timestamp;
			registro->value = reg->value;
			registro->key = reg->key;
			registro->value_long = strlen(reg->value) + 1;
			char* registroSerializado = serializarRegistro(registro);
			enviarPaquete(socket_cli, registroSerializado, registro->length);
			free(registro);
			free(packSelect);
			break;

		case INSERT:
			desSerializarInsert(packInsert, socket_cli);
			packInsert->type = header;
			printf("Comando INSERT recibido: tabla %s con key %d y value %s \n",
					packInsert->nombre_tabla, packInsert->key,
					packInsert->value);
			errorHandler = insertarEnMemtable(packInsert);
			if (errorHandler == todoJoya)
				log_debug(logger, "se inserto bien");
			free(packInsert);
			break;

		case CREATE:
			//TODO: Restablecer funcionamiento normal de CREATE
			log_debug(logger, "Comando CREATE recibido");
			desSerializarCreate(packCreate, socket_cli);
			packCreate->type = header;
			errorHandler = Create(packCreate->nombre_tabla,
					packCreate->consistencia, packCreate->particiones,
					packCreate->compaction_time);
			if (errorHandler == todoJoya)
				log_debug(logger, "se creo bien");
			free(packCreate);
			//	dumpeoMemoria();
			break;

		case ADD:
			break;

		case DESCRIBE:
			break;

		case DROP:

			log_debug(logger, "Recibi un drop");
			desSerializarDrop(packDrop, socket_cli);
			log_debug(logger, "Desserialice bien");
			packDrop->type = header;

			errorHandler = Drop(packDrop->nombre_tabla);
			if (errorHandler == todoJoya)
				log_debug(logger, "se elimino bien");

			free(packDrop);
			break;

		case JOURNAL:
			break;

		case NIL:
			break;

		case REGISTRO:
			break;

		}
	}

	//Rutina de cierre
	if (errorHandler)
		logeoDeErrores(errorHandler, logger);
	log_destroy(logger);
	close(socket_cli);
	close(socket_sv);
	config_destroy(configLFS);
	config_destroy(configMetadata);
	return 0;
}

// ---------------MEMTABLE-----------------

t_list* inicializarMemtable() {
	return list_create();
}

int insertarEnMemtable(tInsert *packinsert) {
	char* mi_ruta = string_new();
	string_append(&mi_ruta, dirMontaje);
	string_append(&mi_ruta, "Tablas/");
	string_append(&mi_ruta, packinsert->nombre_tabla);
	log_debug(logger, mi_ruta);
	if (!verificadorDeTabla(mi_ruta))
		return noExisteTabla;
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
		if (strcmp(tabla->nombreTabla, nombre_tabla) == 0)
			return 1;
		return 0;
	}
	//TODO: Que pasó aca? Hay una funcion arriba sin iterate, medio loco. Ver si se puede organizar sin esa funcion.
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

// ---------------APIs-----------------

int Create(char* NOMBRE_TABLA, char* TIPO_CONSISTENCIA, int NUMERO_PARTICIONES,
		int COMPACTATION_TIME) {
	char* ruta = string_new();
	string_append(&ruta, dirMontaje);
	string_append(&ruta, "Tablas/");
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
	log_debug(logger, "cree metadata");
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
	log_debug(logger, "entre al drop");
	// ---- Verifico que la tabla exista ----
	char* ruta = string_new();
	string_append(&ruta, dirMontaje);
	string_append(&ruta, "Tablas/");
	string_append(&ruta, NOMBRE_TABLA);
	log_debug(logger, "no me fijo en %s", ruta);
	if (!verificadorDeTabla(ruta)) {
		log_debug(logger, "no existe la tabla en %s", ruta);
		return noExisteTabla;
	}
	//eliminar de la memtable
	char* rutametadata = string_new();
	string_append(&rutametadata, ruta);
	string_append(&rutametadata, "/metadata");
	t_config* metadataTabla = config_create(rutametadata);
	int cantParticiones = config_get_int_value(metadataTabla, "PARTITIONS");
	log_debug(logger, "PART: %d", cantParticiones);
	config_destroy(metadataTabla);
	free(rutametadata);
	for (int i = 1; i <= cantParticiones; i++) {
		log_debug(logger, "entre al for");
		char* binario_a_limpiar = string_new();
		string_append(&binario_a_limpiar, dirMontaje);
		string_append(&binario_a_limpiar, "Tablas/");
		string_append(&binario_a_limpiar, NOMBRE_TABLA);
		string_append_with_format(&binario_a_limpiar, "/%d", i);
		string_append(&binario_a_limpiar, ".bin");
		log_debug(logger, "Me voy a fijar bloques en : %s", binario_a_limpiar);
		t_config* binario = config_create(binario_a_limpiar);
		char** bloques_a_limpiar = config_get_array_value(binario, "BLOCKS");
		int i = 0;
		while (bloques_a_limpiar[i] != NULL) {
			log_debug(logger, "Me voy a fijar en el: %s", bloques_a_limpiar[i]);
			limpiar_bit(bloques_a_limpiar[i]);
			i++;
		}
		free(binario_a_limpiar);
		config_destroy(binario);
	}
	if (cantidadDeDumpeos > 1) {
		for (int j = 1; j <= cantidadDeDumpeos; j++) {
			log_debug(logger, "entre al for");
			char* temporal_a_limpiar = string_new();
			string_append(&temporal_a_limpiar, dirMontaje);
			string_append(&temporal_a_limpiar, "Tablas/");
			string_append(&temporal_a_limpiar, NOMBRE_TABLA);
			string_append_with_format(&temporal_a_limpiar, "/%d", j);
			string_append(&temporal_a_limpiar, ".tmp");
			log_debug(logger, "Me voy a fijar bloques en : %s",
					temporal_a_limpiar);
			t_config* temporal = config_create(temporal_a_limpiar);
			char** bloques_a_limpiar = config_get_array_value(temporal,
					"BLOCKS");
			int i = 0;
			while (bloques_a_limpiar[i] != NULL) {
				log_debug(logger, "Me voy a fijar en el: %s",
						bloques_a_limpiar[i]);
				limpiar_bit(bloques_a_limpiar[i]);
				i++;
			}
			free(temporal_a_limpiar);
			config_destroy(temporal);
		}
	}
	char* comando_a_ejecutar = string_new();
	string_append_with_format(&comando_a_ejecutar, "rm -rf %s", ruta);
	system(comando_a_ejecutar);
	free(comando_a_ejecutar);

	// ---- Elimino el directorio y todos los archivos ----
	//if (borrarDirectorio(NOMBRE_TABLA) != 0)
	//	return tablaNoEliminada;

	free(rutametadata);
	return todoJoya;
}
off_t limpiar_bit(char* bit) {
	t_bitarray* bitmap = levantarBitmap();

	log_debug(logger, "levanteBitmap y voy a setear el %s", bit);
	bitarray_clean_bit(bitmap, atoi(bit));
//	bitarray_clean_bit(bitmap,bit);

	return bit;
}

registro* Select(char* NOMBRE_TABLA, int KEY) {
	int es_mayor(registro* unReg, registro* otroReg) {
		if (unReg->timestamp >= otroReg->timestamp) {
			return 1;
		}
		return 0;
	}
	char* ruta = string_new();
	string_append(&ruta, dirMontaje);
	string_append(&ruta, "Tablas/");
	string_append(&ruta, NOMBRE_TABLA);
	if (!verificadorDeTabla(ruta)) {
		log_debug(logger, "no existe la tabla");
		return noExisteTabla;
	}
	registro* registro_select = malloc(sizeof(registro));
	t_list* registros = list_create();
	log_debug(logger, "me voy a fijar a memtable");
	list_add_all(registros, selectEnMemtable(KEY, NOMBRE_TABLA));
	log_debug(logger, "me voy a fijar a FS");
	list_add(registros, SelectFS(ruta, KEY));
	log_debug(logger, "me voy a fijar a Temp");
	list_add_all(registros, SelectTemp(ruta, KEY));
	list_sort(registros, (int) &es_mayor);
	registro_select = list_get(registros, 0);
	log_debug(logger, registro_select->value);
	free(ruta);
	return registro_select;

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

// ---------------AUXILIARES DE SELECT-----------------
t_list* selectEnMemtable(uint16_t key, char* tabla) {
	int es_tabla(t_tabla* UnaTabla) {
		if (strcmp(UnaTabla->nombreTabla, tabla) == 0)
			return 1;
		return 0;
	}

	int es_registro(registro* unregistro) {
		if (unregistro->key == key)
			return 1;
		return 0;
	}

	t_tabla* tablaSelect;
	t_list* list_reg = list_create();
	tablaSelect = list_find(memtable, (int) &es_tabla);
	if (tablaSelect != NULL)
		list_add_all(list_reg,
				list_filter(tablaSelect->registros, (int) &es_registro));
//	log_debug(logger, reg->value);
//	free(tabla);
	return list_reg; //devuelvo valor del select
}

registro* SelectFS(char* ruta, int KEY) {
	int particiones;
	log_debug(logger, "%s", ruta);
	particiones = buscarEnMetadata(ruta, "PARTITIONS");
	log_debug(logger, "%d", particiones);
	if (particiones < 0)
		return particiones;

	// ---- Calculo particion del KEY ----
	particiones = (KEY % particiones);
	char* rutaFS = string_new();
	string_append(&rutaFS, ruta);
	string_append(&rutaFS, ("/"));
	string_append(&rutaFS, string_itoa(particiones));
	string_append(&rutaFS, ".bin");
	t_config* particion = config_create(rutaFS);
	int size = config_get_int_value(particion, "SIZE");
	char** bloquesABuscar = config_get_array_value(particion, "BLOCKS");
	int i = 0;
	registro* registro = malloc(sizeof(registro));
	while (bloquesABuscar[i] != NULL) {
		char* bloque = string_new();
		string_append(&bloque, dirMontaje);
		string_append(&bloque, "Bloques/");
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
				free(registro->value);
				registro->timestamp = atol(datos_registro[0]);
				registro->key = atoi(datos_registro[1]);
				registro->value = malloc(strlen(datos_registro[2]) + 1);
				strcpy(registro->value, datos_registro[2]);
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
	free(rutaFS);
	free(bloquesABuscar);
	config_destroy(particion);
	return registro;
}

t_list* SelectTemp(char* ruta, int KEY) {
	t_list* listRegistros = list_create();
	log_debug(logger, "arranca SelectTemp con contDumpeos:%d",
			cantidadDeDumpeos);
//	Ejecuto lo siguiento por cada temporal creado (uno por dumpeo)
	for (int aux = 0; aux < cantidadDeDumpeos; aux++) {
		char* rutaTemporal = string_new();
		string_append(&rutaTemporal, ruta);
		string_append(&rutaTemporal, ("/"));
		string_append_with_format(&rutaTemporal, "%d", aux);
		string_append(&rutaTemporal, ".tmp");
		log_debug(logger, "Ruta de SelectTemp: %s", rutaTemporal);
		t_config* particion = config_create(rutaTemporal);
		int size = config_get_int_value(particion, "SIZE");
		log_debug(logger, "size tiene un tamaño de %d", size);
		char** bloquesABuscar = config_get_array_value(particion, "BLOCKS");
		int i = 0;
//		Unifico la informacion de todos los bloques en los que esta dividido el archivo .tmp
		char* bloquesUnificados = string_new();
		while (bloquesABuscar[i] != NULL) {
			log_debug(logger, "Arranco a buscar bloques: Bloque N°%d", i);
			char* bloque = string_new();
			string_append(&bloque, dirMontaje);
			string_append(&bloque, "Bloques/");
			string_append(&bloque, bloquesABuscar[i]);
			string_append(&bloque, ".bin");
			log_debug(logger, "Ruta para buscar bloques: %s", bloque);
			int fd = open(bloque, O_RDONLY, S_IRUSR | S_IWUSR);
			struct stat s;
			int status = fstat(fd, &s);
			size = s.st_size;
			char* f = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);
			string_append(&bloquesUnificados, f);
			close(fd);
			free(bloque);
			i++;
		}
		int j = 0;
		log_debug(logger, "Tengo los bloques unificados");
		log_debug(logger, "%s", bloquesUnificados);
		char** registros = string_split(bloquesUnificados, "\n");
//		Recorro y divido los datos unificado del archivos temporal, almacenando solo las keys que coincidan con la solicitada
		while (registros[j] != NULL) {
			registro* registro = malloc(sizeof(registro));
			char** datos_registro = string_split(registros[j], ";");
			log_debug(logger, "Lei esto: %s", registros[j]);
			if (atoi(datos_registro[1]) == KEY) {
				registro->timestamp = atoi(datos_registro[0]);
				registro->key = atoi(datos_registro[1]);
				registro->value = malloc(strlen(datos_registro[2]) + 1);
				strcpy(registro->value, datos_registro[2]);
				list_add(listRegistros, registro);
			}
			string_iterate_lines(datos_registro, (void*) free);
			free(datos_registro);
			j++;
		}
		string_iterate_lines(registros, (void*) free);
		free(registros);
		free(rutaTemporal);
		free(bloquesABuscar);
		config_destroy(particion);
		free(bloquesUnificados);
	}
	return listRegistros;
}

// ---------------TABLAS-----------------

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
	log_debug(logger, "voy a crear bin");
	while (NUMERO_PARTICIONES > 0) {
		char* nuevaParticion = string_new();
		string_append(&nuevaParticion, NOMBRE_TABLA);
		string_append(&nuevaParticion, "/");
		string_append_with_format(&nuevaParticion, "%d", NUMERO_PARTICIONES);
		string_append(&nuevaParticion, ".bin");
		log_debug(logger, nuevaParticion);
		int fd = creat(nuevaParticion, (mode_t) 0600);
		close(fd);
		t_config* particion = config_create(nuevaParticion);
		char* bloques = string_new();
		string_append_with_format(&bloques, "[%d]", obtener_bit_libre());
		config_set_value(particion, "BLOCKS", bloques);
		config_set_value(particion, "SIZE", "0");
		config_save(particion);
		config_destroy(particion);
		free(bloques);
		NUMERO_PARTICIONES -= 1;
		free(nuevaParticion);
	}

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

int buscarEnMetadata(char* NOMBRE_TABLA, char* objetivo) {

	char* ruta = string_new();
	string_append(&ruta, NOMBRE_TABLA);
	string_append(&ruta, "/metadata");
	t_config* metadata = config_create(ruta);
	int obj = config_get_int_value(metadata, objetivo);
	config_destroy(metadata);
	free(ruta);
	return obj;
}

t_bitarray* levantarBitmap() {
	char* ruta = string_new();
	string_append(&ruta, dirMontaje);
	string_append(&ruta, "Metadata/Bitmap.bin");
	int size = cantBloques / 8;
	if (cantBloques % 8 != 0)
		size++;
	int fd = open(ruta, O_RDWR, S_IRUSR | S_IWUSR);
	ftruncate(fd, size);
	char* bitarray = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd,
			0);
	t_bitarray* structBitarray = bitarray_create_with_mode(bitarray, size,
			MSB_FIRST);
	free(ruta);
	return structBitarray;
}

off_t obtener_bit_libre() {
	t_bitarray* bitmap = levantarBitmap();
	off_t bit_index = 0;
	//leer el primero libre y ponerle 1
	while (bitarray_test_bit(bitmap, bit_index))
		bit_index++;
	bitarray_set_bit(bitmap, bit_index);
	return bit_index;
}

// ---------------DUMPEO-----------------

int dumpeoMemoria() {
	if (list_is_empty(memtable))
//		TODO: poner define para memtable vacia
		return 1;
	list_iterate(memtable, &dumpearTabla);
	list_clean(memtable);
	cantidadDeDumpeos++;
	return 0;
}

void dumpearTabla(t_tabla* UnaTabla) {
	char* tablaParaDumpeo = string_new();
	string_append(&tablaParaDumpeo, dirMontaje);
	string_append(&tablaParaDumpeo, "Tablas/");
	string_append(&tablaParaDumpeo, UnaTabla->nombreTabla);
	string_append(&tablaParaDumpeo, "/");
	string_append(&tablaParaDumpeo, string_itoa(cantidadDeDumpeos));
	string_append(&tablaParaDumpeo, ".tmp");
	off_t bit_index = obtener_bit_libre();
	int fd = creat(tablaParaDumpeo, (mode_t) 0600);
	close(fd);
	t_config* tmp = config_create(tablaParaDumpeo);
	char* bloque = string_new();
	string_append_with_format(&bloque, "[%d]", bit_index);
	config_set_value(tmp, "SIZE", "0");
	config_set_value(tmp, "BLOCKS", bloque);
	config_save(tmp);

	char* bloqueDumpeo = string_new();
	string_append(&bloqueDumpeo, dirMontaje);
	string_append(&bloqueDumpeo, "Bloques/");
	string_append_with_format(&bloqueDumpeo, "%d", bit_index);
	string_append(&bloqueDumpeo, ".bin");

	int fd2 = open(bloqueDumpeo, O_RDWR | O_CREAT | O_TRUNC, (mode_t) 0600);
	char* registroParaEscribir = string_new();
	void dumpearRegistros(registro* UnRegistro) {
		string_append(&registroParaEscribir,
				string_itoa(UnRegistro->timestamp));
		string_append(&registroParaEscribir, ";");
		string_append(&registroParaEscribir, string_itoa(UnRegistro->key));
		string_append(&registroParaEscribir, ";");
		string_append(&registroParaEscribir, UnRegistro->value);
		string_append(&registroParaEscribir, "\n");
		log_debug(logger, "voy a escribir esto: %s", registroParaEscribir);
	}
	list_iterate(UnaTabla->registros, (void*) &dumpearRegistros);
	bajarAMemoria(&fd2, registroParaEscribir, tmp);
//	struct stat s;
//	int status = fstat(fd2, &s);
	close(fd2);
	config_destroy(tmp);
	free(registroParaEscribir);
}

void bajarAMemoria(int* fd2, char* registroParaEscribir, t_config* tmp) {
	int block_size = config_get_int_value(configMetadata, "BLOCK_SIZE");
	//TODO: levantar un struct metadata global del metadata general
	size_t textsize = strlen(registroParaEscribir) + 1; // + \0 null character
	int size = config_get_int_value(tmp, "SIZE");
	char* strsize = string_new();
	string_append_with_format(&strsize, "%d", (size + textsize - 1));
	config_set_value(tmp, "SIZE", strsize);
	config_save(tmp);
	free(strsize);
	log_debug(logger, "libere strsize");
	char* map = mapearBloque(*fd2, textsize);
	int i = 0;
	int posmap = 0;
	log_debug(logger, "VOY A ARRANCAR EL WHILE");
	while (i < textsize) {
		if (posmap < block_size)
			map[posmap] = registroParaEscribir[i];
		else {
			//llamar a bloque
//			TODO: armar lo que sigue como subfuncion, esta en dumpeo memoria repetido
			off_t bit_index = obtener_bit_libre();
			log_debug(logger, "entre al else de textsize y bit_index vale %d",
					bit_index);
			actualizarBloquesEnTemporal(tmp, bit_index);
			log_debug(logger, "voy a crear string de bloques dumpeo");
			char* bloqueDumpeoNuevo = string_new();
			string_append(&bloqueDumpeoNuevo, dirMontaje);
			string_append(&bloqueDumpeoNuevo, "Bloques/");
			string_append_with_format(&bloqueDumpeoNuevo, "%d", bit_index);
			string_append(&bloqueDumpeoNuevo, ".bin");
			log_debug(logger, "cree string de bloques dumpeo");
			*fd2 = open(bloqueDumpeoNuevo, O_RDWR | O_CREAT | O_TRUNC,
					(mode_t) 0600);
//			Hasta aca
			map = mapearBloque(*fd2, textsize - i);
			posmap = 0;
			map[posmap] = registroParaEscribir[i];
		}
		i++;
		posmap++;
	}
	log_debug(logger, "baje a memoria esto: %s", map);
	msync(map, size, MS_SYNC);
	munmap(map, textsize);
}

char* mapearBloque(int fd2, size_t textsize) {
	lseek(fd2, textsize, SEEK_SET);
	write(fd2, "", 1);
	log_debug(logger, "escribi el barra 0");
	char *map = mmap(0, textsize, PROT_READ | PROT_WRITE, MAP_SHARED, fd2, 0);
	return map;
}

void actualizarBloquesEnTemporal(t_config* tmp, off_t bloque) {
	char* bloques = config_get_string_value(tmp, "BLOCKS");
	log_debug(logger, "habia %s bloques en el .tmp", bloques);
	bloques = string_substring_until(bloques, string_length(bloques) - 1);
	log_debug(logger, "queda %s despues de cortar %d caracteres", bloques,
			string_length(bloques) + 2);
	string_append_with_format(&bloques, ",%d]", bloque);
	log_debug(logger, "van a quedar %s bloques en el .tmp", bloques);
	config_set_value(tmp, "BLOCKS", bloques);
	config_save(tmp);
	log_debug(logger, "grabe los bloques");
	free(bloques);
}

// ---------------OTROS-----------------

void crearBitmapNuestro() {
	int size = cantBloques / 8;
	if (cantBloques % 8 != 0)
		size++;
	char* bitarray = calloc(cantBloques / 8, sizeof(char));
	t_bitarray* structBitarray = bitarray_create_with_mode(bitarray, size,
			MSB_FIRST);
	for (int i = 0; i < cantBloques; i++) {
		if (i == 10)
			bitarray_set_bit(structBitarray, i);
		else
			bitarray_clean_bit(structBitarray, i);
	}
	char* path = string_from_format("%s/Metadata/Bitmap.bin", dirMontaje);
	FILE* file = fopen(path, "wb+");
	fwrite(structBitarray->bitarray, sizeof(char), cantBloques / 8, file);
	fclose(file);
	bitarray_destroy(structBitarray);
	free(path);
}

int borrarDirectorio(char *dir) {
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
	return todoJoya;
}

// ---------------CIERRE-----------------

void finalizarEjecutcion() {
	printf("------------------------\n");
	printf("¿chau chau adios?\n");
	printf("------------------------\n");
	log_destroy(logger);
	close(socket_cli);
	close(socket_sv);
	list_iterate(memtable, free);
	config_destroy(configLFS);
	config_destroy(configMetadata);
	raise(SIGTERM);
}
