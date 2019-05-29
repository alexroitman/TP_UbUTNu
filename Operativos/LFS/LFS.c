/*
 * LFS.c
 *
 *  Created on: 8 abr. 2019
 *      Author: USUARIO
 */

#include "LFS.h"

#define PUERTOLFS "7879"
t_log* logger;
t_list* memtable;
int main (void)
{
	int errorHandler;

		int socket_sv = levantarServidor(PUERTOLFS);
		int socket_cli = aceptarCliente(socket_sv);
		type header;
		memtable=inicializarMemtable();
		logger=iniciar_logger();
			while (1) {
				header = leerHeader(socket_cli);
				tSelect* packSelect=malloc(sizeof(tSelect));
				tInsert* packInsert=malloc(sizeof(tInsert));

				switch (header) {
				case SELECT:
					desSerializarSelect(packSelect,socket_cli);
					packSelect->type = header;
					errorHandler=SelectApi(packSelect->nombre_tabla,packSelect->key);
					/*printf("recibi un una consulta SELECT de la tabla %s con le key %d \n",
							packSelect->nombre_tabla, packSelect->key);*/
					break;
				case INSERT:
					desSerializarInsert(packInsert,socket_cli);
					packInsert->type = header;
					printf("recibi un una consulta INSERT de la tabla %s con le key %d y el value %s \n",
							packInsert->nombre_tabla, packInsert->key, packInsert->value);
					errorHandler=insertarEnMemtable(memtable,packInsert);
					if(errorHandler==todoJoya)
					{
						log_debug(logger,"se inserto bien");
					}
					break;
				}
			}

	t_log* logger = iniciar_logger();
//	t_config* metadata = config_create("metadata");


//	PRUEBA COMANDO CREATE
//	errorHandler = CREATE("tables/TABLA1",1,4,1);

//	PRUEBA COMANDO INSERT
//	errorHandler = INSERT("tables/TABLA1", 3, "Mi nombre es Lissandra", 1548421507);

//	PRUEBA COMANDO DROP
//	errorHandler = DROP("tables/TABLA1");

//	PRUEBA COMANDO SELECT
//	errorHandler = SELECT("tables/TABLA1", 6);

	if(errorHandler) {logeoDeErrores(errorHandler, logger);}
	close(socket_cli);
			close(socket_sv);

	return 0;
}

//memtable
t_list* inicializarMemtable(){
	return list_create();
}
int insertarEnMemtable(t_list* memtable,tInsert *packinsert){

	char* mi_ruta = string_new();
	char* tables = "/tables/";
	string_append(&mi_ruta, tables);
	string_append(&mi_ruta, packinsert->nombre_tabla);


	t_memtable* tabla;
	if(!verificadorDeTabla(mi_ruta)){
		return noExisteTabla;
	}
	registro* registro_insert = malloc(sizeof(registro));
	registro_insert->key = packinsert->key;

	log_debug(logger, string_itoa(registro_insert->key));
	//registro_insert->timestamp = packinsert->timestamp;//argregar
	char* value = malloc(packinsert->value_long);
	strcpy(value, packinsert->value);
	registro_insert->value = malloc(strlen(value) + 1);
	strcpy(registro_insert->value, value);


	if (!existe_tabla_en_memtable(packinsert->nombre_tabla )) {//chequeo si existe la tabla en la memtable
		if (!agregar_tabla_a_memtable(packinsert->nombre_tabla)) {//si no existe la agrego
			return -12;
		}
	}


	return todoJoya;
}
int insertarRegistro(registro* registro, char* nombre_tabla) {

	int es_tabla(t_tabla* tabla) {
		if (strcmp(tabla->nombreTabla, nombre_tabla) == 0) {
			return 1;
		}
		return 0;
	}
	t_tabla* tabla = (t_tabla*) list_find(memtable, (int) &es_tabla);
	list_add(tabla->registros, registro);
	return todoJoya;

}
int agregar_tabla_a_memtable(char* tabla) {
	t_tabla* tabla_a_agregar = malloc(sizeof(t_tabla));
	strcpy(tabla_a_agregar->nombreTabla , tabla);
	tabla_a_agregar->registros = list_create();

	return todoJoya;
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




int selectEnMemtable(t_list* memtable,uint16_t key,char* tabla){


	t_memtable* tablaSelect;
	//tablaSelect=list_filter(memtable,esTabla);
	if(tablaSelect!=NULL)
	{
		//list iterate y por cada iteracion hacer un list filter
	}


	free(tabla);
	return todoJoya;//devuelvo valor del select
}


// APIs

int Create(char* NOMBRE_TABLA, int TIPO_CONSISTENCIA, int NUMERO_PARTICIONES, int COMPACTATION_TIME)
{
	char aux[strlen(NOMBRE_TABLA)+10];
	// ---- Verifico que la tabla no exista ----
	if (verificadorDeTabla(NOMBRE_TABLA) == 0)
		return tablaExistente;

	// ---- Creo directorio de la tabla ----
	if(mkdir(NOMBRE_TABLA, 0700) == -1)
		return carpetaNoCreada;

	// ---- Creo y grabo Metadata de la tabla ----
	if(crearMetadata(NOMBRE_TABLA, TIPO_CONSISTENCIA, NUMERO_PARTICIONES,COMPACTATION_TIME))
		return metadataNoCreada;

	// ---- Creo los archivos binarios ----
	crearBinarios(NOMBRE_TABLA, NUMERO_PARTICIONES);
	return todoJoya;
}

int Insert (char* NOMBRE_TABLA, int KEY, char* VALUE, int Timestamp)
{
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

int Drop(char* NOMBRE_TABLA)
{

	// ---- Verifico que la tabla exista ----
	if (verificadorDeTabla(NOMBRE_TABLA) != 0)
		return noExisteTabla;

	// ---- Elimino el directorio y todos los archivos ----
	if(borrarDirectorio(NOMBRE_TABLA) != 0)
		return tablaNoEliminada;
	return todoJoya;
}

int SelectApi(char* NOMBRE_TABLA, int KEY)
{
	int particiones;
	// ---- Verifico que la tabla exista ----
	if (verificadorDeTabla(NOMBRE_TABLA) != 0)
		return noExisteTabla;

	// ---- Obtengo la metadata ----
	particiones = buscarEnMetadata(NOMBRE_TABLA, "PARTITIONS");
	if (particiones < 0)
		return particiones;

	// ---- Calculo particion del KEY ----
	particiones = KEY % particiones;

	// ---- Escaneo particion objetivo ----
	//Escaneo en la memtable
	//Escaneo en archivos temporales

	// ---- Retorno la KEY de mayor Timestamp ----

	return todoJoya;
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

int crearMetadata(char* NOMBRE_TABLA, int TIPO_CONSISTENCIA, int NUMERO_PARTICIONES, int COMPACTATION_TIME)
{
	char aux[strlen(NOMBRE_TABLA)+10];
	//Opitimizacion1A: Se puede hace que fp se pase por parámetro para no abrirlo en crearMetadata y crearBIN
	FILE *fp;
	snprintf(aux, sizeof(aux), "%s/metadata", NOMBRE_TABLA);
	fp = fopen(aux, "w+");
	if (fp==NULL)
		return -1;
	fprintf(fp, "CONSISTENCY=%i\n", TIPO_CONSISTENCIA);
	fprintf(fp, "PARTITIONS=%i\n", NUMERO_PARTICIONES);
	fprintf(fp, "COMPACTION_TIME=%i", COMPACTATION_TIME);
	fclose(fp);
	return todoJoya;
}

int crearBinarios(char* NOMBRE_TABLA, int NUMERO_PARTICIONES)
{
	//Opitimizacion1B: Se puede hace que fp se pase por parámetro para no abrirlo en crearMetadata y crearBIN
	FILE *fp;
	char aux[strlen(NOMBRE_TABLA)+10];
	while(NUMERO_PARTICIONES > 0)
	{
		snprintf(aux, sizeof(aux), "%s/%i.bin", NOMBRE_TABLA, NUMERO_PARTICIONES);
		fp = fopen(aux, "w+");
		if (!fp)
			return BINNoCreado;
		NUMERO_PARTICIONES -= 1;
	}
	fclose(fp);
	return todoJoya;
}

int verificadorDeTabla(char* NOMBRE_TABLA)
{
	int status = 1;
	DIR *dirp;

	dirp = opendir(NOMBRE_TABLA);

	if (dirp == NULL) {
		status = 0;
	}
	closedir(dirp);
	return status;
}

int borrarDirectorio(const char *dir)
{
    FTS* ftsp;
    FTSENT* curr;
    char* archivos[] = {(char *) dir, NULL};

    ftsp = fts_open(archivos, FTS_NOCHDIR | FTS_PHYSICAL | FTS_XDEV, NULL);
    if (!ftsp)
    	return -1;
    while ((curr = fts_read(ftsp)))
    {
        switch (curr->fts_info)
        {
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

int buscarEnMetadata(char* NOMBRE_TABLA, char* objetivo)
{
	char aux[40];
	FILE *fp;
	snprintf(aux, sizeof(aux), "%s/metadata", NOMBRE_TABLA);
	fp = fopen(aux, "r+");
	if (!fp)
		return noAbreMetadata;
	fgets(aux, 40, fp);
	while(strcmp(strtok(aux, "="), objetivo) != 0)
	{
		if(aux[strlen(aux)-1] == '\0')
			return noExisteParametro;
		fgets(aux, 40, fp);
	}// probar return
	return atoi(strtok(NULL, "="));
}


// Funciones de logger

t_log* iniciar_logger() {
	return log_create("LFS.log", "LFS", 1, LOG_LEVEL_INFO);
}

void logeoDeErrores(int errorHandler, t_log* logger)
{
	//Optimizacion: ver si hay una forma mejor de manejar los errores
	switch(errorHandler)
		{
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

metadata* listarDirectorios(char *dir){
	FTS* ftsp;
	    FTSENT* curr;
	    char* directorios[] = {(char *) dir, NULL};

	    ftsp = fts_open(directorios, FTS_NOCHDIR | FTS_PHYSICAL | FTS_XDEV, NULL);
	    if (!ftsp)
	    	return -1;
	    while ((curr = fts_read(ftsp)))
	    {
	        switch (curr->fts_info)
	        {
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





t_log* inicializar_logger(void) {

	return log_create("/home/utnso/tp-2019-1c-UbUTNu/Operativos/LFS", "LFS", 1, LOG_LEVEL_DEBUG);

}
