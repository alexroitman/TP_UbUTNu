/*
 * LFS.c
 *
 *  Created on: 8 abr. 2019
 *      Author: USUARIO
 */

#include "LFS.h"

int main (void)
{
	t_log* logger = iniciar_logger();
//	t_config* metadata = config_create("metadata");
	int errorHandler;

//	PRUEBA COMANDO CREATE
//	errorHandler = CREATE("TABLA1",1,4,1);

//	PRUEBA COMANDO INSERT
//	errorHandler = INSERT("TABLA1", 3, "Mi nombre es Lissandra", 1548421507);

//	PRUEBA COMANDO DROP
//	errorHandler = DROP("TABLA1");

//	PRUEBA COMANDO SELECT
	errorHandler = SELECT("TABLA1", 6);

	if(errorHandler) {logeoDeErrores(errorHandler, logger);}
	return 0;
}

// APIs


int CREATE(char* NOMBRE_TABLA, int TIPO_CONSISTENCIA, int NUMERO_PARTICIONES, int COMPACTATION_TIME)
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

int INSERT (char* NOMBRE_TABLA, int KEY, char* VALUE, int Timestamp)
{

	// ---- Verifico que la tabla exista ----
	if (verificadorDeTabla(NOMBRE_TABLA) != 0)
		return noExisteTabla;

	// ---- Obtengo la metadata ----


	// ---- Verifico si existe memoria para dumpeo ----


	// ---- Manejo de Timestamp opcional ----


	// ---- Inserto datos en memoria temporal ----

	return todoJoya;
}

int DROP(char* NOMBRE_TABLA)
{

	// ---- Verifico que la tabla exista ----
	if (verificadorDeTabla(NOMBRE_TABLA) != 0)
		return noExisteTabla;

	// ---- Elimino el directorio y todos los archivos ----
	if(borrarDirectorio(NOMBRE_TABLA) != 0)
		return tablaNoEliminada;
	return todoJoya;
}

int SELECT(char* NOMBRE_TABLA, int KEY)
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


	// ---- Escaneo particion objetivo ----


	// ---- Retorno la KEY de mayor Timestamp ----

	return todoJoya;
}


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
	struct stat st = {0};
	return lstat(NOMBRE_TABLA, &st);
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
	char* token;
	int eof;
	FILE *fp;
	snprintf(aux, sizeof(aux), "%s/metadata", NOMBRE_TABLA);
	fp = fopen(aux, "w");
	if (!fp)
		return noAbreMetadata;
	fgets(aux, 40, fp);
	while(strcmp(!strtok(aux, "="), objetivo))
	{
		if(aux[strlen(aux)-1] == 0)
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
