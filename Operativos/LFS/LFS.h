/*
 * LFS.h
 *
 *  Created on: 8 abr. 2019
 *      Author: USUARIO
 */

#ifndef LFS_H_
#define LFS_H_

// TIPOS DE CONSISTENCIA
#define SC 1 //Strong Consistency
#define SHC 2 //Strong Hash Consistency
#define EC 3 // Eventual Consistency

// TIPOS DE ERRORES PARA LOGGER
#define todoJoya 0
#define tablaExistente -1
#define carpetaNoCreada -2
#define metadataNoCreada -3
#define BINNoCreado -4
#define tablaNoEliminada -6
#define noAbreMetadata -7
#define noExisteParametro -8
#define noExisteTabla -9

#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <commons/log.h>
#include <commons/string.h>
#include <commons/config.h>
#include <readline/readline.h>
#include <time.h>
#include <fts.h>
#include <pthread.h>
#include <dirent.h>
#include <sock/sockets-lib.h>
#include <sock/comunicacion.h>
#include <sys/socket.h>
// Estructuras
typedef struct {
	int particiones;
	int consistencia;
	int tiempo_compactacion;
} metadata;

// APIs
int SelectApi(char* NOMBRE_TABLA, int KEY);
int Insert (char* NOMBRE_TABLA, int KEY, char* VALUE, int Timestamp);
int Create(char* NOMBRE_TABLA, int TIPO_CONSISTENCIA, int NUMERO_PARTICIONES, int COMPACTATION_TIME);
metadata Describe(char* NOMBRE_TABLA);
int Drop(char* NOMBRE_TABLA);

// Funciones de tabla
int crearMetadata(char* NOMBRE_TABLA, int TIPO_CONSISTENCIA, int NUMERO_PARTICIONES, int COMPACTATION_TIME);
int crearBinarios(char* NOMBRE_TABLA, int NUMERO_PARTICIONES);
int verificadorDeTabla(char* NOMBRE_TABLA);
int borrarDirectorio(const char* directorio);
int buscarEnMetadata(char* NOMBRE_TABLA, char* objetivo);

// Funciones de logger
t_log* iniciar_logger(void);
void logeoDeErrores(int errorHandler, t_log* logger);

#endif /* LFS_H_ */
