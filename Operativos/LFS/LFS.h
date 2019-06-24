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

#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <commons/string.h>
#include <commons/bitarray.h>
#include <commons/config.h>
#include <commons/collections/list.h>
#include <readline/readline.h>
#include <time.h>
#include <fts.h>
#include <pthread.h>
#include <dirent.h>
#include <sock/sockets-lib.h>
#include <sock/comunicacion.h>
#include <sock/logger.h>
#include <sys/socket.h>
#include <stdbool.h>
#include <dirent.h>
#include <errno.h>
#include <signal.h>

// Estructuras
typedef struct {
	int particiones;
	int consistencia;
	int tiempo_compactacion;
} metadata;

typedef struct{
	char* nombreTabla;
	t_list* registros;
}t_tabla;

typedef struct{
	t_tabla tabla;
}t_memtable ;


// MEMTABLE
t_list* inicializarMemtable();
int insertarEnMemtable(tInsert* packinsert);
int insertarRegistro(registro* registro, char* nombre_tabla);
int agregar_tabla_a_memtable(char* tabla);
int existe_tabla_en_memtable(char* posible_tabla);

// APIs
int Create(char* NOMBRE_TABLA, char* TIPO_CONSISTENCIA, int NUMERO_PARTICIONES, int COMPACTATION_TIME);
int Insert (char* NOMBRE_TABLA, int KEY, char* VALUE, int Timestamp);
int Drop(char* NOMBRE_TABLA);
registro* Select(char* NOMBRE_TABLA, int KEY);
metadata Describe(char* NOMBRE_TABLA);

// AUXILIARES DE SELECT
registro* SelectFS(char* NOMBRE_TABLA, int KEY);
t_list* selectEnMemtable( uint16_t key, char* tabla);
t_list* SelectTemp(char* ruta, int KEY);

// TABLAS
int crearMetadata(char* NOMBRE_TABLA, char* TIPO_CONSISTENCIA, int NUMERO_PARTICIONES, int COMPACTATION_TIME);
int crearBinarios(char* NOMBRE_TABLA, int NUMERO_PARTICIONES);
int verificadorDeTabla(char* NOMBRE_TABLA);
int buscarEnMetadata(char* NOMBRE_TABLA, char* objetivo);
t_bitarray* levantarBitmap();
off_t obtener_bit_libre();
off_t limpiar_bit(char* bit);

// DUMPEO
void bajarAMemoria(int* fd2, char* registroParaEscribir, t_config* tmp);
void dumpearTabla(t_tabla* UnaTabla);
int dumpeoMemoria();
char* mapearBloque(int fd2, size_t textsize);
void actualizarBloquesEnTemporal(t_config* tmp, off_t bloque);

// OTROS
void crearBitmapNuestro(); // Solo lo usamos para pruebas
int borrarDirectorio(char* directorio);

// CIERRE
void finalizarEjecutcion();
#endif /* LFS_H_ */
