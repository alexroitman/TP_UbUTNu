/*
 * LFS.h
 *
 *  Created on: 8 abr. 2019
 *      Author: USUARIO
 */

#ifndef LFS_H_
#define LFS_H_
#define EVENT_SIZE  ( sizeof (struct inotify_event) + 24 )

#define BUF_LEN     ( 1024 * EVENT_SIZE )
// TIPOS DE CONSISTENCIA
#define SC 1 //Strong Consistency
#define SHC 2 //Strong Hash Consistency
#define EC 3 // Eventual Consistency
#include <sys/inotify.h>
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
#include <stdarg.h>

// Estructuras
typedef struct {
	int particiones;
	int consistency;
	int tiempo_compactacion;
	char nombre_tabla[12];
} Metadata;

typedef struct{
	char* nombreTabla;
	t_list* registros;
}t_tabla;

typedef struct{
	t_tabla tabla;
}t_memtable ;

typedef struct{
	int blockSize;
	int blocks;
	char* magicNumber;
} metadataFS;

typedef struct{
	char* puerto;
	char* dirMontaje;
	int retardo;
	int tamanioValue;
	int tiempoDumpeo;
} config_FS;

typedef struct{
	type* header;
	char consulta[256];
}tHiloConsola;
tHiloConsola* paramsConsola;

//MAIN
void levantarMetadataFS();
void levantarConfigLFS();

//HILOS
void abrirHiloSockets();
void abrirHiloConsola();
void abrirHiloCompactacion();
type stringToHeader(char* str);
void hiloCompactar(char*nombre_tabla);
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
int Select(registro** reg, char* NOMBRE_TABLA, int KEY);
t_list* Describe();
t_list* DESCRIBEespecifico(char* NOMBRE_TABLA) ;

int consistency_to_int(char* cons);
Metadata* obtener_metadata(char* ruta);
// AUXILIARES DE SELECT
int SelectFS(char* NOMBRE_TABLA, int KEY, registro* registro);
t_list* selectEnMemtable( uint16_t key, char* tabla);
t_list* SelectTemp(char* ruta, int KEY);

// TABLAS
int crearMetadata(char* NOMBRE_TABLA, char* TIPO_CONSISTENCIA, int NUMERO_PARTICIONES, int COMPACTATION_TIME);
void crearBinarios(char* NOMBRE_TABLA, int NUMERO_PARTICIONES);
int verificadorDeTabla(char* NOMBRE_TABLA);
int buscarEnMetadata(char* NOMBRE_TABLA, char* objetivo);
t_bitarray* levantarBitmap();
off_t obtener_bit_libre();

// DUMPEO Y COMPACTACION
void bajarAMemoria(int* fd2, char* registroParaEscribir, t_config* tmp);
void dumpearTabla(t_list* registros, char* ruta);
int dumpeoMemoria();
char* mapearBloque(int fd2, size_t textsize);
void actualizarBloquesEnTemporal(t_config* tmp, off_t bloque);
int compactacion(char* nombre_tabla);
void guardar_en_disco(t_list* binarios, int cantParticiones,char* nombre_tabla);
void crearListaRegistros(char** string, t_list* lista);
int levantarbinarios(char* nombre_tabla, char** bloquesUnificados);
int obtener_temporales(char* nombre_tabla, char** bloquesUnificados);

// OTROS
char* direccionarTabla(char* tabla);
void crearBitmapNuestro(); // Solo lo usamos para pruebas
int borrarDirectorio(char* directorio);

// CIERRE
void finalizarEjecutcion();
void innotificar();
#endif /* LFS_H_ */

