/*
 * Kernel.h
 *
 *  Created on: 11 abr. 2019
 *      Author: utnso
 */

#include <stdio.h>
#include <ctype.h>

#include <signal.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <pthread.h>
#include <sock/sockets-lib.h>
#include <sock/comunicacion.h>
#include <commons/log.h>
#include <commons/collections/queue.h>
#include <commons/collections/list.h>
#include <commons/config.h>

#ifndef KERNEL_KERNEL_H_
#define KERNEL_KERNEL_H_
#define MAX_MESSAGE_SIZE 300
typedef enum{
	new,
	ready,
	exec,
	exit_
}estados;

typedef enum{
	sc,
	shc,
	ec
}consistencias;


typedef struct{
    char* path;
    int pos;
    estados estado;
    int id;
}script;

typedef struct{
	int id;
	int puerto;
}t_infoMem;

typedef struct{
	char* ip_mem;
	char* puerto_mem;
	int quantum;
	int MULT_PROC;
	int metadata_refresh;
	int sleep;
}configKernel;

type leerConsola(); //Lee la consulta y devuelve el string
type validarSegunHeader(char* header);
bool analizarConsulta();

void run();
int despacharQuery(char* consulta, int socket_memoria);
void add();
void cargarPaqueteSelect();
void cargarPaqueteInsert();
void CPU(int socket_memoria);
void planificador();
int leerLinea(char* path, int linea, char* leido);
void cargarPaqueteCreate(tCreate *pack, char* cons);
int validarSelect(char* consulta);
int validarCreate(char* consulta);
int validarInsert(char* consulta);
int validarAdd(char* consulta);
t_infoMem* generarMem(char* consulta);
int levantarCpus(int socket_memoria, pthread_t* cpus);
void cargarConfig(t_config* config);
consistencias obtCons(char* criterio);
void inicializarTodo();
void finalizarEjecucion();

#endif /* KERNEL_KERNEL_H_ */
