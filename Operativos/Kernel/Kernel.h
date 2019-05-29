/*
 * Kernel.h
 *
 *  Created on: 11 abr. 2019
 *      Author: utnso
 */

#include <stdio.h>
#include <ctype.h>

#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <pthread.h>
#include <sock/sockets-lib.h>
#include <sock/comunicacion.h>


#ifndef KERNEL_KERNEL_H_
#define KERNEL_KERNEL_H_
#define MAX_MESSAGE_SIZE 300
struct arg_RUN {
    FILE* archivoLQL;
    int socket_memoria;
};
typedef enum{
	new,
	ready,
	exec,
	exit_
}estados;


struct script {
    char* path;
    int pos;
    estados estado;
};

type leerConsola(); //Lee la consulta y devuelve el string
type validarSegunHeader(char* header);
bool analizarConsulta();

void run();
int despacharQuery(char* consulta, int socket_memoria);
void add();
void cargarPaqueteSelect();
void cargarPaqueteInsert();
//void rutinaRUN(void* argumentos);
void CPU(int socket_memoria);
void planificador();
int levantarCpus(int socket_memoria);
int leerLinea(char* path, int linea, char* leido);


#endif /* KERNEL_KERNEL_H_ */
