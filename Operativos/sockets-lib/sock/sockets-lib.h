/*
 * sockets-lib.h
 *
 *  Created on: 16 abr. 2019
 *      Author: utnso
 */

#ifndef SOCKETS_LIB_H_
#define SOCKETS_LIB_H_

#include <unistd.h>
#define BACKLOG 10			// Define cuantas conexiones vamos a mantener pendientes al mismo tiempo
#define PACKAGESIZE 1024	// Define cual va a ser el size maximo del paquete a enviar
#define MAX_BUFFER 1024
char package[PACKAGESIZE];
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <commons/string.h>
#include <sys/time.h>
#include <commons/collections/queue.h>
#include <sys/socket.h>
#include <netdb.h>
#include <semaphore.h>
#include <commons/log.h>
#include <time.h>
struct addrinfo hints;
struct addrinfo *serverInfo;






int levantarCliente(char* puerto,char* ip);
int levantarServidor(char* puerto);
int levantarClienteNoBloqueante(char* puerto, char* ip);
int aceptarCliente(int serverSocket);

void cerrarConexiones(int socket_cliente,int socket_servidor);


void dispose_package(char **package);
#endif /* SOCKETS_LIB_H_ */
