/*
 * sockets-lib.h
 *
 *  Created on: 16 abr. 2019
 *      Author: utnso
 */

#ifndef SOCKETS_LIB_H_
#define SOCKETS_LIB_H_

#include <unistd.h>
#define BACKLOG 5			// Define cuantas conexiones vamos a mantener pendientes al mismo tiempo
#define PACKAGESIZE 1024	// Define cual va a ser el size maximo del paquete a enviar
char package[PACKAGESIZE];
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
struct addrinfo hints;
struct addrinfo *serverInfo;

int levantarCliente(char* puerto,char* ip);
int levantarServidor(char* puerto);
void enviarMensaje(int clienteSocket);
int aceptarCliente(int serverSocket);
void recibirMensaje(int socketServidor);
void cerrarConexiones(int socket_cliente,int socket_servidor);
#endif /* SOCKETS_LIB_H_ */
