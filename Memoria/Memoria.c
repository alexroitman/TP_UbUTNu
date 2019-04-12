/*
 * Kernel.c
 *
 *  Created on: 11 abr. 2019
 *      Author: utnso
 */

#include "Memoria.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <pthread.h>
#define IP "127.0.0.1"
#define PUERTOESCUCHA "6667"
#define PUERTOESCRIBE "6666"
#define BACKLOG 5			// Define cuantas conexiones vamos a mantener pendientes al mismo tiempo
#define PACKAGESIZE 1024	// Define cual va a ser el size maximo del paquete a enviar
char package[PACKAGESIZE];
struct addrinfo hints;
struct addrinfo *serverInfo;

// Define cual va a ser el size maximo del paquete a enviar

int main() {
	//char* consulta = leerConsola();
	int socket_cliente = levantarCliente();
	int socket_servidor = levantarServidor();
	pthread_t hilo_Server;
	pthread_t hilo_Cliente;
	pthread_create(&hilo_Server, NULL, (void*) recibirMensajeDeKernel,
			(int*) socket_servidor);
	pthread_create(&hilo_Cliente, NULL, (void*) enviarMensajeAKernel,
			(int*) socket_cliente);
	pthread_join(hilo_Server,NULL);
	pthread_join(hilo_Cliente,NULL);
	cerrarConexiones(socket_cliente, socket_servidor);
	return 0;
}

void recibirMensajeDeKernel(int serverSocket) {
	char package[PACKAGESIZE];
	int status = 1;		// Estructura que manjea el status de los recieve.

	printf("Cliente conectado. Esperando mensajes:\n");

	while (status != 0) {
		status = recv(serverSocket, (void*) package, PACKAGESIZE, 0);
		if (status != 0)
			printf("%s", package);
	}
}

void enviarMensajeAKernel(int clienteSocket) {
	int enviar = 1;
	char message[PACKAGESIZE];

	printf("Conectado al servidor. Bienvenido al sistema, ya puede enviar mensajes. Escriba 'exit' para salir\n");

	while (enviar) {
		fgets(message, PACKAGESIZE, stdin);	// Lee una linea en el stdin (lo que escribimos en la consola) hasta encontrar un \n (y lo incluye) o llegar a PACKAGESIZE.
		if (!strcmp(message, "exit\n"))
			enviar = 0;			// Chequeo que el usuario no quiera salir
		if (enviar)
			send(clienteSocket, message, strlen(message) + 1, 0); // Solo envio si el usuario no quiere salir.

	}
}

int levantarCliente() {

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC; // Permite que la maquina se encargue de verificar si usamos IPv4 o IPv6
	hints.ai_socktype = SOCK_STREAM; // Indica que usaremos el protocolo TCP

	getaddrinfo(IP, PUERTOESCRIBE, &hints, &serverInfo); // Carga en serverInfo los datos de la conexion

	int serverSocket;
	serverSocket = socket(serverInfo->ai_family, serverInfo->ai_socktype,
			serverInfo->ai_protocol);

	connect(serverSocket, serverInfo->ai_addr, serverInfo->ai_addrlen);
	freeaddrinfo(serverInfo);	// No lo necesitamos mas
	return serverSocket;
}

int levantarServidor() {

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;		// No importa si uso IPv4 o IPv6
	hints.ai_flags = AI_PASSIVE;// Asigna el address del localhost: 127.0.0.1
	hints.ai_socktype = SOCK_STREAM;	// Indica que usaremos el protocolo TCP

	getaddrinfo(NULL, PUERTOESCUCHA, &hints, &serverInfo); // Notar que le pasamos NULL como IP, ya que le indicamos que use localhost en AI_PASSIVE
	int listenningSocket;
	listenningSocket = socket(serverInfo->ai_family, serverInfo->ai_socktype,
			serverInfo->ai_protocol);

	bind(listenningSocket, serverInfo->ai_addr, serverInfo->ai_addrlen);
	freeaddrinfo(serverInfo); // Ya no lo vamos a necesitar

	listen(listenningSocket, BACKLOG);
	return listenningSocket;
}

void cerrarConexiones(int socket_cliente, int socket_servidor) {
	close(socket_cliente);
	close(socket_servidor);
}
