/*
 * sockets-lib.c
 *
 *  Created on: 16 abr. 2019
 *      Author: utnso
 */

#include "sockets-lib.h"


int levantarCliente(char* puerto,char* ip) {

	memset(&hints, 0, sizeof(hints));

		hints.ai_family = AF_UNSPEC; // Permite que la maquina se encargue de verificar si usamos IPv4 o IPv6
		hints.ai_socktype =SOCK_STREAM; // Indica que usaremos el protocolo TCP

		getaddrinfo(ip, puerto, &hints, &serverInfo); // Carga en serverInfo los datos de la conexion

		int serverSocket;
		serverSocket = socket(serverInfo->ai_family, serverInfo->ai_socktype,
				serverInfo->ai_protocol);


		while(connect(serverSocket, serverInfo->ai_addr, serverInfo->ai_addrlen) != 0){
			connect(serverSocket, serverInfo->ai_addr, serverInfo->ai_addrlen);
		}
		freeaddrinfo(serverInfo);	// No lo necesitamos mas
		return serverSocket;
}

int levantarServidor(char* puerto) {

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;		// No importa si uso IPv4 o IPv6
	hints.ai_flags = AI_PASSIVE;// Asigna el address del localhost: 127.0.0.1
	hints.ai_socktype = SOCK_STREAM;	// Indica que usaremos el protocolo TCP

	getaddrinfo(NULL, puerto, &hints, &serverInfo); // Notar que le pasamos NULL como IP, ya que le indicamos que use localhost en AI_PASSIVE
	int listenningSocket;
	listenningSocket = socket(serverInfo->ai_family, serverInfo->ai_socktype,
			serverInfo->ai_protocol);

	bind(listenningSocket, serverInfo->ai_addr, serverInfo->ai_addrlen);
	freeaddrinfo(serverInfo); // Ya no lo vamos a necesitar

	listen(listenningSocket, BACKLOG);

	return listenningSocket;
}

void enviarMensaje(int clienteSocket) {
	int enviar = 1;
	char message[PACKAGESIZE];

	printf("Conectado al servidor. Bienvenido al sistema, ya puede enviar mensajes. Escriba 'exit' para salir\n");

	while (enviar) {
		int status = 1;
		fgets(message, PACKAGESIZE, stdin);	// Lee una linea en el stdin (lo que escribimos en la consola) hasta encontrar un \n (y lo incluye) o llegar a PACKAGESIZE.
		if (!strcmp(message, "exit\n"))
			enviar = 0;			// Chequeo que el usuario no quiera salir
		if (enviar)
			send(clienteSocket, message, strlen(message) + 1, 0); // Solo envio si el usuario no quiere salir.
	}
}

int aceptarCliente(int serverSocket) {
	struct sockaddr_in addr;			// Esta estructura contendra los datos de la conexion del cliente. IP, puerto, etc.
	socklen_t addrlen = sizeof(addr);

		int socketCliente = accept(serverSocket, (struct sockaddr *) &addr, &addrlen);
		printf("Cliente conectado. Esperando mensajes:\n");
		return socketCliente;
}
void recibirMensaje(int socketServidor){
	char package[PACKAGESIZE];
	int status = 1;		// Estructura que manjea el status de los recieve.


	while (status != 0) {
		status = recv(socketServidor, (void*) package, PACKAGESIZE, 0);
		if (status != 0)
			printf("%s", package);
	}
}

