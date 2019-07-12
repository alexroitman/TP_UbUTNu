/*
 * sockets-lib.c
 *
 *  Created on: 16 abr. 2019
 *      Author: utnso
 */

#include "sockets-lib.h"

#include <stdio.h>
#include "comunicacion.h"
int levantarCliente(char* puerto,char* ip) {

	memset(&hints, 0, sizeof(hints));

		hints.ai_family = AF_UNSPEC; // Permite que la maquina se encargue de verificar si usamos IPv4 o IPv6
		hints.ai_socktype =SOCK_STREAM; // Indica que usaremos el protocolo TCP
		struct addrinfo *serverInfo;
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
	struct addrinfo *serverInfo;
	getaddrinfo(NULL, puerto, &hints, &serverInfo); // Notar que le pasamos NULL como IP, ya que le indicamos que use localhost en AI_PASSIVE
	int listenningSocket;
	listenningSocket = socket(serverInfo->ai_family, serverInfo->ai_socktype,
			serverInfo->ai_protocol);

	bind(listenningSocket, serverInfo->ai_addr, serverInfo->ai_addrlen);
	freeaddrinfo(serverInfo); // Ya no lo vamos a necesitar

	listen(listenningSocket, BACKLOG);

	return listenningSocket;
}

int aceptarCliente(int serverSocket) {
	struct sockaddr_in addr;			// Esta estructura contendra los datos de la conexion del cliente. IP, puerto, etc.
	socklen_t addrlen = sizeof(addr);

		int socketCliente = accept(serverSocket, (struct sockaddr *) &addr, &addrlen);
		printf("Cliente conectado. Esperando mensajes: \n");

		return socketCliente;
}


void llenarPaqueteRequest(t_Package_Request *package,char* msg){
	// Me guardo los datos del usuario y el mensaje que manda
 	// Me guardo lugar para el \0
	char** spliteado = string_n_split(msg,2," ");
	package->header=spliteado[0];
	package->query=spliteado[1];
	//printf("%s", package->query);
	//(package->header)[strlen(package->header)] = '\0';
	// Si, este ultimo valor es calculable. Pero a fines didacticos la calculo aca y la guardo a futuro, ya que no se modificara en otro lado.
}


void dispose_package(char **package){
	free(*package);
}

