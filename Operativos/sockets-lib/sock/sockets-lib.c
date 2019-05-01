/*
 * sockets-lib.c
 *
 *  Created on: 16 abr. 2019
 *      Author: utnso
 */

#include "sockets-lib.h"

#include <stdio.h>
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
/*
void enviarMensaje(int clienteSocket) {
	int enviar = 1;
	char message[PACKAGESIZE];
	t_Package_Request package;
	char *serializedPackage;
	printf("Conectado al servidor. Bienvenido al sistema, ya puede enviar mensajes. Escriba 'exit' para salir\n");

	while (enviar) {
		fgets(message, PACKAGESIZE, stdin);	// Lee una linea en el stdin (lo que escribimos en la consola) hasta encontrar un \n (y lo incluye) o llegar a PACKAGESIZE.
		if (!strcmp(message, "exit\n"))
			enviar = 0;			// Chequeo que el usuario no quiera salir
		llenarPaqueteRequest(&package,message);
		if (enviar)
			serializedPackage =serializarRequest(&package);
			send(clienteSocket, serializedPackage, package.total_size, 0); // Solo envio si el usuario no quiere salir.

			dispose_package(&serializedPackage);
			//recv(clienteSocket, (void*) message, PACKAGESIZE, 0);
	}
}*/

int aceptarCliente(int serverSocket) {
	struct sockaddr_in addr;			// Esta estructura contendra los datos de la conexion del cliente. IP, puerto, etc.
	socklen_t addrlen = sizeof(addr);

		int socketCliente = accept(serverSocket, (struct sockaddr *) &addr, &addrlen);
		printf("Cliente conectado. Esperando mensajes:\n");
		recibirMensaje(socketCliente);
		return socketCliente;
}

int enviarPaquete(int clienteSocket,tPaquete* paquete_a_enviar){
	return send(clienteSocket,(tPaquete*) paquete_a_enviar,paquete_a_enviar->length,0);
}

int recibirPaquete(int socketReceptor,char** serializado){
	return recv(socketReceptor,&(serializado),21,MSG_WAITALL);
}
/*
void recibirMensaje(int socketServidor){

	int status = 1;		// Estructura que manjea el status de los recieve.

	t_Package_Request package;
	while (status != 0) {
		int status;
			int buffer_size;
			char *buffer = malloc(buffer_size = sizeof(uint32_t));

			uint32_t header_long;
			status = recv(socketServidor, buffer, sizeof(package.message_long_header), 0);
			memcpy(&(header_long), buffer, buffer_size);
			printf("header long: %d",header_long);
			if (!status) status=0;

			char* header = malloc(package.message_long_header);
			status = recv(socketServidor, header, header_long, 0);
			package.header = header;
			printf("Header que recibii: %s",package.header);
			free(header);
			if (!status) status=0;

			uint32_t message_long_query;
			status = recv(socketServidor, buffer, sizeof(package.message_long_query), 0);
			memcpy(&(message_long_query), buffer, buffer_size);
			if (!status) status=0;

			status = recv(socketServidor, package.query, message_long_query, 0);
			printf("Query que recibi: %s",package.query);
			if (!status) status=0;


			free(buffer);
			sleep(3);
		if (status != 0)
		send(socketServidor, "Recibi tu msg",14, 0);
	}

}*/
tPaquete* serializarRequest(t_Package_Request package){

	tPaquete *serializedPackage = malloc(sizeof(package));

	int offset = 0;
	int size_to_send;

	size_to_send =  strlen(package.header)+1;
	memcpy(serializedPackage->payload + offset, package.header, size_to_send);
	offset += size_to_send;

	size_to_send =  strlen(package.query)+1;
	memcpy(serializedPackage->payload + offset, package.query, size_to_send);
	serializedPackage->length = offset + size_to_send;

	return serializedPackage;
}

t_Package_Request* desSerializarRequest(char** packSerializado){
	t_Package_Request *tpack = malloc(strlen(packSerializado)*sizeof(char));
	int offset=0;
	int size_to_send;
	for(size_to_send=1;(packSerializado+offset)[size_to_send-1]!= '\0';size_to_send++);
	tpack->header=malloc(size_to_send);
	memcpy(tpack->header,packSerializado+offset,size_to_send);
	for(size_to_send=1;(packSerializado+offset)[size_to_send-1]!= '\0';size_to_send++);
	tpack->query=malloc(size_to_send);
	memcpy(tpack->query,packSerializado+offset,size_to_send);
	return tpack;
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

