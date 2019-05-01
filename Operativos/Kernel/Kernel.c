/*
 * Kernel.c
 *
 *  Created on: 11 abr. 2019
 *      Author: utnso
 */

#include "Kernel.h"


#define IP "127.0.0.1"
#define PUERTOMEM "7878"
#define BACKLOG 5			// Define cuantas conexiones vamos a mantener pendientes al mismo tiempo
#define PACKAGESIZE 1024	// Define cual va a ser el size maximo del paquete a enviar
char package[PACKAGESIZE];
struct addrinfo hints;
struct addrinfo *serverInfo;

// Define cual va a ser el size maximo del paquete a enviar

int main() {
	int socket_memoria = levantarCliente(PUERTOMEM,IP);
	t_Package_Request paqueteRequest;
	paqueteRequest.header="SELECT";
	paqueteRequest.query = "HOLA";
	tPaquete* miPaquete;

	miPaquete=serializarRequest(paqueteRequest);
	int bytes = enviarPaquete(socket_memoria,miPaquete);
	close(socket_memoria);
}
