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
	tSelect paqueteSelect;
	paqueteSelect.type=SELECT;
	paqueteSelect.key=4;
	paqueteSelect.nombre_tabla= "HOLA\0";
	paqueteSelect.nombre_tabla_long= strlen(paqueteSelect.nombre_tabla);
	paqueteSelect.length=sizeof(paqueteSelect.type) +sizeof(paqueteSelect.nombre_tabla_long)+paqueteSelect.nombre_tabla_long+sizeof(paqueteSelect.key);


	char* respuesta= serializarSelect(&paqueteSelect);
	printf("%c",respuesta);

	enviarPaquete(socket_memoria,respuesta,paqueteSelect.length);

	//close(socket_memoria);
}
