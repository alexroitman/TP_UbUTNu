/*
 * Kernel.c
 *
 *  Created on: 11 abr. 2019
 *      Author: utnso
 */

#include </home/utnso/workspace/tp-2019-1c-UbUTNu/tp-libs/sockets-lib.h>

#define IP "127.0.0.1"
#define PUERTO "2001"
#define PUERTOESCRIBE "2000"
#include <pthread.h>


// Define cual va a ser el size maximo del paquete a enviar

int main() {
	//char* consulta = leerConsola();
	int socket_servidor = levantarServidor(PUERTO);
	int socket_cliente = levantarCliente(PUERTOESCRIBE,IP);
	pthread_t hilo_Server;
	pthread_t hilo_Cliente;
	pthread_create(&hilo_Server, NULL, (void*) recibirMensaje,
			(int*) socket_servidor);
	pthread_create(&hilo_Cliente, NULL, (void*) enviarMensaje,
			(int*) socket_cliente);
	pthread_join(hilo_Server,NULL);
	pthread_join(hilo_Cliente,NULL);
	close(socket_cliente);
	close(socket_servidor);
	return 0;
}
