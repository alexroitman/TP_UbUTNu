/*
 * lfs.c
 *
 *  Created on: 18 abr. 2019
 *      Author: utnso
 */

#include <sock/sockets-lib.h>
#define IP "127.0.0.1"
#define PUERTOESCRIBE "2001"
#include <pthread.h>
int main(){
	int mi_socket = levantarCliente(PUERTOESCRIBE,IP);
	enviarMensaje(mi_socket);
	close(mi_socket);
	return 0;
}


