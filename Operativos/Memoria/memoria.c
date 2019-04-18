/*
 * pmemoria.c
 *
 *  Created on: 18 abr. 2019
 *      Author: utnso
 */


#include <sock/sockets-lib.h>
#define IP "127.0.0.1"
#define PUERTOESCUCHAKERNEL "2000"
#define PUERTOESCUCHALFS "2001"
#include <pthread.h>
int main(){
	pthread_t hilo_kernel;
	pthread_t hilo_lfs;
	int socket_kernel = levantarServidor(PUERTOESCUCHAKERNEL);
	int socket_lfs = levantarServidor(PUERTOESCUCHALFS);
	socket_kernel = pthread_create(&hilo_kernel, NULL, (void*) aceptarCliente,
			(int*) socket_kernel);
	pthread_create(&hilo_kernel, NULL, (void*) recibirMensaje,
				(int*) socket_kernel);
	socket_lfs = pthread_create(&hilo_lfs, NULL, (void*) aceptarCliente,
				(int*) socket_lfs);
	pthread_create(&hilo_lfs, NULL, (void*) recibirMensaje,
					(int*) socket_lfs);
	pthread_join(hilo_kernel,NULL);
	pthread_join(hilo_lfs,NULL);
	close(socket_kernel);
	close(socket_lfs);
	return 0;
}
