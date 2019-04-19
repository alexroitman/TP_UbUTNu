
/*
 * Memoria.h
 *
 *  Created on: 11 abr. 2019
 *      Author: utnso
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <pthread.h>
#include <sock/sockets-lib.h>

#ifndef MEMORIA_MEMORIA_H_
#define MEMORIA_MEMORIA_H_
#define IP "127.0.0.1"
#define PUERTO "6666"
#define BACKLOG 5			// Define cuantas conexiones vamos a mantener pendientes al mismo tiempo
#define PACKAGESIZE 1024	// Define cual va a ser el size maximo del paquete a enviar

typedef struct _t_Package {

	char* username;
	uint32_t username_long;
	char* message;
	uint32_t message_long;
	char* funcionalidad;
	uint32_t funcionalidad_long;
	char* restoString;
	uint32_t restoString_long;
	uint32_t total_size;			// NOTA: Es calculable. Aca lo tenemos por fines didacticos!
} t_Package;

typedef struct {
	char* table;
	uint32_t key;
} t_select;

typedef struct {
	char* table;
	char* value;
	uint32_t timestap;
}t_Insert;

typedef struct {
	char* table;
	char* tipoConsistencia;
	uint32_t particiones;
	uint32_t tiempoCompactacion;

}t_Create;

typedef struct {
	char* table;
}t_Drop;

typedef struct {
	char* table;
}t_Describe;


char package[PACKAGESIZE];
struct addrinfo hints;
struct addrinfo *serverInfo;

void recibirMensajeDeKernel();

void enviarMensajeAKernel();

int levantarCliente();

int levantarServidor();
void cerrarConexiones();

int recieve_and_deserialize(t_Package *package, int socketCliente);


#endif /* MEMORIA_MEMORIA_H_ */
