
/*
 * Memoria.h
 *
 *  Created on: 11 abr. 2019
 *      Author: utnso
 */

#ifndef MEMORIA_MEMORIA_H_
#define MEMORIA_MEMORIA_H_
#define IP "127.0.0.1"
#define PUERTO "6666"
#define BACKLOG 5			// Define cuantas conexiones vamos a mantener pendientes al mismo tiempo
#define PACKAGESIZE 1024	// Define cual va a ser el size maximo del paquete a enviar
char package[PACKAGESIZE];
struct addrinfo hints;
struct addrinfo *serverInfo;

void recibirMensajeDeKernel();

void enviarMensajeAKernel();

int levantarCliente();

int levantarServidor();
void cerrarConexiones();

#endif /* MEMORIA_MEMORIA_H_ */
