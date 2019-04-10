/*
 * Kernel.h
 *
 *  Created on: 9 abr. 2019
 *      Author: utnso
 */

#ifndef KERNEL_KERNEL_H_
#define KERNEL_KERNEL_H_

#define PUERTO "6666"
#define BACKLOG 5			// Define cuantas conexiones vamos a mantener pendientes al mismo tiempo
#define PACKAGESIZE 1024	// Define cual va a ser el size maximo del paquete a enviar
char package[PACKAGESIZE];


#endif /* KERNEL_KERNEL_H_ */
