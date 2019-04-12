/*
 * Kernel.h
 *
 *  Created on: 11 abr. 2019
 *      Author: utnso
 */

#ifndef KERNEL_KERNEL_H_
#define KERNEL_KERNEL_H_

typedef enum { false, true } bool;
char* leerConsola(); //Lee la consulta y devuelve el string

bool analizarConsulta();

void run();

void add();



int levantarCliente();

int levantarServidor();

void enviarMensajeAMemoria();

void recibirMensajeDeMemoria();

void cerrarConexiones();


#endif /* KERNEL_KERNEL_H_ */
