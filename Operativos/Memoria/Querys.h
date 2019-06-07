/*
 * Querys.h
 *
 *  Created on: 7 jun. 2019
 *      Author: utnso
 */

#ifndef QUERYS_H_
#define QUERYS_H_
#include "Memoria.h"
void enviarPagina(int socket,tPagina* pagina);
void pedirRegistroALFS(int socket,tSelect* packSelect,tRegistroRespuesta* reg);
void enviarRegistroAKernel(tRegistroRespuesta* reg, int socket);
#endif /* QUERYS_H_ */
