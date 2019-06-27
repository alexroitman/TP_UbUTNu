/*
 * Querys.h
 *
 *  Created on: 7 jun. 2019
 *      Author: utnso
 */

#ifndef QUERYS_H_
#define QUERYS_H_
#include "Memoria.h"
void ejecutarConsulta(void* memoria);
void pedirRegistroALFS(int socket,tSelect* packSelect,tRegistroRespuesta* reg);
void enviarRegistroAKernel(tRegistroRespuesta* reg, int socket,bool leyoConsola);
void* leerQuery(void* params);
void cargarPackCreate(tCreate* packCreate,bool leyoConsola,char consulta[]);
void cargarPackSelect(tSelect* packSelect,bool leyoConsola,char* consulta);
void cargarPackInsert(tInsert* packInsert, bool leyoConsola, char consulta[]);
void cargarPackDrop(tDrop* packDrop, bool leyoConsola, char consulta[]);
void cargarPackJournal(tJournal* packJournal, bool leyoConsola, char consulta[]);
#endif /* QUERYS_H_ */
