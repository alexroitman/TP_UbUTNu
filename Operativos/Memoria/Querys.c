/*
 * Querys.c
 *
 *  Created on: 7 jun. 2019
 *      Author: utnso
 */

#include "Querys.h"


void pedirRegistroALFS(int socket, tSelect* packSelect, tRegistroRespuesta* reg) {
	char* selectAEnviar = serializarSelect(packSelect);
	enviarPaquete(socket, selectAEnviar, packSelect->length);

	type header = leerHeader(socket);
	desSerializarRegistro(reg, socket);
	reg->tipo = REGISTRO;
}


void enviarRegistroAKernel(tRegistroRespuesta* reg, int socket,bool leyoConsola){
	//SI LEYO DE CONSOLA NO QUIERO ENVIARSELO A KERNEL
	if(!leyoConsola){
		char* registroSerializado = serializarRegistro(reg);
		enviarPaquete(socket, registroSerializado,reg->length);
		log_debug(logger, "Value enviado a Kernel");
	}
}

void* leerQuery(void* params) {
    tHiloConsola* parametros =(tHiloConsola*) params;
	leyoConsola = false;
	fgets(parametros->consulta, 256, stdin);
	log_debug(logger,"lei de consola");
	char** tempSplit;
	tempSplit = string_n_split(parametros->consulta, 2, " ");
	if(!strcmp(tempSplit[0],"SELECT")){
		log_debug(logger,"compara piola");
		*(parametros->header) = SELECT;
	}
	if(!strcmp(tempSplit[0],"INSERT")){
		*(parametros->header) = INSERT;
	}
	if(!strcmp(tempSplit[0],"CREATE")){
		*(parametros->header) = CREATE;
	}
	leyoConsola = true;
	sem_post(&semConsulta);
}



