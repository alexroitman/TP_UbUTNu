/*
 * Querys.c
 *
 *  Created on: 7 jun. 2019
 *      Author: utnso
 */

#include "Querys.h"

void enviarPagina(int socket,tPagina* pagina) {
	tRegistroRespuesta* registro = malloc(sizeof(tRegistroRespuesta));
	registro->tipo = REGISTRO;
	registro->timestamp = pagina->timestamp;
	registro->value = pagina->value;
	registro->key = pagina->key;
	registro->value_long = strlen(pagina->value) + 1;
	enviarRegistroAKernel(registro,socket);
	free(registro);
}

void pedirRegistroALFS(int socket, tSelect* packSelect, tRegistroRespuesta* reg) {
	char* selectAEnviar = serializarSelect(packSelect);
	enviarPaquete(socket, selectAEnviar, packSelect->length);

	type header = leerHeader(socket);
	desSerializarRegistro(reg, socket);
	reg->tipo = REGISTRO;
}


void enviarRegistroAKernel(tRegistroRespuesta* reg, int socket){
	char* registroSerializado = serializarRegistro(reg);
	enviarPaquete(socket, registroSerializado,reg->length);
}
