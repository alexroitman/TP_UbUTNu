/*
 * Querys.c
 *
 *  Created on: 7 jun. 2019
 *      Author: utnso
 */

#include "Querys.h"



void ejecutarConsulta(void* memoria) {
	typedef struct{
			int timestamp;
			u_int16_t key;
			char value[20];
			//Valor hardcodeado, cuando haya handshake con lfs lo tengo que sacar de ahi el tamanio max
	}tPagina;

	tPagina* pagina = malloc(sizeof(tPagina));
	tSegmento* miSegmento = malloc(sizeof(tSegmento));
	elem_tabla_pag* pagTabla = malloc(sizeof(elem_tabla_pag));
	encontroSeg = -1;
	indexPag = -1;
	switch (*header) {
	case SELECT:
		packSelect = malloc(sizeof(tSelect));
		cargarPackSelect(packSelect, leyoConsola, paramsConsola->consulta);
		encontroSeg = buscarSegmentoEnTabla(packSelect->nombre_tabla,
				miSegmento, tablaSegmentos);
		if (encontroSeg == 1) {
			log_debug(logger, "Encontre segmento: %s ",
					packSelect->nombre_tabla);
			indexPag = buscarPaginaEnMemoria(packSelect->key, miSegmento,
					pagTabla);
		}
		tRegistroRespuesta* reg = malloc(sizeof(tRegistroRespuesta));
		if (indexPag >= 0) {
			log_debug(logger, "Encontre pagina buscada");
			*pagina = *(tPagina*) (memoria + pagTabla->offsetMemoria);
			reg->tipo = REGISTRO;
			reg->timestamp = pagina->timestamp;
			reg->value = pagina->value;
			reg->key = pagina->key;
			reg->value_long = strlen(pagina->value) + 1;
			enviarRegistroAKernel(reg, socket_kernel, leyoConsola);
			log_debug(logger, "El value es: %s", pagina->value);
		} else {
			pedirRegistroALFS(socket_lfs, packSelect, reg);
			if (reg->key != -1) {
				char value[20];
				strcpy(value, reg->value);
				if (encontroSeg != 1) {
					cargarSegmentoEnTabla(packSelect->nombre_tabla,
							tablaSegmentos);
					miSegmento = obtenerUltimoSegmentoDeTabla(tablaSegmentos);
				}
				agregarPaginaAMemoria(miSegmento, reg->key, reg->timestamp,
						value);

			}
			//SI NO LO ENCONTRO IGUALMENTE SE LO MANDO A KERNEL PARA QUE TAMBIEN MANEJE EL ERROR
			enviarRegistroAKernel(reg, socket_kernel, leyoConsola);

			free(reg);

		}

		free(packSelect);
		log_debug(logger,"sale del case");
		break;
	case INSERT:
		packInsert = malloc(sizeof(tInsert));
		cargarPackInsert(packInsert, leyoConsola, paramsConsola->consulta);
		char value[20];
		strcpy(value, packInsert->value);
		encontroSeg = buscarSegmentoEnTabla(packInsert->nombre_tabla,
				miSegmento, tablaSegmentos);

		if (encontroSeg == 1) {
			log_debug(logger, "Encontre segmento %s ",
					packInsert->nombre_tabla);

			indexPag = buscarPaginaEnMemoria(packInsert->key, miSegmento,
					pagTabla);
			if (indexPag >= 0) {
				log_debug(logger,
						"Encontre la pagina buscada en el segmento %s ",
						packInsert->nombre_tabla);
				actualizarPaginaEnMemoria(miSegmento, indexPag, packInsert->key,
						value);

			} else {
				//Encontro el segmento en tabla pero no tiene la pagina en memoria

				log_debug(logger,
						"Encontro el segmento en tabla pero no tiene la pagina en memoria");
				agregarPaginaAMemoria(miSegmento, packInsert->key,
						(int) time(NULL), value);
			}

		} else {
			//No encontro el segmento en tabla de segmentos
			log_debug(logger, "No encontro el segmento en tabla de segmentos");
			cargarSegmentoEnTabla(packInsert->nombre_tabla, tablaSegmentos);
			tSegmento* newSeg = obtenerUltimoSegmentoDeTabla(tablaSegmentos);
			agregarPaginaAMemoria(newSeg, packInsert->key, (int) time(NULL),
					value);
		}
		free(packInsert);
		break;
	case CREATE:
		packCreate = malloc(sizeof(tCreate));
		cargarPackCreate(packCreate, leyoConsola, paramsConsola->consulta);
		char* createAEnviar = serializarCreate(packCreate);
		enviarPaquete(socket_lfs, createAEnviar, packCreate->length);
		log_debug(logger, "Mando la consulta a LFS");
		free(packCreate);
		break;
	case NIL:
		log_error(logger, "No entendi la consulta");
		break;
	}

	free(miSegmento);
	free(pagina);
	free(pagTabla);

}





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
	while (1) {
		tHiloConsola* parametros = (tHiloConsola*) params;
		leyoConsola = false;
		fgets(parametros->consulta, 256, stdin);
		log_debug(logger, "lei de consola");
		char** tempSplit;
		tempSplit = string_n_split(parametros->consulta, 2, " ");
		if (!strcmp(tempSplit[0], "SELECT")) {
			log_debug(logger, "compara piola");
			*(parametros->header) = SELECT;
		}
		if (!strcmp(tempSplit[0], "INSERT")) {
			*(parametros->header) = INSERT;
		}
		if (!strcmp(tempSplit[0], "CREATE")) {
			*(parametros->header) = CREATE;
		}
		leyoConsola = true;
		ejecutarConsulta(memoria);
	}
}



