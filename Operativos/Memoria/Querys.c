/*
 * Querys.c
 *
 *  Created on: 7 jun. 2019
 *      Author: utnso
 */

#include "Querys.h"



void ejecutarConsulta() {
	encontroSeg = -1;
	indexPag = -1;
	tPagina* pagina = malloc(sizeof(tPagina));
	pagina->value = malloc(tamanioMaxValue);
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
			indexPag = buscarPaginaEnMemoria(packSelect->key,miSegmento ,pagTabla,pagina);
		}
		tRegistroRespuesta* reg = malloc(sizeof(tRegistroRespuesta));
		reg->value = malloc(tamanioMaxValue);
		if (indexPag >= 0) {
			log_debug(logger, "Encontre pagina buscada");
			//*pagina = *(tPagina*) (memoria + pagTabla->offsetMemoria);
			reg->tipo = REGISTRO;
			reg->timestamp = pagina->timestamp;
			strcpy(reg->value,pagina->value);
			reg->key = pagina->key;
			reg->value_long = strlen(pagina->value) + 1;
			enviarRegistroAKernel(reg, socket_kernel, leyoConsola);
			log_debug(logger, "El value es: %s", pagina->value);
		} else {
			pedirRegistroALFS(socket_lfs, packSelect, reg);
			if (reg->key != -1) {
				pagina->key = reg->key;
				pagina->timestamp = reg->timestamp;
				strcpy(pagina->value, reg->value);
				if (encontroSeg != 1) {
					cargarSegmentoEnTabla(packSelect->nombre_tabla,
							tablaSegmentos);
					miSegmento = obtenerUltimoSegmentoDeTabla(tablaSegmentos);
				}
				int error = agregarPaginaAMemoria(miSegmento, pagina);
				send(socket_kernel, &error, sizeof(error), 0);
			}else{
				log_error(logger,"No existe el registro");
			}
			//SI NO LO ENCONTRO IGUALMENTE SE LO MANDO A KERNEL PARA QUE TAMBIEN MANEJE EL ERROR
			enviarRegistroAKernel(reg, socket_kernel, leyoConsola);


		}
		free(reg->value);
		free(reg);
		free(packSelect);
		break;
	case INSERT:
		packInsert = malloc(sizeof(tInsert));
		int error = 1;
		cargarPackInsert(packInsert, leyoConsola, paramsConsola->consulta);
		encontroSeg = buscarSegmentoEnTabla(packInsert->nombre_tabla,
				miSegmento, tablaSegmentos);
		if (encontroSeg == 1) {
			log_debug(logger, "Encontre segmento %s ",
					packInsert->nombre_tabla);

			indexPag = buscarPaginaEnMemoria(packInsert->key, miSegmento,
					pagTabla,pagina);
			if (indexPag >= 0) {
				log_debug(logger,
						"Encontre la pagina buscada en el segmento %s ",
						packInsert->nombre_tabla);
				actualizarPaginaEnMemoria(miSegmento, indexPag, packInsert->value);


			} else {
				log_debug(logger,
						"Encontro el segmento en tabla pero no tiene la pagina en memoria");
				pagina->key = packInsert->key;
				pagina->timestamp = (int) time (NULL);
				strcpy(pagina->value,packInsert->value);
				error = agregarPaginaAMemoria(miSegmento,pagina);

			}

		} else {
			//No encontro el segmento en tabla de segmentos
			log_debug(logger, "No encontro el segmento en tabla de segmentos");
			pagina->key = packInsert->key;
			pagina->timestamp = (int) time(NULL);
			strcpy(pagina->value, packInsert->value);
			cargarSegmentoEnTabla(packInsert->nombre_tabla, tablaSegmentos);
			miSegmento = obtenerUltimoSegmentoDeTabla(tablaSegmentos);
			error = agregarPaginaAMemoria(miSegmento,pagina);

		}

		send(socket_kernel, &error, sizeof(error), 0); //Aviso a Kernel que onda con el insert
															//1 salio todo joya
															//-1 hubo error
		if (error == -1) {
			int errorLRU;
			errorLRU = ejecutarLRU();
			if (errorLRU == 1) {
				agregarPaginaAMemoria(miSegmento, pagina);
			}
		}

		}
		free(packInsert);
		free(packInsert->nombre_tabla);
		free(packInsert->value);

		break;
	case CREATE:
		packCreate = malloc(sizeof(tCreate));
		cargarPackCreate(packCreate, leyoConsola, paramsConsola->consulta);
		char* createAEnviar = serializarCreate(packCreate);
		enviarPaquete(socket_lfs, createAEnviar, packCreate->length);
		log_debug(logger, "Mando la consulta a LFS");
		free(packCreate);
		free(createAEnviar);
		break;

	case DROP:
		packDrop = malloc(sizeof(tDrop));
		cargarPackDrop(packDrop, leyoConsola, paramsConsola->consulta);
		packDrop->type = DROP;
		log_debug(logger, "LLEGO UN DROP");
		log_debug(logger, "Drop Tabla: %s", packDrop->nombre_tabla);
		encontroSeg = buscarSegmentoEnTabla(packDrop->nombre_tabla, miSegmento, tablaSegmentos);
		if(encontroSeg == 1){
			log_debug(logger, "Encontre segmento: %s", packDrop->nombre_tabla);
			liberarPaginasDelSegmento(miSegmento, tablaSegmentos);
			log_debug(logger, "Segmento eliminado");
			char* serializado = serializarDrop(packDrop);
			enviarPaquete(socket_lfs, serializado, packDrop->length);
			log_debug(logger, "Envio DROP a LFS");
		} else {
			log_error(logger, "No se encontro el segmento");
		}

		break;
	case JOURNAL:
		packJournal = malloc(sizeof(tJournal));
		cargarPackJournal(packJournal, leyoConsola, paramsConsola->consulta);
		log_debug(logger, "Se realizara el JOURNAL");
		ejecutarJournal();
		log_debug(logger, "JOURNAL finalizado");
		break;
	case DESCRIBE:
		packDescribe = malloc(sizeof(tDescribe));
		packDescResp = malloc(sizeof(t_describe));
		desSerializarDescribe(packDescribe, socket_kernel);
		char* serializado = serializarDescribe(packDescribe);
		enviarPaquete(socket_kernel,serializado,packDescribe->length);
		type header = leerHeader(socket_lfs);
		desserializarDescribe_Response(packDescResp,socket_lfs);
		char* respSerializada = serializarDescribe_Response(packDescResp);
		int length = packDescResp->cant_tablas * sizeof(t_metadata) + sizeof(uint16_t);
		enviarPaquete(socket_kernel,respSerializada,length);
		free(packDescResp->tablas);
		free(packDescResp);
		free(packDescribe->nombre_tabla);
		free(packDescribe);
		break;
	case NIL:
		log_error(logger, "No entendi la consulta");
		break;

	}

	free(miSegmento);
	free(pagina);
	free(pagTabla);
	free(pagina->value);

}

void pedirRegistroALFS(int socket, tSelect* packSelect, tRegistroRespuesta* reg) {
	char* selectAEnviar = serializarSelect(packSelect);
	enviarPaquete(socket, selectAEnviar, packSelect->length);

	type header = leerHeader(socket);
	if (header == REGISTRO) {
		desSerializarRegistro(reg, socket);
		reg->tipo = REGISTRO;
	}

}

void enviarRegistroAKernel(tRegistroRespuesta* reg, int socket,
		bool leyoConsola) {
	//SI LEYO DE CONSOLA NO QUIERO ENVIARSELO A KERNEL
	if (!leyoConsola) {
		char* registroSerializado = serializarRegistro(reg);
		enviarPaquete(socket, registroSerializado, reg->length);
		log_debug(logger, "Value enviado a Kernel");
	}
}

void* leerQuery(void* params) {
	while (1) {
		tHiloConsola* parametros = (tHiloConsola*) params;
		leyoConsola = false;
		fgets(parametros->consulta, 256, stdin);
		char** tempSplit;
		tempSplit = string_n_split(parametros->consulta, 2, " ");
		if (!strcmp(tempSplit[0], "SELECT")) {
			*(parametros->header) = SELECT;
		}
		if (!strcmp(tempSplit[0], "INSERT")) {
			*(parametros->header) = INSERT;
		}
		if (!strcmp(tempSplit[0], "CREATE")) {
			*(parametros->header) = CREATE;
		}
		if(!strcmp(tempSplit[0], "DROP")){
			*(parametros->header) = DROP;
		}
		if(!strcmp(tempSplit[0], "JOURNAL")){
			*(parametros->header) = JOURNAL;
		}
		leyoConsola = true;
		free(tempSplit[0]);
		free(tempSplit[1]);
		free(tempSplit);
		ejecutarConsulta();

	}
}


void cargarPackSelect(tSelect* packSelect,bool leyoConsola,char consulta[]){
	if(leyoConsola){
		cargarPaqueteSelect(packSelect, consulta);
	}else{
		desSerializarSelect(packSelect, socket_kernel);
	}

}

void cargarPackInsert(tInsert* packInsert, bool leyoConsola, char consulta[]) {
	if (leyoConsola) {
		cargarPaqueteInsert(packInsert, consulta);
	} else {
		desSerializarInsert(packInsert, socket_kernel);
	}

}

void cargarPackCreate(tCreate* packCreate,bool leyoConsola,char consulta[]){
	if(leyoConsola){
		cargarPaqueteCreate(packCreate, consulta);
	}else{
		desSerializarCreate(packCreate, socket_kernel);
	}

}
void cargarPackDrop(tDrop* packDrop, bool leyoConsola, char consulta[]){
	if(leyoConsola){
		cargarPaqueteDrop(packDrop, consulta);
	} else {
		desSerializarDrop(packDrop, socket_kernel);
	}
}


void cargarPackJournal(tJournal* packJournal, bool leyoConsola, char consulta[]){
	if(leyoConsola){
		cargarPaqueteJournal(packJournal, consulta);
	} else {
		desSerializarJournal(packJournal, socket_kernel);
	}
}
int handshakeLFS(int socket_lfs){
	int buffer;
	recv(socket_lfs,&buffer,4,MSG_WAITALL);
	return buffer;
}
