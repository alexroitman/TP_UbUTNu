/*
 * Memoria.c
 *
 *  Created on: 11 abr. 2019
 *      Author: utnso
 */

#include "Memoria.h"
#include "Querys.h"
#define IP "127.0.0.1"
#define PUERTOKERNEL "7878"
#define BACKLOG 5			// Define cuantas conexiones vamos a mantener pendientes al mismo tiempo
#define PACKAGESIZE 1024	// Define cual va a ser el size maximo del paquete a enviar
char package[PACKAGESIZE];
struct addrinfo hints;
struct addrinfo *serverInfo;
#define TAMANIOMAXVALUE 10
#define PUERTOLFS "7879"
int socket_kernel;
pthread_t hiloConsola;
pthread_t hiloSocket;
// Define cual va a ser el size maximo del paquete a enviar

int main() {
	logger = log_create("Memoria.log", "Memoria.c", 1, LOG_LEVEL_DEBUG);
	t_miConfig* miConfig;
	miConfig = cargarConfig();
	log_debug(logger, "Levanta archivo de config");
	int socket_sv = levantarServidor((char*) miConfig->puerto_kernel);
	socket_kernel = aceptarCliente(socket_sv);
	log_debug(logger, "Levanta conexion con kernel");
	int socket_lfs = levantarCliente((char*) miConfig->puerto_fs,miConfig->ip_fs);
	log_debug(logger, "Levanta conexion con LFS");
	sem_init(&semConsulta,0,0);
	void* memoria = malloc(miConfig->tam_mem);
	t_list* tablaSegmentos = list_create();

	int encontroSeg = -1;
	int indexPag = -1;
	type* header = malloc(sizeof(type));
	tHiloConsola* paramsConsola;
	paramsConsola->header = header;
	tSelect* packSelect;
	tInsert* packInsert;
	tCreate* packCreate;
	leyoConsola = true;
	recibioSocket = true;
	while (1) {
		*header = NIL;
		crearHilosRecepcion(header,hiloSocket,hiloConsola,paramsConsola);
		tPagina* pagina = malloc(sizeof(tPagina));
		tSegmento* miSegmento = malloc(sizeof(tSegmento));
		encontroSeg = -1;
		indexPag = -1;
		sem_wait(&semConsulta);
		switch (*header) {
		case SELECT:
				packSelect = malloc(sizeof(tSelect));
				cargarPackSelect(packSelect,leyoConsola,paramsConsola->consulta);
				encontroSeg = buscarSegmentoEnTabla(packSelect->nombre_tabla,
						miSegmento, tablaSegmentos);
				if (encontroSeg == 1) {
					log_debug(logger, "Encontre segmento: %s ",
							packSelect->nombre_tabla);
					indexPag = buscarPaginaEnMemoria(packSelect->key,
							miSegmento, memoria, pagina);
				}
				if (indexPag >= 0) {
					log_debug(logger, "Encontre pagina buscada");
					enviarPagina(socket_kernel, pagina,leyoConsola);
					log_debug(logger, "El value es: %s", pagina->value);
				} else {
					tRegistroRespuesta* reg = malloc(
							sizeof(tRegistroRespuesta));
					pedirRegistroALFS(socket_lfs, packSelect, reg);
					if (reg->key != -1) {
						tPagina pagAux;
						pagAux.key = reg->key;
						pagAux.timestamp = reg->timestamp;
						pagAux.value = reg->value;
						if (encontroSeg != 1) {
							cargarSegmentoEnTabla(packSelect->nombre_tabla,
									tablaSegmentos);
							miSegmento = obtenerUltimoSegmentoDeTabla(
									tablaSegmentos);
						}
						agregarPaginaAMemoria(miSegmento, pagAux, memoria,
								miConfig->tam_mem);

					}
					//SI NO LO ENCONTRO IGUALMENTE SE LO MANDO A KERNEL PARA QUE TAMBIEN MANEJE EL ERROR
					enviarRegistroAKernel(reg, socket_kernel,leyoConsola);

					free(reg);

				}
				free(packSelect);



			break;
		case INSERT:
			packInsert = malloc(sizeof(tInsert));
			cargarPackInsert(packInsert,leyoConsola,paramsConsola->consulta);
			tPagina pagAux;
			pagAux.key = packInsert->key;
			pagAux.timestamp = (int) time(NULL);
			pagAux.value = packInsert->value;

			encontroSeg = buscarSegmentoEnTabla(packInsert->nombre_tabla,
					miSegmento, tablaSegmentos);
			if (encontroSeg == 1) {
				log_debug(logger, "Encontre segmento %s ",
						packInsert->nombre_tabla);

				indexPag = buscarPaginaEnMemoria(packInsert->key, miSegmento,
						memoria, pagina);
				if (indexPag >= 0) {
					log_debug(logger,
							"Encontre la pagina buscada en el segmento %s ",
							packInsert->nombre_tabla);
					actualizarPaginaEnMemoria(packInsert, miSegmento, memoria,
							indexPag);

				} else {
					//Encontro el segmento en tabla pero no tiene la pagina en memoria

					log_debug(logger,
							"Encontro el segmento en tabla pero no tiene la pagina en memoria");
					agregarPaginaAMemoria(miSegmento, pagAux, memoria,
							miConfig->tam_mem);
				}

			} else {
				//No encontro el segmento en tabla de segmentos
				log_debug(logger,
						"No encontro el segmento en tabla de segmentos");
				cargarSegmentoEnTabla(packInsert->nombre_tabla, tablaSegmentos);
				tSegmento* newSeg = obtenerUltimoSegmentoDeTabla(tablaSegmentos);
				agregarPaginaAMemoria(newSeg, pagAux, memoria,
						miConfig->tam_mem);
			}
			free(packInsert);
			break;
		case CREATE:
			packCreate = malloc(sizeof(tCreate));
			cargarPackCreate(packCreate,leyoConsola,paramsConsola->consulta);
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
		pthread_detach(hiloSocket);
		pthread_detach(hiloConsola);

	}
	close(socket_lfs);
	close(socket_kernel);
	close(socket_sv);

}

void crearHilosRecepcion(type* header, pthread_t hiloSocket,
		pthread_t hiloConsola,tHiloConsola* paramsConsola) {

	//PREGUNTO PARA SABER SI EL HILO SIGUE BLOQUEADO ESPERANDO ALGO O YA TERMINO SU EJECUCION
	//SI TERNINO SU EJECUCION CREO UNO NUEVO
	//SI SIGUE BLOQUEADO ESPERANDO QUE LE LLEGUE ALGO, NO HAGO NADA
	if (recibioSocket) {
		pthread_create(&hiloSocket, NULL, (void*) recibirHeader,
				(void*) header);
	}
	if (leyoConsola) {
		pthread_create(&hiloConsola, NULL, (void*) leerQuery, (void*) paramsConsola);
	}
}

void* recibirHeader(void* arg) {
	recibioSocket = false;
	type* header = (type*) arg;
	recv(socket_kernel, &(*header), sizeof(type), MSG_WAITALL);
	recibioSocket = true;
	if(*header != NIL){
		//EN REALIDAD LO QUE ESTOY PREGUNTANDO ES SI RECIBIO ALGO DE KERNEL
		//PORQUE SI SE CORTO LA CONEXION CON KERNEL NO QUIERO QUE HAGA EL SIGNAL DEL SEMAFORO
		sem_post(&semConsulta);
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


void actualizarPaginaEnMemoria(tInsert* packInsert, tSegmento* segmento, void* memoria, int index) {
	tPagina* pag;
	elem_tabla_pag* elemTablaPag =  malloc(sizeof(elem_tabla_pag));
	elemTablaPag = (elem_tabla_pag*)list_get(segmento->tablaPaginas, index);
	int offsetMemoria = elemTablaPag->offsetMemoria;
	pag = (tPagina*) (memoria + offsetMemoria);
	pag->value = packInsert->value;
	pag->timestamp = (int) time(NULL);
	elemTablaPag->modificado = true;
	log_debug(logger, "Pagina encontrada y actualizada.");
}

tSegmento* obtenerUltimoSegmentoDeTabla(t_list* tablaSeg) {
	int cantSegmentos = list_size(tablaSeg);
	tSegmento* seg = malloc(sizeof(tSegmento*));
	*seg = *(tSegmento*) list_get(tablaSeg, cantSegmentos-1);
	return seg;
}

int agregarPaginaAMemoria(tSegmento* seg, tPagina pag, void* memoria, int tam_max) {
	int cantPags = 0;
	int cantPagsMax = tam_max / sizeof(tPagina);
	tPagina temp = *(tPagina*) memoria;
	while (temp.timestamp != 0) {
		cantPags++;
		if (cantPags <= cantPagsMax) {
			temp = *(tPagina*) (memoria + cantPags * sizeof(tPagina));
		} else {
			return -1;
			//no hay mas lugar en memoria
		}
	}
	int offset = (cantPags * sizeof(tPagina));
	*(tPagina*) (memoria + offset) = pag;


	elem_tabla_pag* pagina = malloc(sizeof(elem_tabla_pag));
	pagina->modificado = true;
	pagina->offsetMemoria = offset;
	pagina->index = list_size(seg->tablaPaginas);

	list_add(seg->tablaPaginas, (elem_tabla_pag*)pagina);
	log_debug(logger, "Pagina cargada en memoria.");
	log_debug(logger, "El value es: %s", pag.value);
	return 1;

}

void cargarSegmentoEnTabla(char* path, t_list* listaSeg) {
	tSegmento* seg = malloc(sizeof(tSegmento));
	seg->path = path;
	seg->tablaPaginas = list_create();
	list_add(listaSeg, (tSegmento*) seg);
	log_debug(logger, "Segmento cargado en tabla");
}

tSegmento* obtenerSegmentoDeTabla(t_list* tablaSeg, int index) {
	tSegmento* seg = malloc(sizeof(tSegmento*));
	*seg = *(tSegmento*) list_get(tablaSeg, index);
	return seg;
}

int buscarSegmentoEnTabla(char* nombreTabla, tSegmento* miseg, t_list* listaSegmentos) {
	bool mismoNombre(void* elemento) {
		tSegmento* seg = malloc(sizeof(tSegmento));
		seg = (tSegmento*) elemento;
		char* path = seg->path;
		return !strcmp(nombreTabla, separarNombrePath(path));
	}
	if (!list_is_empty(listaSegmentos)) {
		if (list_find(listaSegmentos, mismoNombre) != NULL) {
			*miseg = *(tSegmento*) list_find(listaSegmentos, mismoNombre);
			return 1;
		}
	}
	return -1;
}

char* separarNombrePath(char* path) {
	char** separado = string_split(path, "/");
	int i = 0;
	char* nombre;
	while (separado[i + 1] != NULL) {
		i++;
	}
	nombre = separado[i];
	/*int j = 0;
	 while(separado[j] != NULL && j != i){
	 free(separado[j]);
	 }
	 free(separado);*/
	return nombre;
}

int buscarPaginaEnMemoria(int key, tSegmento* miseg, void* memoria, tPagina* pagina) {
	elem_tabla_pag* tablaPaginas = (elem_tabla_pag*)miseg->tablaPaginas;

	bool buscarKey(void* elemento){
		elem_tabla_pag* elemPagAux;
		elemPagAux = (elem_tabla_pag*)elemento;
		tPagina* paginaAux;
		paginaAux = (tPagina*)(memoria + elemPagAux->offsetMemoria);
		return paginaAux->key == key;
	}

	if(!list_is_empty((t_list*)tablaPaginas)){
		if(list_find((t_list*)tablaPaginas, buscarKey) != NULL){
			elem_tabla_pag* miElem = (elem_tabla_pag*)list_find((t_list*)tablaPaginas, buscarKey);
			*pagina = *(tPagina*)(memoria + miElem->offsetMemoria);
			return miElem->index;
		} else {
			return -1;
		}

	} else {
		return -1;
	}

}

t_miConfig* cargarConfig() {
	char* buffer = malloc(256);
	char* pathConfig = malloc(256);
	printf("Se levanto un proceso Memoria. \nPor favor ingrese el path de su archivo de configuracion (Memoria.config): ");
	fgets(buffer, 256, stdin);
	strncpy(pathConfig, buffer, strlen(buffer)-1);
	t_miConfig* miConfig = malloc(sizeof(t_miConfig));
	t_config* config;
	config = config_create(pathConfig);
	miConfig->puerto_kernel = config_get_string_value(config, "PUERTO_KERNEL");
	miConfig->puerto_fs = config_get_string_value(config, "PUERTO_FS");
	miConfig->ip_fs = config_get_string_value(config, "IP");
	miConfig->tam_mem = config_get_int_value(config, "TAM_MEM");
	return miConfig;
}
