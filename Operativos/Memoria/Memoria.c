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
#define PUERTOLFS "7879"
pthread_t hiloConsola;
pthread_t hiloSocket;
t_miConfig* miConfig;

// Define cual va a ser el size maximo del paquete a enviar

int main() {
	typedef struct{
		int timestamp;
		u_int16_t key;
		char value[20];
		//Valor hardcodeado, cuando haya handshake con lfs lo tengo que sacar de ahi el tamanio max
	}tPagina;
	signal(SIGINT, finalizarEjecucion);
	logger = log_create("Memoria.log", "Memoria.c", 1, LOG_LEVEL_DEBUG);
	miConfig = cargarConfig();
	log_debug(logger, "Levanta archivo de config");
	socket_sv = levantarServidor((char*) miConfig->puerto_kernel);
	socket_kernel = aceptarCliente(socket_sv);
	log_debug(logger, "Levanta conexion con kernel");
	socket_lfs = levantarCliente((char*) miConfig->puerto_fs,miConfig->ip_fs);
	log_debug(logger, "Levanta conexion con LFS");
	sem_init(&semConsulta,0,0);
	memoria = malloc(miConfig->tam_mem);
	tablaSegmentos = list_create();
	header = malloc(sizeof(type));
	leyoConsola = true;
	recibioSocket = true;
	*header = NIL;
	encontroSeg = -1;
	indexPag = -1;
	paramsConsola = malloc(sizeof(tHiloConsola));
	paramsConsola->header = header;
	pthread_create(&hiloSocket, NULL, (void*) recibirHeader, (void*) header);
	pthread_create(&hiloConsola, NULL, (void*) leerQuery,
			(void*) paramsConsola);
	pthread_join(hiloSocket, NULL);
	pthread_join(hiloConsola, NULL);


}

void* recibirHeader(void* arg) {
	while (1) {
		recibioSocket = false;
		type* header = (type*) arg;
		recv(socket_kernel, &(*header), sizeof(type), MSG_WAITALL);
		recibioSocket = true;
		if (*header != NIL) {
			//EN REALIDAD LO QUE ESTOY PREGUNTANDO ES SI RECIBIO ALGO DE KERNEL
			//PORQUE SI SE CORTO LA CONEXION CON KERNEL NO QUIERO QUE HAGA EL SIGNAL DEL SEMAFORO
			ejecutarConsulta(memoria);
		}
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


void actualizarPaginaEnMemoria(tSegmento* segmento,int index, u_int16_t key, char value[20]) {
	typedef struct{
			int timestamp;
			u_int16_t key;
			char value[20];
			//Valor hardcodeado, cuando haya handshake con lfs lo tengo que sacar de ahi el tamanio max
	}tPagina;
	elem_tabla_pag* elemTablaPag =  malloc(sizeof(elem_tabla_pag));
	elemTablaPag = (elem_tabla_pag*)list_get(segmento->tablaPaginas, index);
	int offsetMemoria = elemTablaPag->offsetMemoria;
	((tPagina*) (memoria + offsetMemoria))->key = key;
	strcpy(((tPagina*) (memoria + offsetMemoria))->value,value);

	((tPagina*) (memoria + offsetMemoria))->timestamp = (int) time(NULL);
	elemTablaPag->modificado = true;
	log_debug(logger, "Pagina encontrada y actualizada.");
}

tSegmento* obtenerUltimoSegmentoDeTabla(t_list* tablaSeg) {
	int cantSegmentos = list_size(tablaSeg);
	tSegmento* seg = malloc(sizeof(tSegmento*));
	*seg = *(tSegmento*) list_get(tablaSeg, cantSegmentos-1);
	return seg;
}

int agregarPaginaAMemoria(tSegmento* seg,u_int16_t key, int time, char value[20]) {
	typedef struct{
			int timestamp;
			u_int16_t key;
			char value[20];
			//Valor hardcodeado, cuando haya handshake con lfs lo tengo que sacar de ahi el tamanio max
	}tPagina;
	int cantPags = 0;
	int cantPagsMax = miConfig->tam_mem / sizeof(tPagina);
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
	((tPagina*)(memoria + offset))->key = key;
	((tPagina*)(memoria + offset))->timestamp = time;
	strcpy(((tPagina*)(memoria + offset))->value,value);

	elem_tabla_pag* pagina = malloc(sizeof(elem_tabla_pag));
	pagina->modificado = true;
	pagina->offsetMemoria = offset;
	pagina->index = list_size(seg->tablaPaginas);

	list_add(seg->tablaPaginas, (elem_tabla_pag*)pagina);
	log_debug(logger, "Pagina cargada en memoria.");
	log_debug(logger, "El value es: %s", ((tPagina*)(memoria + offset))->value);
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

int buscarPaginaEnMemoria(int key, tSegmento* miseg,elem_tabla_pag* pagTabla) {
	typedef struct{
				int timestamp;
				u_int16_t key;
				char value[20];
				//Valor hardcodeado, cuando haya handshake con lfs lo tengo que sacar de ahi el tamanio max
	}tPagina;
	bool buscarKey(void* elemento){
		elem_tabla_pag* elemPagAux;
		elemPagAux = (elem_tabla_pag*)elemento;
		return ((tPagina*)(memoria + elemPagAux->offsetMemoria))->key == key;
	}

	if(!list_is_empty((t_list*)miseg->tablaPaginas)){
		if(list_find((t_list*)miseg->tablaPaginas, buscarKey) != NULL){
			*pagTabla = *(elem_tabla_pag*)list_find((t_list*)miseg->tablaPaginas, buscarKey);
			//*pagina = *(tPagina*)(memoria + miElem->offsetMemoria);
			return pagTabla->index;
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
	config = config_create(pathConfig);
	miConfig->puerto_kernel = config_get_string_value(config, "PUERTO_KERNEL");
	miConfig->puerto_fs = config_get_string_value(config, "PUERTO_FS");
	miConfig->ip_fs = config_get_string_value(config, "IP");
	miConfig->tam_mem = config_get_int_value(config, "TAM_MEM");
	return miConfig;
}

void finalizarEjecucion() {
	printf("------------------------\n");
	printf("¿chau chau adios?\n");
	printf("------------------------\n");
	log_destroy(logger);
	config_destroy(config);
	close(socket_lfs);
	close(socket_kernel);
	close(socket_sv);
	list_iterate(tablaSegmentos, free);
	raise(SIGTERM);
}


