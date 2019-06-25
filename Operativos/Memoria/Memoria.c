#include "Memoria.h"
#include "Querys.h"
#define BACKLOG 5			// Define cuantas conexiones vamos a mantener pendientes al mismo tiempo
#define PACKAGESIZE 1024	// Define cual va a ser el size maximo del paquete a enviar
char package[PACKAGESIZE];
struct addrinfo hints;
struct addrinfo *serverInfo;
pthread_t hiloConsola;
pthread_t hiloSocket;
t_miConfig* miConfig;


int main() {
	tamanioMaxValue = 10;
	signal(SIGINT, finalizarEjecucion);
	logger = log_create("Memoria.log", "Memoria.c", 1, LOG_LEVEL_DEBUG);
	miConfig = cargarConfig();
	socket_sv = levantarServidor((char*) miConfig->puerto_kernel);
	socket_kernel = aceptarCliente(socket_sv);
	log_debug(logger, "Levanta conexion con kernel");
	socket_lfs = levantarCliente((char*) miConfig->puerto_fs, miConfig->ip_fs);
	tamanioMaxValue = handshakeLFS(socket_lfs);
	log_debug(logger, "Handshake con LFS realizado. Tamanio max del value: %d",
			tamanioMaxValue);
	memoria = calloc(miConfig->tam_mem, 6 + tamanioMaxValue);
	tablaSegmentos = list_create();
	header = malloc(sizeof(type));
	leyoConsola = true;
	recibioSocket = true;
	*header = NIL;
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

int handshakeLFS(int socket_lfs){
	int buffer;
	recv(socket_lfs,&buffer,4,MSG_WAITALL);
	return buffer;
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


void actualizarPaginaEnMemoria(tSegmento* segmento,int index, char* newValue) {
	elem_tabla_pag* elemTablaPag =  malloc(sizeof(elem_tabla_pag));
	elemTablaPag = (elem_tabla_pag*)list_get(segmento->tablaPaginas, index);
	int offsetMemoria = elemTablaPag->offsetMemoria;
	int timestamp = (int) time(NULL);
	memcpy(memoria + offsetMemoria + 2, &(timestamp),4);
	memcpy(memoria + offsetMemoria + 6,newValue,tamanioMaxValue);
	elemTablaPag->modificado = true;
	log_debug(logger, "Pagina encontrada y actualizada.");
}

tSegmento* obtenerUltimoSegmentoDeTabla(t_list* tablaSeg) {
	int cantSegmentos = list_size(tablaSeg);
	tSegmento* seg = malloc(sizeof(tSegmento*));
	*seg = *(tSegmento*) list_get(tablaSeg, cantSegmentos-1);
	return seg;
}

int agregarPaginaAMemoria(tSegmento* seg,tPagina* pagina) {
	int cantPags = 0;
	int cantPagsMax = miConfig->tam_mem / (6 + tamanioMaxValue);
	tPagina* pag = (tPagina*)(memoria);
	while (pag->timestamp != 0) {
		cantPags++;
		if (cantPags <= cantPagsMax) {
			pag = (tPagina*)(memoria + cantPags*(6+tamanioMaxValue));
		} else {
			return -1;
			//no hay mas lugar en memoria
		}
	}
	int offset = (cantPags * (6 + tamanioMaxValue));

	memcpy((memoria + offset),&(pagina->key),sizeof(uint16_t));

	memcpy((memoria + offset + 2),&(pagina->timestamp),sizeof(int));

	memcpy((memoria + offset + 6),pagina->value,tamanioMaxValue);

	elem_tabla_pag* pagTabla = malloc(sizeof(elem_tabla_pag));
	pagTabla->modificado = true;
	pagTabla->offsetMemoria = offset;
	pagTabla->index = list_size(seg->tablaPaginas);

	list_add(seg->tablaPaginas, (elem_tabla_pag*)pagTabla);
	log_debug(logger, "Pagina cargada en memoria.");

	return 1;

}

void cargarSegmentoEnTabla(char* path, t_list* listaSeg) {
	tSegmento* seg = malloc(sizeof(tSegmento));
	seg->path = malloc(strlen(path));
	strcpy(seg->path,path);
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
		return !strcmp(nombreTabla, seg->path);
	}
	if (!list_is_empty(listaSegmentos)) {
		if (list_find(listaSegmentos, mismoNombre) != NULL) {
			*miseg = *(tSegmento*) list_find(listaSegmentos, mismoNombre);
			return 1;
		}
	}
	return -1;
}

void liberarPaginasDelSegmento(tSegmento* miSegmento, t_list* tablaSegmentos) {
	void eliminarDeMemoria(void* elemento) {
		elem_tabla_pag* elemPag = (elem_tabla_pag*) elemento;
		*(int*) (memoria + elemPag->offsetMemoria + 2) = 0; //LIMPIAR EL TIMESTAMP
	}

	bool mismoNombre(void* elemento) {
		tSegmento* miseg = (tSegmento*) elemento;
		return !strcmp(miseg->path, miSegmento->path);
	}
	t_list* tablaDePaginas = miSegmento->tablaPaginas;
	if (!list_is_empty(tablaDePaginas)) {
		list_iterate(tablaDePaginas, eliminarDeMemoria);
	}
	list_destroy(tablaDePaginas);
	list_remove_by_condition(tablaSegmentos, mismoNombre);

}


/*
char* separarNombrePath(char* path) {
	char** separado = string_split(path, "/");
	int i = 0;
	char* nombre;
	while (separado[i + 1] != NULL) {
		i++;
	}
	nombre = separado[i];
	int j = 0;
	 while(separado[j] != NULL && j != i){
	 free(separado[j]);
	 }
	 free(separado);
	return nombre;
}*/

int buscarPaginaEnMemoria(int key, tSegmento* miseg,elem_tabla_pag* pagTabla,tPagina* pagina) {
	bool buscarKey(void* elemento){
		elem_tabla_pag* elemPagAux;
		elemPagAux = (elem_tabla_pag*)elemento;
		return (*(uint16_t*)(memoria + elemPagAux->offsetMemoria)) == key;
	}

	if(!list_is_empty((t_list*)miseg->tablaPaginas)){
		if(list_find((t_list*)miseg->tablaPaginas, buscarKey) != NULL){
			*pagTabla = *(elem_tabla_pag*)list_find((t_list*)miseg->tablaPaginas, buscarKey);

			memcpy(&(pagina->key),memoria + pagTabla->offsetMemoria,2);

			memcpy(&(pagina->timestamp),memoria + pagTabla->offsetMemoria + 2,4);

			memcpy(pagina->value,memoria + pagTabla->offsetMemoria + 6,tamanioMaxValue);
			return pagTabla->index;

		} else {
			log_debug(logger,"no encontro la pagina");
			return -1;
		}

	} else {
		return -1;
	}

}

t_miConfig* cargarConfig() {
	char* buffer = malloc(256);
	char* pathConfig = malloc(256);
	log_info(logger,
			"Por favor ingrese el path de su archivo de configuracion (Memoria.config): ");
	fgets(buffer, 256, stdin);
	strncpy(pathConfig, buffer, strlen(buffer) - 1);
	t_miConfig* miConfig = malloc(sizeof(t_miConfig));
	config = config_create(pathConfig);
	miConfig->puerto_kernel = (int) config_get_string_value(config,
			"PUERTO_KERNEL");
	miConfig->puerto_fs = (int) config_get_string_value(config, "PUERTO_FS");
	miConfig->ip_fs = config_get_string_value(config, "IP");
	miConfig->tam_mem = config_get_int_value(config, "TAM_MEM");
	log_debug(logger, "Levanta archivo de config");
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
	free(header);
	free(memoria);
	free(paramsConsola);
	raise(SIGTERM);
}


