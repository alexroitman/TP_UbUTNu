#include "Memoria.h"
#include "Querys.h"
#define BACKLOG 5			// Define cuantas conexiones vamos a mantener pendientes al mismo tiempo
#define PACKAGESIZE 1024	// Define cual va a ser el size maximo del paquete a enviar




int main() {



	tamanioMaxValue = 10;
	signal(SIGINT, finalizarEjecucion);
	logger = log_create("Memoria.log", "Memoria.c", 1, LOG_LEVEL_DEBUG);
	miConfig = cargarConfig();
	log_debug(logger,"Tamanio Memoria: %d",miConfig->tam_mem);
	socket_sv = levantarServidor((char*) miConfig->puerto_kernel);
	socket_kernel = aceptarCliente(socket_sv);
	log_debug(logger, "Levanta conexion con kernel");
	socket_lfs = levantarCliente((char*) miConfig->puerto_fs, miConfig->ip_fs);
	tamanioMaxValue = handshakeLFS(socket_lfs);
	log_debug(logger, "Handshake con LFS realizado. Tamanio max del value: %d",
			tamanioMaxValue);
	memoria = calloc(miConfig->tam_mem,6 + tamanioMaxValue);
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
		sleep(2);
		recv(socket_kernel, &(*header), sizeof(type), MSG_WAITALL);
		recibioSocket = true;
		if (*header != NIL) {
			//EN REALIDAD LO QUE ESTOY PREGUNTANDO ES SI RECIBIO ALGO DE KERNEL
			//PORQUE SI SE CORTO LA CONEXION CON KERNEL NO QUIERO QUE HAGA EL SIGNAL DEL SEMAFORO
			ejecutarConsulta(memoria);
		}
	}
}


void actualizarPaginaEnMemoria(tSegmento* segmento,int index, char* newValue) {
	elem_tabla_pag* elemTablaPag =  malloc(sizeof(elem_tabla_pag));
	elemTablaPag = (elem_tabla_pag*)list_get(segmento->tablaPaginas, index);
	int offsetMemoria = elemTablaPag->offsetMemoria;
	int timestamp = (int) time(NULL);
	memcpy(memoria + offsetMemoria + 2, &(timestamp),4);
	log_debug(logger,"offset: %d",offsetMemoria);
	memcpy(memoria + offsetMemoria + 6,newValue,tamanioMaxValue);
	elemTablaPag->modificado = true; //PARA PROBARRRRR ! TIENE QUE SER TRUE
	elemTablaPag->ultimoTime = (int) time (NULL);
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
	log_debug(logger,"cant paginas max: %d",cantPagsMax);
	//tPagina* pag = (tPagina*)(memoria);
	int timeAux;
	memcpy(&timeAux,memoria + 2,4);
	while (timeAux != 0) {
		cantPags++;
		if (cantPags < cantPagsMax) {
			memcpy(&timeAux,memoria + cantPags*(6 + tamanioMaxValue) + 2,4);
		} else {
			return -1;
			//no hay mas lugar en memoria
		}
	}
	int offset = (cantPags * (6 + tamanioMaxValue));
	log_debug(logger,"offset: %d",offset);
	memcpy((memoria + offset),&(pagina->key),sizeof(uint16_t));

	memcpy((memoria + offset + 2),&(pagina->timestamp),sizeof(int));

	memcpy((memoria + offset + 6),pagina->value,tamanioMaxValue);

	elem_tabla_pag* pagTabla = malloc(sizeof(elem_tabla_pag));
	pagTabla->modificado = true;//PARA PROBARRRRRRRRRRRR!!! TIENE QUE SERTRUE
	pagTabla->offsetMemoria = offset;
	pagTabla->index = list_size(seg->tablaPaginas);
	pagTabla->ultimoTime = (int) time (NULL);
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

void eliminarDeMemoria(void* elemento) {
	elem_tabla_pag* elemPag = (elem_tabla_pag*) elemento;
	memset(memoria + elemPag->offsetMemoria + 2, 0 , 4);
}

void liberarPaginasDelSegmento(tSegmento* miSegmento, t_list* tablaSegmentos) {

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

int ejecutarLRU(){
	t_list* LRUPaginaPorSegmento;
	LRUPaginaPorSegmento = list_create();

	bool pagLRU(void* pag1,void* pag2){
		elem_tabla_pag* pagina1 = (elem_tabla_pag*) pag1;
		elem_tabla_pag* pagina2 = (elem_tabla_pag*) pag2;
		log_debug(logger,"Comparo %d con %d",pagina1->offsetMemoria,pagina2->offsetMemoria);
		return pagina1->ultimoTime < pagina2->ultimoTime;
	}

	bool filtrarFlagModificado (void* pag){
		elem_tabla_pag* pagina = (elem_tabla_pag*) pag;
		return !(pagina->modificado);
	}

	void paginaMenorTimePorSeg(void* seg){
		tSegmento* miSeg = (tSegmento*) seg;
		t_list* tablaPags = miSeg->tablaPaginas;
		t_list* tablaPagsOrdenada = list_sorted(tablaPags,pagLRU);
		tablaPagsOrdenada = list_filter(tablaPagsOrdenada,filtrarFlagModificado);
		log_debug(logger,"size de %s: %d",miSeg->path,list_size(tablaPagsOrdenada));
		if(!list_is_empty(tablaPagsOrdenada)){
			elem_tabla_pag* pag = list_get(tablaPagsOrdenada,0);
			list_add(LRUPaginaPorSegmento,pag);
		}

	}

	list_iterate(tablaSegmentos,paginaMenorTimePorSeg);
	if(list_is_empty(LRUPaginaPorSegmento)){
		return -1;
	}

	elem_tabla_pag* pagBorrar = malloc(sizeof(elem_tabla_pag));
	int indexMin = listMinTimestamp(LRUPaginaPorSegmento, pagBorrar);
	//LRUPaginaPorSegmento = list_sorted(LRUPaginaPorSegmento,pagLRU);
	//void* pagABorrar = list_get(LRUPaginaPorSegmento,0);
	log_debug(logger,"voy a borrar la pagina: %d",pagBorrar->offsetMemoria);
	tSegmento* segmento = obtenerSegmentoDeTabla(tablaSegmentos,indexMin);
	list_remove(segmento->tablaPaginas,pagBorrar->index);
	eliminarDeMemoria(pagBorrar);
	return 1;
}

int listMinTimestamp(t_list* listaPaginas,elem_tabla_pag* pagina){
	int size = list_size(listaPaginas);
	elem_tabla_pag* pagAux = list_get(listaPaginas, 0);
	*pagina = *pagAux;
	int min = pagAux->ultimoTime;
	int i;
	int indexMin=0;
	for (i = 1; i < size; i++) {
		pagAux = list_get(listaPaginas,i);
		if(pagAux->ultimoTime < min){
			min = pagAux->ultimoTime;
			*pagina = *pagAux;
			indexMin = i;
		}
	}
	return indexMin;
}

bool estaModificada(void* elemento){
	elem_tabla_pag* miPagina = (elem_tabla_pag*) elemento;
	return miPagina->modificado;
}

int ejecutarJournal(){
	int index = 0;
	int errorMandarInserts = 0;
	int errorBorrasPaginas = 0;
	tSegmento* miSegmento = list_get(tablaSegmentos, 0);

	while(miSegmento != NULL){
		t_list* tablaPaginasMiSegmento = miSegmento->tablaPaginas;
		t_list* paginasModificadas = list_create();
		paginasModificadas = list_filter(tablaPaginasMiSegmento, estaModificada);
		errorMandarInserts = mandarInsertDePaginasModificadas(paginasModificadas, socket_lfs);
		errorBorrasPaginas = borrarPaginasModificadas(paginasModificadas, tablaPaginasMiSegmento);
		miSegmento = list_get(tablaSegmentos, index++);
	}
	return errorBorrasPaginas || errorMandarInserts;
}

int mandarInsertDePaginasModificadas(t_list* paginasModificadas, int socket_lfs){
	int error = 0;
	int index = 0;
	while(paginasModificadas != NULL){
		//como verga le mando el insert a lfs, ahora veo
	}
	return error;
}

int borrarPaginasModificadas(t_list* paginasModificadas, t_list* tablaPaginasMiSegmento){
	int error = 0;
	int index = 0;
	while(paginasModificadas != NULL){
		elem_tabla_pag* pagABorrar = (elem_tabla_pag*) list_get(paginasModificadas, index);
		list_remove(tablaPaginasMiSegmento, pagABorrar->index); //BORRO DE LA TABLA DE PAGINAS
		eliminarDeMemoria(pagABorrar);							//BORRO DE LA MEMORIA
		index++;
	}
	return error;
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


