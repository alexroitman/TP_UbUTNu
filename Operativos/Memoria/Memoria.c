#include "Memoria.h"
#include "Querys.h"
#define BACKLOG 5			// Define cuantas conexiones vamos a mantener pendientes al mismo tiempo
#define PACKAGESIZE 1024	// Define cual va a ser el size maximo del paquete a enviar
int tam_pag;


int main() {
	tamanioMaxValue = 10;
	pathConfig = malloc(256);
	signal(SIGINT, finalizarEjecucion);
	logger = log_create("../Memoria.log", "Memoria.c", 1, LOG_LEVEL_DEBUG);
	miConfig = malloc(sizeof(t_miConfig));
    pedirPathConfig();
	log_debug(logger,"Tamanio Memoria: %d",miConfig->tam_mem);

	


	//if (miConfig->numeroMemoria == 1) {
		socket_lfs = levantarClienteNoBloqueante((char*) miConfig->puerto_fs, miConfig->ip_fs);
		if (socket_lfs <= 0) {
			log_error(logger, "No se pudo conectar a LFS");
			close(socket_lfs);
			raise(SIGTERM);
		}else{
			tamanioMaxValue = handshakeLFS(socket_lfs);
			log_debug(logger,
					"Handshake con LFS realizado. Tamanio max del value: %d",
					tamanioMaxValue);
			log_debug(logger,"Levanta conexion con LFS");

		}
//}

	socket_sv = levantarServidor((char*) miConfig->puerto_escucha);
	socket_gossip = levantarServidor((char*) miConfig->puerto_gossip);
	tam_pag = sizeof(unsigned long long) + sizeof(uint16_t) + tamanioMaxValue;

	memoria = calloc(miConfig->tam_mem,tam_pag);
	cantPagsMax = miConfig->tam_mem / (tam_pag);
	log_debug(logger,"Cantidad maxima de paginas en memoria: %d",cantPagsMax);
	tablaSegmentos = list_create();
	//header = malloc(sizeof(type));
	sem_init(&mutexJournal,0,1);
	sem_init(&mutexLRU,0,1);
	leyoConsola = true;
	recibioSocket = true;
	//*header = NIL;
	paramsConsola = malloc(sizeof(tHiloConsola));
	//paramsConsola->header = header;
	inicializarTablaGossip();
	pthread_create(&hiloSocket, NULL, (void*) recibirHeader, NULL);
	pthread_create(&hiloConsola, NULL, (void*) leerQuery,
			(void*) paramsConsola);
	pthread_create(&hiloJournal, NULL, (void*) journalAsincronico, NULL);
	pthread_create(&hiloInnotify, NULL, (void*) innotificar, NULL);
	pthread_create(&hiloGossip, NULL, (void*) realizarGossiping, NULL);
	pthread_join(hiloSocket, NULL);
	pthread_join(hiloConsola, NULL);
	pthread_join(hiloJournal, NULL);
	pthread_join(hiloGossip, NULL);
}

void* recibirHeader(void* arg) {
	int listener = socket_sv;
	FD_ZERO(&active_fd_set);
	FD_SET(socket_sv, &active_fd_set);
	FD_SET(socket_gossip, &active_fd_set);

	while (1) {

		//int flagError = 0;

	//	while (flagError != 1) {
			read_fd_set = active_fd_set;
			recibioSocket = false;
			if (select(FD_SETSIZE, &read_fd_set, NULL, NULL, NULL) < 0) {
				log_error(logger, "error de socket");
			} else {
				for (int i = 0; i < FD_SETSIZE; ++i) {
					if (FD_ISSET(i, &read_fd_set)) {
						//El cliente quiere algo!!, vamos a ver quien es primero y que quiere

						if (i == listener || i == socket_gossip) {
							/*
							 El socket i resultó ser el listener! o sea el socket del servidor. Cuando el que quiere algo es el propio servidor
							 significa que tenemos un nuevo cliente QUE SE QUIERE CONECTAR. Por lo que tenemos que hacer un accept() para generar un nuevo socket para ese lciente
							 y guardar dicho nuevo cliente en nuestra lista general de sockets
							 */
							int clienteNuevo = aceptarCliente(i);
							log_debug(logger, "cliente nuevo: %d",
									clienteNuevo);
							FD_SET(clienteNuevo, &active_fd_set);
						} else {

							//type* header = (type*) arg;
							type head;
							if (recv(i, &(head), sizeof(type), MSG_WAITALL)
									> 0) {
								log_debug(logger, "llego algo del cliente %d", i);
								recibioSocket = true;
								usleep(miConfig->retardoMemoria * 1000);
								sem_wait(&mutexJournal);
								ejecutarConsulta(i,head);
								sem_post(&mutexJournal);
							} else {
								FD_CLR(i,&active_fd_set);
								log_debug(logger, "Se ha cortado la conexion con el cliente %d",i);
							}

							//Si no es el listener, es un cliente, por lo que acá tenemso que hacer un recv(i) para ver que es lo que quiere el cliente
						}
					}
				}
			}
	//	}
		//log_debug(logger, "Se ha cortado la conexion con un cliente");
	}

}


void inicializarTablaGossip() {
	tablaGossip = list_create();
	tMemoria* yo = malloc(sizeof(tMemoria));
	strcpy(yo->ip, miConfig->mi_IP);
	strcpy(yo->puerto,(char*) miConfig->puerto_escucha);
	yo->numeroMemoria = miConfig->numeroMemoria;
	list_add(tablaGossip, yo);
}

void realizarGossiping() {
	int cant = 0;
	while (miConfig->puerto_seeds[cant] != NULL) {
		cant++;
	}
	log_debug(logger, "cant seeds: %d", cant);

	//if ((miConfig->puerto_seeds)[0] != NULL && (miConfig->ip_seeds)[0] != NULL) {
	if (cant > 0) {
		int seeds[cant];
		for(int j =0;j<cant;j++){
			seeds[j] = -1;
		}
		while (1) {
			int error = 0;
			usleep(miConfig->retardoGossiping * 1000);
			for (int i = 0; i < cant; i++) {
				if (seeds[i] <= 0) {
					seeds[i] = levantarClienteNoBloqueante(
							(miConfig->puerto_seeds)[i],
							(miConfig->ip_seeds)[i]);
				}
				if (seeds[i] > 0) {
					int cantMemorias = tablaGossip->elements_count;
					tGossip* packGossip = malloc(sizeof(tGossip));
					packGossip->memorias = malloc(
							cantMemorias * sizeof(tMemoria));
					cargarPackGossip(packGossip, tablaGossip, GOSSIPING);
					char* serializado = serializarGossip(packGossip);
					int tam_tot = cantMemorias * sizeof(tMemoria) + sizeof(type)
							+ sizeof(int);
					error = enviarPaquete(seeds[i], serializado, tam_tot);
					type head;
					if (recv(seeds[i], &head, sizeof(type),
					MSG_WAITALL) > 0 && error >= 0) {
						//log_debug(logger, "llego algo del cliente %d", clienteGossip);
						recibioSocket = true;
						sem_wait(&mutexJournal);
						ejecutarConsulta(seeds[i],head);
						sem_post(&mutexJournal);
					}else{
						seeds[i] = -1;
						log_debug(logger,"Error al enviar tabla a seed %d",i);
					}
					free(packGossip->memorias);
					free(packGossip);
					free(serializado);
				}else{
					log_debug(logger,"No me pude conectar con mi seed %d",i);
				}
			}
		}

	}else{
		log_debug(logger,"No tengo seeds");
	}
/*
	if ((miConfig->puerto_seeds)[0] != NULL && (miConfig->ip_seeds)[0] != NULL) {

		while (1) {
			int clienteGossip;
			do {
				usleep(miConfig->retardoGossiping * 1000);
				//log_debug(logger, "pruebo conectarme");
				clienteGossip = levantarClienteNoBloqueante(
						(miConfig->puerto_seeds)[0], (miConfig->ip_seeds)[0]);
				if (clienteGossip == -1) {
					log_debug(logger, "No me pude conectar con mi seed");
				}
			} while (clienteGossip <= 0);
			log_debug(logger, "me pude conectar a mi seed");
			//FD_SET(clienteGossip,&active_fd_set);
			int error = 0;
			while (error != -1) {
				usleep(miConfig->retardoGossiping * 1000);
				int cantMemorias = tablaGossip->elements_count;
				tGossip* packGossip = malloc(sizeof(tGossip));
				packGossip->memorias = malloc(cantMemorias * sizeof(tMemoria));
				cargarPackGossip(packGossip, tablaGossip, GOSSIPING);
				char* serializado = serializarGossip(packGossip);
				int tam_tot = cantMemorias * sizeof(tMemoria) + sizeof(type)
						+ sizeof(int);
				error = enviarPaquete(clienteGossip, serializado, tam_tot);
				type head;
				if (recv(clienteGossip, &(head), sizeof(type), MSG_WAITALL)
						> 0) {
					//log_debug(logger, "llego algo del cliente %d", clienteGossip);
					recibioSocket = true;
					usleep(miConfig->retardoMemoria * 1000);
					sem_wait(&mutexJournal);
					ejecutarConsulta(clienteGossip,head);
					sem_post(&mutexJournal);
				} else {
					error = -1;
				}

				free(packGossip);
				free(packGossip->memorias);
				free(serializado);
			}
		}
	}*/

}

void cargarPackGossip(tGossip* packGossip, t_list* tablaGossip, type header){

	packGossip->cant_memorias = tablaGossip->elements_count;
	//log_debug(logger,"cant memorias a mandar %d",packGossip->cant_memorias);
	packGossip->header = header;
	for(int i = 0; i < tablaGossip->elements_count; i++){
		tMemoria* miMemoria;
		miMemoria = (tMemoria*) list_get(tablaGossip, i);
		packGossip->memorias[i] = *miMemoria;
		//log_debug(logger,"mando memoria %d",packGossip->memorias[i].numeroMemoria);
	}
}



void actualizarTablaGossip(tGossip* packGossip){
	void actualizarTabla(void* elemento){
		tMemoria* mem = (tMemoria*) elemento;
		bool compararNumeroMem(void* elem){
			tMemoria* mem2 = (tMemoria*) elem;
			return mem2->numeroMemoria == mem->numeroMemoria;
		}

		if(list_filter(tablaGossip,compararNumeroMem)->elements_count == 0){
			list_add(tablaGossip,mem);
		}
	}


	t_list* listaRecv = list_create();
	for(int i = 0; i < packGossip->cant_memorias; i++){
		tMemoria* mem = malloc(sizeof(tMemoria));
		*mem = packGossip->memorias[i];
		list_add(listaRecv,mem);
	}
	list_iterate(listaRecv,actualizarTabla);
    list_destroy(listaRecv);
}

void devolverTablaGossip(tGossip* packGossip, int socket) {

	cargarPackGossip(packGossip, tablaGossip, RESPGOSS);
	char* serializado = serializarGossip(packGossip);
	int tam_tot = packGossip->cant_memorias * sizeof(tMemoria) + sizeof(type)
			+ sizeof(int);
	enviarPaquete(socket, serializado, tam_tot);
	free(serializado);
}



int actualizarPaginaEnMemoria(tSegmento* segmento, int index, char* newValue) {

	if (verificarTamanioValue(newValue)) {
		log_debug(logger,"Voy a actualizar la pagina");
		elem_tabla_pag* elemTablaPag;
		elemTablaPag = (elem_tabla_pag*) list_get(segmento->tablaPaginas,
				index);
		int offsetMemoria = elemTablaPag->offsetMemoria;
		//int timestamp = (int) time(NULL);
		unsigned long long timestamp = obtenerTimestamp();
		memcpy(memoria + offsetMemoria + 2, &timestamp, 8);
		//memcpy(memoria + offsetMemoria + 2, &(timestamp), 4);
		memcpy(memoria + offsetMemoria + 10, newValue, tamanioMaxValue);
		elemTablaPag->modificado = true;
		elemTablaPag->ultimoTime = timestamp;
		log_debug(logger, "Pagina encontrada y actualizada.");
		return 1;
	} else {
		log_error(logger, "Tamanio de value demasiado grande");
		return -2;
	}
}

tSegmento* obtenerUltimoSegmentoDeTabla(t_list* tablaSeg) {
	int cantSegmentos = list_size(tablaSeg);
	tSegmento* seg = malloc(sizeof(tSegmento*));
	*seg = *(tSegmento*) list_get(tablaSeg, cantSegmentos-1);
	return seg;
}

bool verificarTamanioValue(char* value){
	return (strlen(value) < tamanioMaxValue);
}

int agregarPaginaAMemoria(tSegmento* seg, tPagina* pagina, bool modificado) {
	if (verificarTamanioValue(pagina->value)) {

		int cantPags = 0;
		unsigned long long timeAux;
		memcpy(&timeAux, memoria + 2, sizeof(unsigned long long));
		while (timeAux != 0) {
			cantPags++;
			if (cantPags < cantPagsMax) {
				memcpy(&timeAux, memoria + cantPags * (tam_pag) + 2,
						sizeof(unsigned long long));
			} else {

				return -1;
				//no hay mas lugar en memoria
			}
		}
		int offset = (cantPags * tam_pag);
		memcpy((memoria + offset), &(pagina->key), sizeof(uint16_t));

		memcpy((memoria + offset + 2), &(pagina->timestamp), sizeof(unsigned long long));

		memcpy((memoria + offset + 10), pagina->value, tamanioMaxValue);
		elem_tabla_pag* pagTabla = malloc(sizeof(elem_tabla_pag));
		pagTabla->modificado = modificado;
		pagTabla->offsetMemoria = offset;
		pagTabla->index = list_size(seg->tablaPaginas);
		log_debug(logger,"La pagina insertada tiene index %d",pagTabla->index);
		pagTabla->ultimoTime = obtenerTimestamp();
		list_add(seg->tablaPaginas, (elem_tabla_pag*) pagTabla);
		//actualizarIndexLista(seg->tablaPaginas);
		log_debug(logger, "Pagina cargada en memoria.");
		log_warning(logger,"Flag modificado de la key %d es %d",pagina->key,pagTabla->modificado);
		return 1;
	} else {
		log_error(logger, "Tamanio de value demasiado grande");
		return -2;
	}
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
	tSegmento* seg = malloc(sizeof(tSegmento));
	*seg = *(tSegmento*) list_get(tablaSeg, index);
	return seg;
}

int buscarSegmentoEnTabla(char* nombreTabla, tSegmento* miseg, t_list* listaSegmentos) {
	bool mismoNombre(void* elemento) {
		//tSegmento* seg = malloc(sizeof(tSegmento));
		tSegmento* seg = (tSegmento*) elemento;
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
	memset(memoria + elemPag->offsetMemoria + 2, 0 , sizeof(unsigned long long));
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
		//return (*(uint16_t*)(memoria + elemPagAux->offsetMemoria)) == key;
		uint16_t miKey;
		memcpy(&miKey,memoria + elemPagAux->offsetMemoria,2);
		return miKey == key;
	}

	if(!list_is_empty((t_list*)miseg->tablaPaginas)){
		if(list_find((t_list*)miseg->tablaPaginas, buscarKey) != NULL){
			*pagTabla = *(elem_tabla_pag*)list_find((t_list*)miseg->tablaPaginas, buscarKey);

			memcpy(&(pagina->key), memoria + pagTabla->offsetMemoria, 2);

			memcpy(&(pagina->timestamp), memoria + pagTabla->offsetMemoria + 2,
					sizeof(unsigned long long));

			memcpy(pagina->value, memoria + pagTabla->offsetMemoria + 10,
					tamanioMaxValue);

			return pagTabla->index;

		} else {
			log_debug(logger,"no encontro la pagina");
			return -1;
		}

	} else {
		return -1;
	}

}

bool filtrarFlagModificado (void* pag){
	elem_tabla_pag* pagina = (elem_tabla_pag*) pag;
	return !(pagina->modificado);
}

bool fueModificado(void* pag){
	elem_tabla_pag* pagina = (elem_tabla_pag*) pag;
	return pagina->modificado;
}


int ejecutarLRU(){
	log_warning(logger,"Ejecutando LRU");
	t_list* LRUPaginaPorSegmento;
	LRUPaginaPorSegmento = list_create();

	bool pagLRU(void* pag1,void* pag2){
		elem_tabla_pag* pagina1 = (elem_tabla_pag*) pag1;
		elem_tabla_pag* pagina2 = (elem_tabla_pag*) pag2;
		//log_debug(logger,"Comparo %d con %d",pagina1->offsetMemoria,pagina2->offsetMemoria);
		return pagina1->ultimoTime < pagina2->ultimoTime;
	}
	void logearIndex(void* elem){
		elem_tabla_pag* pag = (elem_tabla_pag*) elem;
		log_debug(logger,"index: %d tiene time %llu y flag %d",pag->index,pag->ultimoTime,pag->modificado);
	}


	void paginaMenorTimePorSeg(void* seg){
		tSegmento* miSeg = (tSegmento*) seg;
		log_debug(logger,"el segmento %s tiene %d paginas",miSeg->path,miSeg->tablaPaginas->elements_count);
		//log_debug(logger, "LOGEO TODAS LAS PAGINAS: ");
		//list_iterate(miSeg->tablaPaginas,logearIndex);
		t_list* tablaPagsOrdenada = list_sorted(miSeg->tablaPaginas,pagLRU);
		tablaPagsOrdenada = list_filter(tablaPagsOrdenada,filtrarFlagModificado);
		//log_debug(logger,"size de %s: %d",miSeg->path,list_size(tablaPagsOrdenada));
		if(!list_is_empty(tablaPagsOrdenada)){
			//log_debug(logger, "LOGEO LAS FILTRADAS Y ORDENADAS: ");
			//list_iterate(tablaPagsOrdenada,logearIndex);
			elem_tabla_pag* pag = malloc(sizeof(elem_tabla_pag));
			*pag =*(elem_tabla_pag*)list_get(tablaPagsOrdenada,0);
			list_add(LRUPaginaPorSegmento,pag);
		}else{
			elem_tabla_pag* pagNull = malloc(sizeof(elem_tabla_pag));
			pagNull->index = -1;
			list_add(LRUPaginaPorSegmento,pagNull);
		}

	}

	log_debug(logger,"cantidad de segmentos: %d",tablaSegmentos->elements_count);
	list_iterate(tablaSegmentos,paginaMenorTimePorSeg);
	//log_debug(logger,"Logeo LRUPagPorSegm");
	list_iterate(LRUPaginaPorSegmento,logearIndex);
	elem_tabla_pag* pagBorrar = malloc(sizeof(elem_tabla_pag));
	int indexMin = listMinTimestamp(LRUPaginaPorSegmento, pagBorrar);
	if(indexMin == -1){
		return -1;
	}
	//log_debug(logger,"size de lista LRU: %d",LRUPaginaPorSegmento->elements_count);
	//int key = *((int*) memoria + pagBorrar->offsetMemoria);
	//log_debug(logger,"Se eliminara la pagina: %d",pagBorrar->index);
	uint16_t key;
	memcpy(&key,memoria + pagBorrar->offsetMemoria,2);
	tSegmento* segmento = list_get(tablaSegmentos,indexMin);
	//actualizarIndexLista(segmento->tablaPaginas);
	log_warning(logger,"Voy a borrar la key %d de la tabla %s",key,segmento->path);
	log_debug(logger,"Voy a borrar el elemento %d de la tabla de paginas de %s",pagBorrar->index,segmento->path);
	list_remove(segmento->tablaPaginas,pagBorrar->index);
	actualizarIndexLista(segmento->tablaPaginas);
	eliminarDeMemoria(pagBorrar);
	list_iterate(LRUPaginaPorSegmento,free);
	return 1;
}

int listMinTimestamp(t_list* listaPaginas,elem_tabla_pag* pagina){
	//int size = list_size(listaPaginas);
	int size = listaPaginas->elements_count;
	//elem_tabla_pag* pagAux = list_get(listaPaginas, 0);
	elem_tabla_pag* pagAux = malloc(sizeof(elem_tabla_pag));
	int indexMin;
	//*pagAux = *(elem_tabla_pag*) list_get(listaPaginas,0);
	bool filtrarModificados(void* elem){
		elem_tabla_pag* pag = (elem_tabla_pag*) elem;
		return pag->index != -1;
	}
	unsigned long long min;
	if(!list_any_satisfy(listaPaginas,filtrarModificados)){
		return -1;
	}else{
		*pagAux = *((elem_tabla_pag*)list_find(listaPaginas,filtrarModificados));
		*pagina = *pagAux;
		min = pagAux->ultimoTime;
	}


	int i;

	for (i = 0; i < size; i++) {
		*pagAux = *(elem_tabla_pag*)list_get(listaPaginas,i);
		if((pagAux->ultimoTime <= min) && filtrarModificados(pagAux)){
			min = pagAux->ultimoTime;
			*pagina = *pagAux;
			indexMin = i;
		}

	}
	free(pagAux);
	return indexMin;

}



void ejecutarJournal(){
	log_debug(logger,"Realizando Journal");

	int index = 0;
	tSegmento* miSegmento = list_get(tablaSegmentos, 0);

	while(miSegmento != NULL){
		t_list* paginasModificadas = list_create();
		paginasModificadas = list_filter(miSegmento->tablaPaginas, fueModificado);
		mandarInsertDePaginasModificadas(paginasModificadas,miSegmento->path, socket_lfs);
		borrarPaginasModificadas(paginasModificadas, miSegmento->tablaPaginas);
		index++;
		miSegmento = list_get(tablaSegmentos, index);
	}

	log_debug(logger, "Journal finalizado");
}

void mandarInsertDePaginasModificadas(t_list* paginasModificadas,char* nombreTabla, int socket_lfs){

	int index = 0;
	tInsert* miInsert = malloc(sizeof(tInsert));
	miInsert->type = INSERT;
	miInsert->nombre_tabla = malloc(strlen(nombreTabla) + 1);
	miInsert->nombre_tabla_long = strlen(nombreTabla) + 1;
	miInsert->value = malloc(tamanioMaxValue);
	strcpy(miInsert->nombre_tabla, nombreTabla);

	elem_tabla_pag* miPagina = (elem_tabla_pag*)list_get(paginasModificadas, index);
	while(miPagina != NULL){
		memcpy(&(miInsert->key), (memoria + miPagina->offsetMemoria), 2);
		memcpy(&(miInsert->timestamp), (memoria + miPagina->offsetMemoria) + 2, sizeof(unsigned long long));
		memcpy(miInsert->value, (memoria + miPagina->offsetMemoria + 10), tamanioMaxValue);


		miInsert->value_long = strlen(miInsert->value) + 1;
		miInsert->length = sizeof(miInsert->type) + sizeof(miInsert->nombre_tabla_long)
						+ miInsert->nombre_tabla_long + sizeof(miInsert->key)
						+ sizeof(miInsert->value_long) + miInsert->value_long
						+ sizeof(miInsert->timestamp);
		char* serializado = serializarInsert(miInsert);
		enviarPaquete(socket_lfs, serializado, miInsert->length);
		index++;
		miPagina = (elem_tabla_pag*)list_get(paginasModificadas, index);
	}
	free(miInsert->value);
	free(miInsert->nombre_tabla);
	free(miInsert);
}

void borrarPaginasModificadas(t_list* paginasModificadas, t_list* tablaPaginasMiSegmento){
	int index = 0;
	elem_tabla_pag* pagABorrar = (elem_tabla_pag*) list_get(paginasModificadas, 0);
	while(pagABorrar != NULL){
		list_remove(tablaPaginasMiSegmento, pagABorrar->index); //BORRO DE LA TABLA DE PAGINAS
		actualizarIndexLista(tablaPaginasMiSegmento);
		eliminarDeMemoria(pagABorrar);							//BORRO DE LA MEMORIA
		index++;
		pagABorrar = (elem_tabla_pag*) list_get(paginasModificadas, index);
	}
}

void actualizarIndexLista(t_list* lista){
	int index = 0;

	void cambiarIndice(void* elemento){
		elem_tabla_pag* pagina = (elem_tabla_pag*) elemento;
		pagina->index = index;
		index++;
	}

	list_iterate(lista, cambiarIndice);
}

void pedirPathConfig(){
	int error = 1;
	while (error == 1) {
		char* buffer = malloc(256);

		log_info(logger,
				"Por favor ingrese el path de su archivo de configuracion");
		fgets(buffer, 256, stdin);
		strncpy(pathConfig, buffer, strlen(buffer) - 1);
		config = config_create(pathConfig);
		if (config != NULL) {
			error = 0;
			miConfig = cargarConfig();

		}else{
			log_info(logger, "Error de apertura de config ");
			error = 1;
		}

		free(buffer);
		//free(pathConfig);
	}

}

t_miConfig* cargarConfig() {

	miConfig->puerto_escucha = malloc(strlen(config_get_string_value(config, "PUERTO"))+1);
	strcpy(miConfig->puerto_escucha, config_get_string_value(config, "PUERTO"));
	//miConfig->puerto_escucha = (int) config_get_string_value(config,"PUERTO");
	miConfig->puerto_fs = malloc(strlen(config_get_string_value(config, "PUERTO_FS"))+1);
	strcpy(miConfig->puerto_fs, config_get_string_value(config, "PUERTO_FS"));
	//miConfig->puerto_fs = (int) config_get_string_value(config, "PUERTO_FS");
	miConfig->ip_fs = malloc(strlen(config_get_string_value(config, "IP"))+1);
	strcpy(miConfig->ip_fs, config_get_string_value(config, "IP"));
	//miConfig->ip_fs = config_get_string_value(config, "IP");
	miConfig->tam_mem = config_get_int_value(config, "TAM_MEM");
	miConfig->retardoJournal = config_get_int_value(config,"RETARDO_JOURNAL");
	miConfig->retardoMemoria = config_get_int_value(config,"RETARDO_MEM");
	miConfig->retardoFS = config_get_int_value(config,"RETARDO_FS");
	miConfig->ip_seeds = config_get_array_value(config,"IP_SEEDS");
	miConfig->puerto_seeds = config_get_array_value(config,"PUERTO_SEEDS");
	miConfig->retardoGossiping = config_get_int_value(config,"RETARDO_GOSSIPING");
	miConfig->numeroMemoria = config_get_int_value(config,"MEMORY_NUMBER");
	//miConfig->mi_IP = config_get_string_value(config, "MI_IP");
	miConfig->mi_IP = malloc(strlen(config_get_string_value(config, "MI_IP"))+1);
	strcpy(miConfig->mi_IP, config_get_string_value(config, "MI_IP"));
	//miConfig->puerto_gossip = (int) config_get_string_value(config, "PUERTO_GOSSIP");
	miConfig->puerto_gossip = malloc(strlen(config_get_string_value(config, "PUERTO_GOSSIP"))+1);
	strcpy(miConfig->puerto_gossip, config_get_string_value(config, "PUERTO_GOSSIP"));
	config_destroy(config);
	log_debug(logger, "Levanta archivo de config");
	return miConfig;
}

void validarAgregadoDePagina(bool leyoConsola, int* error,int socket, tSegmento* miSegmento,
		tPagina* pagina, bool modificado) {

	int errorLRU;
	if (*error == -1) {
		sem_wait(&mutexLRU);
		errorLRU = ejecutarLRU();
		sem_post(&mutexLRU);
		if (errorLRU == -1) {
			log_error(logger,"Memoria FULL");
			if (leyoConsola) {
				ejecutarJournal();
				agregarPaginaAMemoria(miSegmento, pagina,modificado);
			} else {
				//le envio el error a kernel para que me devuelva un JOURNAL y la consulta denuevo
				send(socket, error, sizeof(int), 0);
			}
		} else {
			agregarPaginaAMemoria(miSegmento, pagina,modificado);
			*error = 1;
			send(socket, error, sizeof(int), 0);
		}
	}else{
		if(!leyoConsola){
			//le aviso a kernel que la consulta termino OK
			send(socket,error,sizeof(int),0);
		}
	}
}

void liberarPagina(void* elemento){
	elem_tabla_pag* pag = (elem_tabla_pag*) elemento;
	free(pag);
}


void liberarPaginas(void* elemento){
	tSegmento* seg = (tSegmento*)elemento;
	t_list* tablaPaginas = seg->tablaPaginas;
	list_iterate(tablaPaginas, liberarPagina);
	free(seg->tablaPaginas);
	free(seg->path);
	free(seg);
}


void finalizarEjecucion() {
	printf("------------------------\n");
	printf("¿chau chau adios?\n");
	printf("------------------------\n");
	free(miConfig);
	close(socket_lfs);
	close(socket_kernel);
	close(socket_sv);
	list_iterate(tablaSegmentos, liberarPaginas);
	list_destroy_and_destroy_elements(tablaGossip, free);
	free(tablaSegmentos);
	free(tablaGossip);
//	free(header);
	free(memoria);
	free(paramsConsola);
	sem_destroy(&mutexJournal);
	raise(SIGTERM);
}


