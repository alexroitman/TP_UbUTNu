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

	/*
	socket_lfs = levantarCliente((char*) miConfig->puerto_fs, miConfig->ip_fs);
	socket_sv = levantarServidor((char*) miConfig->puerto_kernel);
	*/
	socket_gossip = levantarServidor((char*) miConfig->miPuerto);

	/*
	log_debug(logger, "Levanta conexion con kernel");
	tamanioMaxValue = handshakeLFS(socket_lfs);
	log_debug(logger,"socket lfs: %d",socket_lfs);
	log_debug(logger,"socket sv: %d",socket_sv);
	log_debug(logger, "Handshake con LFS realizado. Tamanio max del value: %d",
			tamanioMaxValue);
	*/
	memoria = calloc(miConfig->tam_mem,6 + tamanioMaxValue);
	cantPagsMax = miConfig->tam_mem / (6 + tamanioMaxValue);
	log_debug(logger,"Cantidad maxima de paginas en memoria: %d",cantPagsMax);
	tablaSegmentos = list_create();
	header = malloc(sizeof(type));
	sem_init(&mutexJournal,0,1);
	leyoConsola = true;
	recibioSocket = true;
	*header = NIL;
	paramsConsola = malloc(sizeof(tHiloConsola));
	paramsConsola->header = header;
	inicializarTablaGossip();
	pthread_create(&hiloSocket, NULL, (void*) recibirHeader, (void*) header);
	pthread_create(&hiloConsola, NULL, (void*) leerQuery,
			(void*) paramsConsola);
	pthread_create(&hiloJournal, NULL, (void*) journalAsincronico, NULL);
	pthread_create(&hiloGossip, NULL, (void*) realizarGossiping, NULL);
	pthread_join(hiloSocket, NULL);
	pthread_join(hiloConsola, NULL);
	pthread_join(hiloJournal, NULL);
	pthread_join(hiloGossip, NULL);
}

void* recibirHeader(void* arg) {
	while (1) {
		int listener = socket_sv;
		FD_ZERO(&active_fd_set);
		FD_SET(socket_sv, &active_fd_set);
		FD_SET(socket_gossip, &active_fd_set);
		int flagError = 0;

		while (flagError != 1) {
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

							type* header = (type*) arg;
							if (recv(i, &(*header), sizeof(type), MSG_WAITALL)
									> 0) {
								log_debug(logger, "llego algo del cliente %d", i);
								recibioSocket = true;
								usleep(miConfig->retardoMemoria * 1000);
								sem_wait(&mutexJournal);
								ejecutarConsulta(i);
								sem_post(&mutexJournal);
							} else {
								flagError = 1;
							}

							//Si no es el listener, es un cliente, por lo que acá tenemso que hacer un recv(i) para ver que es lo que quiere el cliente
						}
					}
				}
			}
		}
		log_debug(logger, "Se ha cortado la conexion");
	}

}


void inicializarTablaGossip(){
	tablaGossip = list_create();
	tMemoria* yo = malloc(sizeof(tMemoria));
	strcpy(yo->ip,miConfig->mi_IP);
	strcpy(yo->puerto,miConfig->miPuerto);
	yo->numeroMemoria = miConfig->numeroMemoria;
	list_add(tablaGossip,yo);
}

void realizarGossiping() {
	if ((miConfig->puerto_seeds) != NULL && (miConfig->ip_seeds)[0] != NULL) {

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
				if (recv(clienteGossip, &(*header), sizeof(type), MSG_WAITALL) > 0) {
					//log_debug(logger, "llego algo del cliente %d", clienteGossip);
					recibioSocket = true;
					usleep(miConfig->retardoMemoria * 1000);
					sem_wait(&mutexJournal);
					ejecutarConsulta(clienteGossip);
					sem_post(&mutexJournal);
				}else{
					error = -1;
				}

				free(packGossip);
				free(packGossip->memorias);
				free(serializado);
			}
		}
	}

}

void cargarPackGossip(tGossip* packGossip, t_list* tablaGossip, type header){
	packGossip->cant_memorias = tablaGossip->elements_count;
	packGossip->header = header;
	for(int i = 0; i < tablaGossip->elements_count; i++){
		tMemoria* miMemoria;
		miMemoria = (tMemoria*) list_get(tablaGossip, i);
		packGossip->memorias[i] = *miMemoria;
	}
}

char* serializarGossip(tGossip* packGossip) {
	char* serializedPackage = malloc(sizeof(type) +
			sizeof(packGossip->cant_memorias)
					+ packGossip->cant_memorias * sizeof(tMemoria));
	int offset = 0;
	int size_to_send;

	size_to_send = sizeof(type);
	memcpy(serializedPackage + offset, &(packGossip->header),size_to_send);
	offset+= size_to_send;

	size_to_send = sizeof(int);
	memcpy(serializedPackage + offset,&(packGossip->cant_memorias), size_to_send);
	offset+= size_to_send;

	for(int x =0;x<packGossip->cant_memorias;x++){
		size_to_send = sizeof(tMemoria);
		memcpy(serializedPackage + offset, &(packGossip->memorias[x]),size_to_send);
		offset += size_to_send;
	}

	return serializedPackage;
}

int desSerializarGossip(tGossip* packGossip, int socket){


	int status = recv(socket,&(packGossip->cant_memorias),sizeof(int),0);
	if(!status){
		log_debug(logger,"no llego nada wacho");
		return 0;
	}
	//log_debug(logger,"cant memorias: %d",packGossip->cant_memorias);


	packGossip->memorias = malloc(packGossip->cant_memorias * sizeof(tMemoria));
	for(int x = 0; x< packGossip->cant_memorias; x++){
		status = recv(socket,&(packGossip->memorias[x]),sizeof(tMemoria),0);
	}
	return status;
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
}



void actualizarPaginaEnMemoria(tSegmento* segmento,int index, char* newValue) {
	elem_tabla_pag* elemTablaPag =  malloc(sizeof(elem_tabla_pag));
	elemTablaPag = (elem_tabla_pag*)list_get(segmento->tablaPaginas, index);
	int offsetMemoria = elemTablaPag->offsetMemoria;
	int timestamp = (int) time(NULL);
	memcpy(memoria + offsetMemoria + 2, &(timestamp),4);
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

bool filtrarFlagModificado (void* pag){
	elem_tabla_pag* pagina = (elem_tabla_pag*) pag;
	return !(pagina->modificado);
}

bool fueModificado(void* pag){
	elem_tabla_pag* pagina = (elem_tabla_pag*) pag;
	return pagina->modificado;
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



	void paginaMenorTimePorSeg(void* seg){
		tSegmento* miSeg = (tSegmento*) seg;
		t_list* tablaPagsOrdenada = list_sorted(miSeg->tablaPaginas,pagLRU);
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
	log_debug(logger,"Se eliminara la pagina: %d",pagBorrar->offsetMemoria);
	tSegmento* segmento = obtenerSegmentoDeTabla(tablaSegmentos,indexMin);
	list_remove(segmento->tablaPaginas,pagBorrar->index);
	actualizarIndexLista(segmento->tablaPaginas);
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



void ejecutarJournal(){
	sem_wait(&mutexJournal);
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
	sem_post(&mutexJournal);
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
		memcpy(miInsert->value, (memoria + miPagina->offsetMemoria + 6), tamanioMaxValue);
		memcpy(&(miInsert->key), (memoria + miPagina->offsetMemoria), 2);

		miInsert->value_long = strlen(miInsert->value) + 1;
		miInsert->length = sizeof(miInsert->type) + sizeof(miInsert->nombre_tabla_long)
						+ miInsert->nombre_tabla_long + sizeof(miInsert->key)
						+ sizeof(miInsert->value_long) + miInsert->value_long;
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
	miConfig->retardoJournal = config_get_int_value(config,"RETARDO_JOURNAL");
	miConfig->retardoMemoria = config_get_int_value(config,"RETARDO_MEM");
	miConfig->ip_seeds = config_get_array_value(config,"IP_SEEDS");
	miConfig->puerto_seeds = config_get_array_value(config,"PUERTO_SEEDS");
	miConfig->retardoGossiping = config_get_int_value(config,"RETARDO_GOSSIPING");
	miConfig->numeroMemoria = config_get_int_value(config,"MEMORY_NUMBER");
	miConfig->miPuerto = config_get_string_value(config,"MI_PUERTO");
	miConfig->mi_IP = config_get_string_value(config, "MI_IP");
	log_debug(logger,"puerto: %s", miConfig->miPuerto);
	log_debug(logger,"ip: %s", miConfig->mi_IP);
	log_debug(logger, "Levanta archivo de config");
	free(buffer);
	free(pathConfig);
	return miConfig;
}

void chequearMemoriaFull(bool leyoConsola, int error,int socket, tSegmento* miSegmento,
		tPagina* pagina) {
	int errorLRU;
	if (error == -1) {
		errorLRU = ejecutarLRU();
		if (errorLRU == -1) {
			if (leyoConsola) {
				ejecutarJournal();
				agregarPaginaAMemoria(miSegmento, pagina);
			} else {
				//le envio el error a kernel para que me devuelva un JOURNAL y la consulta denuevo
				send(socket, &error, sizeof(int), 0);
			}
		} else {
			agregarPaginaAMemoria(miSegmento, pagina);
		}
	}else{
		if(!leyoConsola){
			//le aviso a kernel que la consulta termino OK
			send(socket,&error,sizeof(int),0);
		}
	}
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


