/*
 * Kernel.c
 *
 *  Created on: 11 abr. 2019
 *      Author: utnso
 */

#include "Kernel.h"

#define BACKLOG 5			// Define cuantas conexiones vamos a mantener pendientes al mismo tiempo
#define PACKAGESIZE 1024	// Define cual va a ser el size maximo del paquete a enviar

char package[PACKAGESIZE];
struct addrinfo hints;
struct addrinfo *serverInfo;
sem_t semReady;
sem_t mutexReady;
sem_t mutexNew;
sem_t semNew;
sem_t mutexMetrics;
sem_t mutexSC;
sem_t mutexSHC;
sem_t mutexEC;
sem_t mutexContMult;
sem_t mutexMems;
sem_t mutexCaido;
sem_t mutexQuantum;
t_contMetrics metricsSC;
t_contMetrics metricsSHC;
t_contMetrics metricsEC;
pthread_t planificador_t;
pthread_t hiloGossip;
pthread_t hiloMetrics;
pthread_t hiloInnotify;
pthread_t hiloDescribe;
t_log *logger;
t_log *loggerError;
t_log *loggerWarning;
t_queue *colaReady;
t_queue *colaNew;
tMemoria *SC;
t_list *listaMemsEC;
t_list *listaMemsSHC;
t_list *memorias;
t_list *listaTablas;
t_list *listaDeLSockets;
t_list *memoriasCaidas;
t_list *memLoads;
configKernel *miConfig;
int contScript;
bool continuar = true;
int contMult = 0;
int cantMemorias = 1;

// Define cual va a ser el size maximo del paquete a enviar


int main(){
	signal(SIGINT, finalizarEjecucion);
	miConfig = malloc(sizeof(configKernel));
	inicializarTodo();
	SC = malloc(sizeof(tMemoria));
	SC->numeroMemoria = -1;
	t_infoMem* socket_memoria = malloc(sizeof(t_infoMem));
	socket_memoria->socket = levantarCliente(miConfig->puerto_mem, miConfig->ip_mem);
	tMemoria *mem = malloc(sizeof(tMemoria));
	strcpy(mem->ip,miConfig->ip_mem);
	strcpy(mem->puerto,miConfig->puerto_mem);
	int size_to_send = sizeof(type);
	char* serializedPackage = malloc(size_to_send);
	type signal = SIGNAL;
	memcpy(serializedPackage,&(signal),size_to_send);
	enviarPaquete(socket_memoria->socket,serializedPackage,size_to_send);
	free(serializedPackage);
	recv(socket_memoria->socket,&mem->numeroMemoria,sizeof(int),0);
	socket_memoria->id = mem->numeroMemoria;
	list_add(memorias,mem);
	tMemLoad* memload = malloc(sizeof(tMemLoad));
	memload->mem = mem;
	memload->cont = 0;
	list_add(memLoads,memload);
	log_debug(logger,"Este es el numero de mi memoria principal: %d",mem->numeroMemoria);
	log_debug(logger, "Realizando describe general");
	pthread_t cpus[miConfig->MULT_PROC];
	t_list *sockets = list_create();
	listaDeLSockets = list_create();
	memoriasCaidas = list_create();
	list_add(sockets,socket_memoria);
	list_add_in_index(listaDeLSockets,contMult,sockets);
	despacharQuery("describe\n",sockets);
	if(levantarCpus(cpus)){
		colaReady = queue_create();
		colaNew = queue_create();
		pthread_create(&planificador_t, NULL, (void*) planificador, NULL);
		pthread_create(&hiloGossip,NULL,(void*) gossip,NULL);
		pthread_create(&hiloMetrics,NULL,(void*)metrics,NULL);
		pthread_create(&hiloInnotify, NULL, (void*) innotificar, NULL);
		pthread_create(&hiloDescribe,NULL,(void*) describe,NULL);
		char consulta[256] = "";
		for(int i = 0; i < miConfig->MULT_PROC; i++){
			pthread_detach(cpus[i]);
		}
		pthread_detach(planificador_t);
		pthread_detach(hiloGossip);
		pthread_detach(hiloInnotify);
		pthread_detach(hiloDescribe);
		while (continuar) {
			fgets(consulta, 256, stdin);
			sem_wait(&mutexMems);
			if(memorias->elements_count > sockets->elements_count){
				for(int i = 0; i < memorias->elements_count;i++){
					tMemoria* mem = (tMemoria*) list_get(memorias,i);
					bool compararNumeroMem(void* elem){
						t_infoMem* mem2 = (t_infoMem*) elem;
						return mem2->id == mem->numeroMemoria;
					}
					if(list_filter(sockets,compararNumeroMem)->elements_count == 0){
						t_infoMem *sock = malloc(sizeof(t_infoMem));
						sock->socket = levantarClienteNoBloqueante(mem->puerto,mem->ip);
						if(sock->socket != -1){
							sock->id = mem->numeroMemoria;
							log_debug(logger,"agrege esta memoria: %d",sock->id);
							list_add(sockets,sock);
							bool compararNumeroMem2(void* elem){
								tMemoria* mem2 = (tMemoria*) elem;
								return mem2->numeroMemoria == mem->numeroMemoria;
							}
							sem_wait(&mutexCaido);
							if(list_filter(memoriasCaidas,compararNumeroMem2)->elements_count != 0){
								list_remove_by_condition(memoriasCaidas,compararNumeroMem2);
							}
							sem_post(&mutexCaido);
						}
					}
				}
			}
			sem_post(&mutexMems);
			sem_wait(&mutexCaido);
			if(memoriasCaidas->elements_count != 0){
				int size_to_send = sizeof(type);
				char* serializedPackage = malloc(size_to_send);
				type gossip = SIGNAL;
				memcpy(serializedPackage,&(gossip),size_to_send);
				for(int i = 0; i < memoriasCaidas->elements_count;i++){
					tMemoria* mem = (tMemoria*) list_get(memoriasCaidas,i);
					bool compararNumeroMem(void* elem){
						t_infoMem* mem2 = (t_infoMem*) elem;
						return mem2->id == mem->numeroMemoria;
					}
					if(list_filter(sockets,compararNumeroMem)->elements_count != 0){
						t_infoMem* memoria = (t_infoMem*)list_find(sockets,compararNumeroMem);
						int info = enviarPaquete(memoria->socket,serializedPackage,size_to_send);
						if(info == -1){
							list_remove_by_condition(sockets,compararNumeroMem);
						}else{
							list_remove(memoriasCaidas,i);
							recv(memoria->socket,&info,sizeof(int),0);
						}
					}
				}
				free(serializedPackage);
			}
			sem_post(&mutexCaido);
			log_warning(loggerWarning,"esta es la cantidad de sockets que tengo: %d",sockets->elements_count);
			int consultaOk = despacharQuery(consulta, sockets);
			if(consultaOk < 0){
				int socketAborrar = consultaOk*-1;
				log_error(loggerError,"Memoria %d caida, eliminando de la lista", socketAborrar);
				t_infoMem* mem = malloc(sizeof(mem));
				mem->id = socketAborrar;
				bool compararNumeroMem(void* elem){
					tMemoria* mem2 = (tMemoria*) elem;
					return mem2->numeroMemoria == mem->id;
				}
				if(list_filter(memoriasCaidas,compararNumeroMem)->elements_count == 0){
					list_add(memoriasCaidas,list_find(memorias,compararNumeroMem));
					log_warning(loggerWarning,"Realizando Jounal general para mantener consistencias");
					despacharQuery("journal\n",sockets);
				}
				bool compararNumeroSocket(void* elem){
					t_infoMem* mem2 = (t_infoMem*) elem;
					return mem2->id == mem->id;
				}
				list_remove_by_condition(sockets,compararNumeroSocket);
			}
			if (!consultaOk) {
				log_error(loggerError,"Se ingreso un header no valido");
			}
			consultaOk = 0;
		}

	}else{
		log_error(loggerError,"No se han podido levantar las cpus...");
	}
	/*sem_destroy(&semReady);
	sem_destroy(&semNew);
	sem_destroy(&mutexNew);
	sem_destroy(&mutexReady);
	sem_destroy(&mutexSocket);*/
}

bool hayCaidoSock(void* recibo){
	t_infoMem* mem = (t_infoMem*) recibo;
	bool estaCaida = true;
	for(int i = 0;i<memoriasCaidas->elements_count;i++){
		tMemoria* mem2 = list_get(memoriasCaidas,i);
		if(mem->id == mem2->numeroMemoria){
			estaCaida = false;
		}
	}
	return estaCaida;
}

bool hayCaida(void* recibo){
	tMemoria* mem = (tMemoria*) recibo;
	bool estaCaida = true;
	for(int i = 0;i<memoriasCaidas->elements_count;i++){
		tMemoria* mem2 = list_get(memoriasCaidas,i);
		if(mem->numeroMemoria == mem2->numeroMemoria){
			estaCaida = false;
		}
	}
	return estaCaida;
}


void describe (){
	t_list* sockets = list_create();
	t_infoMem* mem = malloc(sizeof(t_infoMem));
	tMemoria* unaMem = (tMemoria*)list_get(memorias,0);
	mem->id = unaMem->numeroMemoria;
	mem->socket = levantarClienteNoBloqueante(unaMem->puerto,unaMem->ip);
	list_add_in_index(sockets,0,mem);
	while(1){

		usleep(miConfig->metadata_refresh*1000);

		sem_wait(&mutexMems);
		if(memorias->elements_count > sockets->elements_count){
			for(int i = 0; i < memorias->elements_count;i++){
				tMemoria* mem = (tMemoria*) list_get(memorias,i);
				bool compararNumeroMem(void* elem){
					t_infoMem* mem2 = (t_infoMem*) elem;
					return mem2->id == mem->numeroMemoria;
				}
				if(list_filter(sockets,compararNumeroMem)->elements_count == 0){
					t_infoMem *sock = malloc(sizeof(t_infoMem));
					sock->socket = levantarClienteNoBloqueante(mem->puerto,mem->ip);
					if(sock->socket != -1){
						sock->id = mem->numeroMemoria;
						log_debug(logger,"agrege esta memoria: %d",sock->id);
						list_add(sockets,sock);
						bool compararNumeroMem2(void* elem){
							tMemoria* mem2 = (tMemoria*) elem;
							return mem2->numeroMemoria == mem->numeroMemoria;
						}
						sem_wait(&mutexCaido);
						if(list_filter(memoriasCaidas,compararNumeroMem2)->elements_count != 0){
							list_remove_by_condition(memoriasCaidas,compararNumeroMem2);
						}
						sem_post(&mutexCaido);
					}
				}
			}
		}
		sem_post(&mutexMems);

		sem_wait(&mutexCaido);
		if(memoriasCaidas->elements_count != 0){
			int size_to_send = sizeof(type);
			char* serializedPackage = malloc(size_to_send);
			type gossip = SIGNAL;
			memcpy(serializedPackage,&(gossip),size_to_send);
			for(int i = 0; i < memoriasCaidas->elements_count;i++){
				tMemoria* mem = (tMemoria*) list_get(memoriasCaidas,i);
				bool compararNumeroMem(void* elem){
					t_infoMem* mem2 = (t_infoMem*) elem;
					return mem2->id == mem->numeroMemoria;
				}
				if(list_filter(sockets,compararNumeroMem)->elements_count != 0){
					t_infoMem* memoria = (t_infoMem*)list_find(sockets,compararNumeroMem);
					int info = enviarPaquete(memoria->socket,serializedPackage,size_to_send);
					if(info == -1){
						list_remove_by_condition(sockets,compararNumeroMem);
					}else{
						list_remove(memoriasCaidas,i);
						recv(memoria->socket,&info,sizeof(int),0);
					}
				}
			}
			free(serializedPackage);
		}
		sem_post(&mutexCaido);
		int info = despacharQuery("DESCRIBE\n",sockets);
		if(info<=0){
			bool compararNumeroMem(void* elem){
				tMemoria* mem2 = (tMemoria*) elem;
				return mem2->numeroMemoria == mem->id;
			}
			bool compararNumeroSocket(void* elem){
				t_infoMem* mem2 = (t_infoMem*) elem;
				return mem2->id == mem->id;
			}
			sem_wait(&mutexCaido);
			if(list_filter(memoriasCaidas,compararNumeroMem)->elements_count == 0){
				log_error(loggerError,"Memoria %d caida", mem->id);
				tMemoria* memoriaCaida = malloc(sizeof(tMemoria));
				*memoriaCaida = *(tMemoria*) list_find(memorias,compararNumeroMem);
				list_add(memoriasCaidas,memoriaCaida);
				log_warning(loggerWarning,"Realizando Jounal general para mantener consistencias");
				t_infoMem* sk = (t_infoMem*)list_find(sockets,compararNumeroSocket);
				close(sk->socket);
				list_remove_by_condition(sockets,compararNumeroSocket);
				sem_post(&mutexCaido);
				despacharQuery("journal\n",sockets);
			}else{
				t_infoMem* sk = (t_infoMem*)list_find(sockets,compararNumeroSocket);
				close(sk->socket);
				list_remove_by_condition(sockets,compararNumeroSocket);
				sem_post(&mutexCaido);
			}
			for(int i = 0; i < sockets->elements_count;i++){
				/*if(list_filter(memoriasCaidas,compararNumeroMem)->elements_count == 0){
					list_add(memoriasCaidas,list_find(memorias,compararNumeroMem));
				}
				unaMem = list_get(list_filter(memorias,hayCaida),0);
				mem->id = unaMem->numeroMemoria;
				mem->socket = levantarClienteNoBloqueante(unaMem->puerto,unaMem->ip);
				list_replace(sockets,0,mem);*/
				t_infoMem* unSocket = (t_infoMem*) list_get(sockets,i);
				int size_to_send = sizeof(type);
				char* serializedPackage = malloc(size_to_send);
				type gossip = SIGNAL;
				memcpy(serializedPackage,&(gossip),size_to_send);
				int info = enviarPaquete(unSocket->socket,serializedPackage,size_to_send);
				if(info == -1){
					sem_wait(&mutexCaido);
					if(list_filter(memoriasCaidas,compararNumeroMem)->elements_count == 0){
						log_error(loggerError,"Memoria %d caida", mem->id);
						tMemoria* memoriaCaida = malloc(sizeof(tMemoria));
						*memoriaCaida = *(tMemoria*) list_find(memorias,compararNumeroMem);
						list_add(memoriasCaidas,memoriaCaida);
						log_warning(loggerWarning,"Realizando Jounal general para mantener consistencias");
						t_infoMem* sk = (t_infoMem*)list_find(sockets,compararNumeroSocket);
						close(sk->socket);
						list_remove_by_condition(sockets,compararNumeroSocket);
						sem_post(&mutexCaido);
						despacharQuery("journal\n",sockets);
					}else{
						t_infoMem* sk = (t_infoMem*)list_find(sockets,compararNumeroSocket);
						close(sk->socket);
						list_remove_by_condition(sockets,compararNumeroSocket);
						sem_post(&mutexCaido);
					}
					mem->socket = unSocket->socket;
					mem->id = unSocket->id;
				}
				free(serializedPackage);
			}

		}
	}
}

type validarSegunHeader(char* header) {
	string_to_upper(header);
	if (!strcmp(header, "SELECT")) {
		return SELECT;
	}
	if (!strcmp(header, "INSERT")) {
		return INSERT;
	}
	if (!strcmp(header, "RUN")) {
			return RUN;
	}
	if (!strcmp(header, "CREATE")) {
				return CREATE;
		}
	if (!strcmp(header, "ADD")) {
					return ADD;
			}

	if (!strcmp(header, "DESCRIBE") || !strcmp(header, "DESCRIBE\n") ) {
					return DESCRIBE;
			}
	if (!strcmp(header, "DESCRIBE\n")) {
					return DESCRIBE;
			}
	if (!strcmp(header, "JOURNAL\n")){
					return JOURNAL;
			}
	if (!strcmp(header, "DROP")){
						return DROP;
				}
	if (!strcmp(header, "METRICS\n")){
						return METRICS;
				}
	return NIL;
}

void metricsInsert(consistencias cons,unsigned long long time){
	switch(cons){
	case sc:
		metricsSC.acumtInsert += time;
		metricsSC.contInsert++;
		break;
	case ec:
		metricsEC.acumtInsert += time;
		metricsEC.contInsert++;
		break;
	case shc:
		metricsSHC.acumtInsert += time;
		metricsSHC.contInsert++;
		break;
	}
}
void metricsSelect(consistencias cons,unsigned long long time){
	switch(cons){
	case sc:
		metricsSC.acumtSelect += time;
		metricsSC.contSelect++;
		break;
	case ec:
		metricsEC.acumtSelect += time;
		metricsEC.contSelect++;
		break;
	case shc:
		metricsSHC.acumtSelect += time;
		metricsSHC.contSelect++;
		break;
	}
}

void metrics(){
	while(1){
		sem_wait(&mutexMetrics);
		metricsEC.acumtInsert = 0;
		metricsEC.acumtSelect = 0;
		metricsEC.contInsert = 0;
		metricsEC.contSelect = 0;
		metricsSC.acumtInsert = 0;
		metricsSC.acumtSelect = 0;
		metricsSC.contInsert = 0;
		metricsSC.contSelect = 0;
		metricsSHC.acumtInsert = 0;
		metricsSHC.acumtSelect = 0;
		metricsSHC.contInsert = 0;
		metricsSHC.contSelect = 0;
		for(int i = 0;i < memLoads->elements_count;i++){
			tMemLoad* load = (tMemLoad*) list_get(memLoads,i);
			load->cont = 0;
			list_replace(memLoads,i,load);
		}
		sem_post(&mutexMetrics);
		sleep(30);
		sem_wait(&mutexMetrics);
		ejecutarMetrics();
		sem_post(&mutexMetrics);
	}

}
void ejecutarMetrics(){
	log_warning(loggerWarning,"CRITERIO SC: ");
	unsigned long long latencyRead = 0;
	unsigned long long latencyWrite = 0;
	if(metricsSC.contSelect != 0)
		latencyRead = metricsSC.acumtSelect / metricsSC.contSelect;
	if(metricsSC.contInsert != 0)
		latencyWrite = metricsSC.acumtInsert / metricsSC.contInsert;
	log_debug(logger, "READ LATENCY: %llu ms, WRITE LATENCY: %llu ms, READS: %d, WRITES: %d",
			latencyRead,latencyWrite,metricsSC.contSelect,metricsSC.contInsert);
	log_warning(loggerWarning,"CRITERIO EC: ");
	if(metricsEC.contSelect != 0)
		latencyRead = metricsEC.acumtSelect / metricsEC.contSelect;
	else{ latencyRead = 0;}
	if(metricsEC.contInsert != 0)
		latencyWrite = metricsEC.acumtInsert / metricsEC.contInsert;
	else{latencyWrite = 0;}
	log_debug(logger, "READ LATENCY: %llu ms, WRITE LATENCY: %llu ms, READS: %d, WRITES: %d",
			latencyRead,latencyWrite,metricsEC.contSelect,metricsEC.contInsert);
	log_warning(loggerWarning,"CRITERIO SHC: ");
	if(metricsSHC.contSelect != 0)
		latencyRead = metricsSHC.acumtSelect / metricsSHC.contSelect;
	else{latencyRead = 0;}
	if(metricsSHC.contInsert != 0)
		latencyWrite = metricsSHC.acumtInsert / metricsSHC.contInsert;
	else{latencyWrite = 0;}
	log_debug(logger, "READ LATENCY: %llu ms, WRITE LATENCY: %llu ms, READS: %d, WRITES: %d",
			latencyRead,latencyWrite,metricsSHC.contSelect,metricsSHC.contInsert);
	int total = metricsEC.contInsert + metricsEC.contSelect
			+ metricsSC.contInsert + metricsSC.contSelect+
			metricsSHC.contInsert + metricsSHC.contSelect;
	for(int i = 0; i<memLoads->elements_count;i++){
		tMemLoad* unLoad = (tMemLoad*)list_get(memLoads,i);
		log_warning(loggerWarning,"Memory load de la mem: %d",unLoad->mem->numeroMemoria);
		log_debug(logger,"%d de un total de: %d",unLoad->cont,total);
	}
}


bool existe(char* nombre){
	bool mismoNombre(void* param){
		t_metadata* metadata = (t_metadata*) param;
		return !strcmp(metadata->nombre_tabla,nombre);
	}
	return list_any_satisfy(listaTablas,mismoNombre);
}

int despacharQuery(char* consulta, t_list* sockets) {
	t_infoMem* socket_memoria = malloc(sizeof(t_infoMem));
	char** tempSplit;
	tSelect* paqueteSelect=malloc(sizeof(tSelect));
	tInsert* paqueteInsert=malloc(sizeof(tInsert));
	tCreate* paqueteCreate = malloc(sizeof(tCreate));
	tJournal* paqueteJournal = malloc(sizeof(tJournal));
	tDrop* paqueteDrop = malloc(sizeof(tDrop));
	tDescribe* paqueteDescribe = malloc(sizeof(tDescribe));
	type typeHeader;
	char* serializado = string_new();
	int consultaOk = 0;
	tempSplit = string_n_split(consulta, 2, " ");
	log_debug(logger, "header %s", tempSplit[0]);
	script *unScript;
	if (strcmp(tempSplit[0], "")) {
		typeHeader = validarSegunHeader(tempSplit[0]);
		switch (typeHeader) {
		case SELECT:
			if(validarSelect(consulta)){
				int error;
				unsigned long long comienzo = obtenerTimestamp();
				log_debug(logger,"Se recibio un SELECT");
				cargarPaqueteSelect(paqueteSelect, consulta);
				serializado = serializarSelect(paqueteSelect);
				consistencias cons = consTabla(paqueteSelect->nombre_tabla);
				*socket_memoria = *devolverSocket(cons,sockets,paqueteSelect->key);
				log_debug(logger,"uso esta memoria: %d",socket_memoria->id);
				if(socket_memoria->id != -1){
					consultaOk = enviarPaquete(socket_memoria->socket, serializado, paqueteSelect->length);
					if(consultaOk > 0){
						consultaOk = recv(socket_memoria->socket, &error, sizeof(error), 0);
						if(consultaOk > 0){
							if (error == -1) {
								log_error(loggerError, "Memoria llena, hago JOURNAL");
								cargarPaqueteJournal(paqueteJournal, "JOURNAL");
								serializado = serializarJournal(paqueteJournal);
								enviarPaquete(socket_memoria->socket, serializado,
										paqueteJournal->length);
								serializado = serializarSelect(paqueteSelect);
								enviarPaquete(socket_memoria->socket, serializado,
										paqueteSelect->length);
								recv(socket_memoria->socket, &error, sizeof(error), 0);
								consultaOk = 1;
							}
							type header = leerHeader(socket_memoria->socket);
							tRegistroRespuesta* reg = malloc(sizeof(tRegistroRespuesta));
							desSerializarRegistro(reg, socket_memoria->socket);
							if (reg->timestamp > 0) {
								log_debug(logger, "Value: %s", reg->value);
							}else{
								log_error(loggerError,"No existe la tabla o registro");
							}
							free(reg->value);
							free(reg);
							consultaOk = 1;
							bool sonIguales(void* recibo){
								tMemLoad* unLoad = (tMemLoad*) recibo;
								return unLoad->mem->numeroMemoria == socket_memoria->id;
							}
							sem_wait(&mutexMetrics);
							metricsSelect(cons,
									(obtenerTimestamp() - comienzo));
							tMemLoad* mem = list_find(memLoads,sonIguales);
							mem->cont++;
							sem_post(&mutexMetrics);
						}else{
							consultaOk = socket_memoria->id * -1;
						}
					}else{
						consultaOk = socket_memoria->id * -1;
					}
				}else{
					if(cons != nada){
						free(socket_memoria);
						log_error(loggerError,"No existen memorias disponibles para el criterio de la tabla");
					}else{
						free(socket_memoria);
						log_error(loggerError,"No existe informacion de la tabla, por favor realice un describe");
					}
				}
				free(paqueteSelect->nombre_tabla);
				free(serializado);
			}else{
				log_error(loggerError,"Se ingreso una consulta no valida");
				consultaOk = 2;
			}
			free(socket_memoria);
			break;
		case INSERT:
			if(validarInsert(consulta)){
				int error = 0;
				unsigned long long comienzo = obtenerTimestamp();
				log_debug(logger,"Se recibio un INSERT");
				char* sinFin = string_new();
				string_append(&sinFin, string_substring_until(consulta,strlen(consulta)-1 ));
				cargarPaqueteInsert(paqueteInsert, sinFin);

				serializado = serializarInsert(paqueteInsert);

				consistencias cons = consTabla(paqueteInsert->nombre_tabla);
				*socket_memoria = *devolverSocket(cons,sockets,paqueteInsert->key);
				if(socket_memoria->id != -1){
					log_debug(logger,"uso esta memoria: %d", socket_memoria->id);
					consultaOk = enviarPaquete(socket_memoria->socket, serializado, paqueteInsert->length);
					if(consultaOk > 0){
						consultaOk = recv(socket_memoria->socket, &error, sizeof(int), 0);
						if(consultaOk > 0){
							if(error == 1){
								consultaOk = 1;
								log_debug(logger, "Se inserto el valor: %s", paqueteInsert->value);
							} else if(error == -1){
								log_error(logger, "Memoria llena, hago JOURNAL");
								cargarPaqueteJournal(paqueteJournal, "JOURNAL");
								serializado = serializarJournal(paqueteJournal);
								enviarPaquete(socket_memoria->socket, serializado,
										paqueteJournal->length);
								serializado = serializarInsert(paqueteInsert);
								enviarPaquete(socket_memoria->socket, serializado, paqueteInsert->length);
								recv(socket_memoria->socket, &error, sizeof(error), 0);
								consultaOk = 1;
							}else{
								if(error == -2){
									log_debug(logger,"Tamanio de value demasiado grande");
									consultaOk = 0;
								}
							}
							bool sonIguales(void* recibo){
								tMemLoad* unLoad = (tMemLoad*) recibo;
								return unLoad->mem->numeroMemoria == socket_memoria->id;
							}
							sem_wait(&mutexMetrics);
							metricsInsert(cons,
									(obtenerTimestamp() - comienzo));
							tMemLoad* mem = list_find(memLoads,sonIguales);
							mem->cont++;
							sem_post(&mutexMetrics);
						}else{
							consultaOk = socket_memoria->id * -1;
						}
					}else{
						consultaOk = socket_memoria->id * -1;
					}
				}else{
					if(cons != nada){
						log_error(loggerError,"No existen memorias disponibles para el criterio de la tabla");
					}else{
						log_error(loggerError,"No existe informacion de la tabla, por favor realice un describe");
					}
				}
				free(serializado);
				free(paqueteInsert->nombre_tabla);
				free(paqueteInsert->value);
				free(sinFin);
			}else{
				log_error(loggerError,"Se ingreso una consulta no valida");
				consultaOk = 2;
			}
			free(socket_memoria);
			break;
		case CREATE:
			if(validarCreate(consulta)){
				log_debug(logger,"Se recibio un CREATE");
				char* sinFin = string_substring_until(consulta,string_length(consulta)-1 );
				despacharQuery("describe\n",sockets);
				cargarPaqueteCreate(paqueteCreate,sinFin);
				serializado = serializarCreate(paqueteCreate);
				if(!existe(paqueteCreate->nombre_tabla)){
					*socket_memoria = *devolverSocket(obtCons(paqueteCreate->consistencia)
							,sockets,1);
					log_debug(logger,"voy a usar esta memoria: %d",socket_memoria->id);
					if(socket_memoria->id != -1){
						consultaOk = enviarPaquete(socket_memoria->socket, serializado, paqueteCreate->length);
						if(consultaOk <= 0){
							consultaOk = socket_memoria->id * -1;
						}else{
							consultaOk = 1;
							t_metadata* meta = malloc(sizeof(t_metadata));
							strcpy(meta->nombre_tabla,paqueteCreate->nombre_tabla);
							meta->consistencia = obtCons(paqueteCreate->consistencia);
							meta->particiones = paqueteCreate->particiones;
							meta->tiempo_compactacion = paqueteCreate->compaction_time;
							list_add(listaTablas,meta);
							log_debug(logger,"Agrego la tabla %s a mi lista",meta->nombre_tabla);
						}
					}else{
						log_error(loggerError,"No existen memorias disponibles para el criterio de la tabla");
					}
					free(socket_memoria);
				}else{
					log_error(loggerError,"Se quiso hacer un create de una tabla ya existente");
				}
				free(serializado);
				free(paqueteCreate->consistencia);
				free(paqueteCreate->nombre_tabla);
				free(sinFin);
			}else{
				log_error(loggerError,"Se ingreso una consulta no valida");
				consultaOk = 2;
			}
			break;
		case RUN:
			log_debug(logger,"Se recibio un RUN con la siguiente path: %s", tempSplit[1]);
			char* sinFin = string_substring_until(tempSplit[1],string_length(tempSplit[1])-1 );
			unScript = malloc(sizeof(script));
			unScript->path = malloc(strlen(sinFin)+1);
			strcpy(unScript->path, sinFin);
			unScript->pos = 0;
			unScript->estado = new;
			unScript->id = contScript;
			contScript++;
			free(sinFin);
			sem_wait(&mutexNew);
			queue_push(colaNew, unScript);
			log_debug(logger,"Se cargo a la cola de new el script id: %d", unScript->id);
			sem_post(&semNew);
			sem_post(&mutexNew);
			consultaOk = 1;
			break;
		case ADD:
			if(validarAdd(consulta)){
				log_debug(logger,"Se recibio un ADD");
				ejecutarAdd(consulta);
				//log_debug(logger,"Se agrego el criterio a: %d ", ((t_infoMem*) list_get(listaMems, 0))->id);
				consultaOk = 1;
			}
			break;
		case DESCRIBE:
			cargarPaqueteDescribe(paqueteDescribe,
					string_substring_until(consulta,string_length(consulta)-1  ) );
			serializado = serializarDescribe(paqueteDescribe);
			if(list_filter(sockets,hayCaidoSock)->elements_count != 0){
				t_list* memsDisp = list_filter(sockets,hayCaidoSock);
				*socket_memoria = *(t_infoMem *)list_get(memsDisp,0);
				consultaOk = enviarPaquete(socket_memoria->socket,serializado,paqueteDescribe->length);
				if(consultaOk > 0){
					t_describe* response = malloc(sizeof(t_describe));
					int flag = desserializarDescribe_Response(response,socket_memoria->socket);
					if(flag > 0){
						if(response->cant_tablas != 0){
							log_debug(logger,"Cant tablas: %d", response->cant_tablas);
							ejecutarDescribe(response);
						}else{
							log_warning(loggerWarning, "No se recibio ninguna tabla");
						}
						free(response->tablas);
						free(response);
						consultaOk = 1;
					}else{
						log_error(loggerError,"Se callo la memoria");
						consultaOk = socket_memoria->id * -1;
					}
				}else{
					consultaOk = socket_memoria->id * -1;
				}
				free(socket_memoria);
			}else{
				consultaOk = -1;
			}
			free(serializado);
			free(paqueteDescribe->nombre_tabla);
			break;
		case JOURNAL:
			log_debug(logger,"Se recibio un JOURNAL");
			cargarPaqueteJournal(paqueteJournal,
					string_substring_until(consulta,string_length(consulta)-1  ) );
			serializado = serializarJournal(paqueteJournal);
			for(int i = 0; i<sockets->elements_count;i++){
				*socket_memoria = *(t_infoMem*)list_get(sockets,i);
				log_warning(loggerWarning,"voy a journalear este socket: %d",socket_memoria->id);
				int flag = enviarPaquete(socket_memoria->socket, serializado, paqueteJournal->length);
				if(flag <= 0){
					bool compararNumeroMem(void* elem){
						tMemoria* mem2 = (tMemoria*) elem;
						return mem2->numeroMemoria == socket_memoria->id;
					}
					bool compararNumeroSocket(void* elem){
						t_infoMem* mem2 = (t_infoMem*) elem;
						return mem2->id == socket_memoria->id;
					}
					sem_wait(&mutexCaido);
					if(list_filter(memoriasCaidas,compararNumeroMem)->elements_count == 0){
						log_error(loggerError,"Memoria %d caida", socket_memoria->id);
						tMemoria* memoriaCaida = malloc(sizeof(tMemoria));
						*memoriaCaida = *(tMemoria*) list_find(memorias,compararNumeroMem);
						list_add(memoriasCaidas,memoriaCaida);
						log_warning(loggerWarning,"Realizando Jounal general para mantener consistencias");
						t_infoMem* sk = (t_infoMem*)list_find(sockets,compararNumeroSocket);
						close(sk->socket);
						list_remove_by_condition(sockets,compararNumeroSocket);
						sem_post(&mutexCaido);
						despacharQuery("journal\n",sockets);
					}else{
						t_infoMem* sk = (t_infoMem*)list_find(sockets,compararNumeroSocket);
						close(sk->socket);
						list_remove_by_condition(sockets,compararNumeroSocket);
						sem_post(&mutexCaido);
					}

				}
			}
			free(socket_memoria);
			consultaOk = 1;
			free(serializado);
			break;
		case DROP:
			log_debug(logger, "Se recibio un DROP");
			cargarPaqueteDrop(paqueteDrop,consulta);
			serializado = serializarDrop(paqueteDrop);
			consistencias consis = consTabla(paqueteDrop->nombre_tabla);
			*socket_memoria = *devolverSocket(consis,sockets,1);
			if(socket_memoria->id != -1){
				consultaOk = enviarPaquete(socket_memoria->socket, serializado, paqueteDrop->length);
				if(consultaOk == -1){
					consultaOk = socket_memoria->id * -1;
				}else{
					consultaOk = 1;
				}
			}else{
				if(consis != nada){
					log_error(loggerError,"No existen memorias disponibles para el criterio de la tabla");
				}else{
					log_error(loggerError,"No existe informacion de la tabla, por favor realice un describe");
				}
			}
			free(socket_memoria);
			free(serializado);
			free(paqueteDrop->nombre_tabla);
			break;
		case METRICS:
			sem_wait(&mutexMetrics);
			ejecutarMetrics();
			sem_post(&mutexMetrics);
			consultaOk = 1;
			break;
		default:
			log_error(loggerError,"Se ingreso una consulta no disponible por el momento");
			break;
		}
	}

	free(paqueteCreate);
	free(paqueteInsert);
	free(paqueteSelect);
	free(paqueteDescribe);
	free(paqueteDrop);
	free(paqueteJournal);
	string_iterate_lines(tempSplit,free);
	free(tempSplit);
	return consultaOk;

}




void CPU(){
	t_list *sockets = list_create();
	sem_wait(&mutexContMult);
	contMult++;
	list_add_in_index(listaDeLSockets,contMult,sockets);
	sem_post(&mutexContMult);
	while(continuar){
		script *unScript;
		sem_wait(&semReady);
		sem_wait(&mutexReady);
		unScript = ( script *) queue_pop(colaReady);
		sem_post(&mutexReady);
		sem_wait(&mutexMems);
				if(memorias->elements_count > sockets->elements_count){
					for(int i = 0; i < memorias->elements_count;i++){
						tMemoria* mem = (tMemoria*) list_get(memorias,i);
								bool compararNumeroMem(void* elem){
									t_infoMem* mem2 = (t_infoMem*) elem;
									return mem2->id == mem->numeroMemoria;
								}
								if(list_filter(sockets,compararNumeroMem)->elements_count == 0){
									t_infoMem *sock = malloc(sizeof(t_infoMem));
									sock->socket = levantarClienteNoBloqueante(mem->puerto,mem->ip);
									if(sock->socket != -1){
										sock->id = mem->numeroMemoria;
										list_add(sockets,sock);
										log_debug(logger,"agregue esta memoria %d",sock->id);
										bool compararNumeroMem2(void* elem){
											tMemoria* mem2 = (tMemoria*) elem;
											return mem2->numeroMemoria == mem->numeroMemoria;
										}
										sem_wait(&mutexCaido);
										if(list_filter(memoriasCaidas,compararNumeroMem2)->elements_count != 0){
											list_remove_by_condition(memoriasCaidas,compararNumeroMem2);
										}
										sem_post(&mutexCaido);
									}
								}
								usleep(miConfig->sleep*1000);
					}
				}
				sem_post(&mutexMems);
				sem_wait(&mutexCaido);
				if(memoriasCaidas->elements_count != 0){
					log_warning(loggerWarning,"Hay memorias caidas");
					int size_to_send = sizeof(type);
					char* serializedPackage = malloc(size_to_send);
					type gossip = SIGNAL;
					memcpy(serializedPackage,&(gossip),size_to_send);
					for(int i = 0; i < memoriasCaidas->elements_count;i++){
						tMemoria* mem = (tMemoria*) list_get(memoriasCaidas,i);
						bool compararNumeroMem(void* elem){
							t_infoMem* mem2 = (t_infoMem*) elem;
							return mem2->id == mem->numeroMemoria;
						}
						if(list_filter(sockets,compararNumeroMem)->elements_count != 0){
							t_infoMem* memoria = (t_infoMem*)list_find(sockets,compararNumeroMem);
							int info = enviarPaquete(memoria->socket,serializedPackage,size_to_send);
							if(info == -1){
								t_infoMem* sk = (t_infoMem*)list_find(sockets,compararNumeroMem);
								close(sk->socket);
								list_remove_by_condition(sockets,compararNumeroMem);
							}else{
								list_remove(memoriasCaidas,i);
								recv(memoria->socket,&info,sizeof(int),0);
							}
						}
					}
					free(serializedPackage);
				}
				sem_post(&mutexCaido);
		unScript->estado = exec;
		int info;
		log_debug(logger,"Se esta corriendo el script: %d", unScript->id);
		sem_wait(&mutexQuantum);
		int q = miConfig->quantum;
//		log_debug("este es el quantum: %d",q);
		sem_post(&mutexQuantum);
		for(int i = 0 ; i < q; i++){
			char* consulta = malloc(256);
			info = leerLinea(unScript->path,unScript->pos,consulta);
			unScript->pos++;
			switch(info){
			case 0:
				i = q;
				unScript->estado = exit_;
				log_error(loggerError,"El script rompio en la linea: %d",
						unScript->pos);
				free(unScript->path);
				free(unScript);
				break;
			case 1:
				log_debug(logger,"Enviando linea %d", unScript->pos);
				info = despacharQuery(consulta,sockets);
				if(info!=1){
					if(info < 0){
						int socketAborrar = info*-1;
						log_error(loggerError,"Memoria %d caida, eliminando de la lista", info*-1);
						t_infoMem* mem = malloc(sizeof(mem));
						mem->id = socketAborrar;
						bool compararNumeroMem(void* elem){
							tMemoria* mem2 = (tMemoria*) elem;
							return mem2->numeroMemoria == mem->id;
						}
						bool compararNumeroSocket(void* elem){
							t_infoMem* mem2 = (t_infoMem*) elem;
							return mem2->id == mem->id;
						}
						sem_wait(&mutexCaido);
						if(list_filter(memoriasCaidas,compararNumeroMem)->elements_count == 0){
							tMemoria* memoriaCaida = malloc(sizeof(tMemoria));
							*memoriaCaida = *(tMemoria*) list_find(memorias,compararNumeroMem);
							list_add(memoriasCaidas,memoriaCaida);
							log_warning(loggerWarning,"Realizando Jounal general para mantener consistencias");
							t_infoMem* sk = (t_infoMem*)list_find(sockets,compararNumeroSocket);
							close(sk->socket);
							list_remove_by_condition(sockets,compararNumeroSocket);
							despacharQuery("journal\n",sockets);
							sem_post(&mutexCaido);
						}else{
							t_infoMem* sk = (t_infoMem*)list_find(sockets,compararNumeroSocket);
							close(sk->socket);
							list_remove_by_condition(sockets,compararNumeroSocket);
							sem_post(&mutexCaido);
						}

						i--;
						log_warning(loggerWarning,"Se volvera a ejecutar la consulta: %d",unScript->pos);
						unScript->pos--;
					}else{
						i = q;
						unScript->estado = exit_;
						log_error(loggerError,"El script rompio en la linea: %d",
								unScript->pos);
						free(unScript->path);
						free(unScript);
					}
				}
				break;
			case 2:
				unScript->estado = exit_;
				log_debug(logger,
						"El script id: %d finalizo con exito en la linea: %d",
						unScript->id,
						unScript->pos);
				i = q;
				free(unScript->path);
				free(unScript);
				break;
			}
			free(consulta);
		}
		if(info == 1){
			sem_wait(&mutexReady);
			queue_push(colaReady,unScript);
			log_debug(logger,"el script con id: %d ha vuelto a ready",
					unScript->id);
			sem_post(&semReady);
			sem_post(&mutexReady);
		}
	}
}

void planificador(){
	while(1){
		script *unScript;
		sem_wait(&semNew);
		sem_wait(&mutexNew);
		unScript = ( script *) queue_pop(colaNew);
		sem_post(&mutexNew);
		unScript->estado = ready;
		sem_wait(&mutexReady);
		queue_push(colaReady, unScript);
		log_debug(logger,"Se cargo a la cola de ready el run con id: %d",
				unScript->id);
		sem_post(&semReady);
		sem_post(&mutexReady);
	}
}
void actualizarTablaGossip(tGossip* packGossip){
	void actualizarTabla(void* elemento){
		tMemoria* mem = malloc(sizeof(tMemoria));
		memcpy(mem,(tMemoria*) elemento,sizeof(tMemoria));
		bool compararNumeroMem(void* elem){
			tMemoria* mem2 = (tMemoria*) elem;
			return mem2->numeroMemoria == mem->numeroMemoria;
		}
		sem_wait(&mutexMems);
		if(list_filter(memorias,compararNumeroMem)->elements_count == 0){
			list_add(memorias,mem);
			tMemLoad* memload = malloc(sizeof(tMemLoad));
			memload->mem = mem;
			memload->cont = 0;
			list_add(memLoads,memload);
		}
		sem_post(&mutexMems);
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



void gossip(){
	tMemoria* unaMem = malloc(sizeof(tMemoria));
	int socket_gossip = levantarCliente(miConfig->puerto_mem,miConfig->ip_mem);
	int size_to_send = sizeof(type);
	char* serializedPackage = malloc(size_to_send);
	type gossip = GOSSIPKERNEL;
	memcpy(serializedPackage,&(gossip),size_to_send);
	while(socket_gossip > 0){
		usleep(miConfig->t_gossip*1000);
		int bytes = enviarPaquete(socket_gossip,serializedPackage,size_to_send);
		if(bytes > 0){
			gossip = leerHeader(socket_gossip);
			tGossip *packGossip = malloc(sizeof(tGossip));
			int flag = desSerializarGossip(packGossip,socket_gossip);
			if(flag > 0){
				actualizarTablaGossip(packGossip);
				log_debug(logger,"cantidad memorias: %d",memorias->elements_count);
				free(packGossip);
			}else{
				log_warning(loggerWarning,"Memoria seed caida, volviendo a conectar");
				for(int i = 0; i < memorias->elements_count;i++){
					unaMem = (tMemoria*) list_get(memorias,i);
					socket_gossip = levantarClienteNoBloqueante(unaMem->puerto,unaMem->ip);
					if(socket_gossip != -1){
						i = memorias->elements_count;
					}
				}
				if(socket_gossip == -1){
					log_warning(loggerWarning,"No pude volver a conectarme para hacer gossip, volviendo a intentar...");
				}
			}
		}else{
			log_warning(loggerWarning,"Memoria seed caida, volviendo a conectar");
			for(int i = 0; i < memorias->elements_count;i++){
				unaMem = (tMemoria*) list_get(memorias,i);
				socket_gossip = levantarClienteNoBloqueante(unaMem->puerto,unaMem->ip);
				if(socket_gossip != -1){
					i = memorias->elements_count;
				}
			}
			if(socket_gossip == -1){
				log_warning(loggerWarning,"No pude volver a conectarme para hacer gossip, volviendo a intentar...");
			}
		}
	}
	log_error(loggerError,"No ha sido posible establecer la conexion con la memoria");

}

int levantarCpus(pthread_t cpus[]){
	int devolver = 1;
	for(int i = 0; i < miConfig->MULT_PROC; i++){
		devolver += pthread_create(&cpus[i], NULL, (void*) CPU, NULL);
	}
	return devolver;
}

int leerLinea(char* path, int linea, char* leido){
	log_debug(logger,"%s", path);
	FILE* archivo = fopen(path,"r");
	if(archivo != NULL){
		int cont = 0;
		while (fgets(leido, 256, archivo) != NULL)
		{
			if (cont == linea)
			{
				fclose(archivo);
				return 1; //Salio bien
			}
			else
			{
				cont++;
			}
		}
	}
	else{
		printf("El archivo no existe... \n");
		return 0; //devuelvo 0 si es error
	}
	if(feof(archivo)){
		fclose(archivo);
		return 2; //devuelvo 2 si llegue al fin del archivo
	}
	printf("Algo salio mal \n");
	return 0; //devuelve 0 si no se encontro la linea

}


int validarAdd(char* consulta){
	char** split = string_split(consulta," ");
	bool sintaxis = true;
	int i = 0;
	while(split[i] != NULL && sintaxis){
		string_to_upper(split[i]);
		switch (i){
		case 1:
			if(strcmp(split[i],"MEMORY")){
				sintaxis = false;
			}
			break;
		case 3:
			if(strcmp(split[i],"TO")){
				sintaxis = false;
			}
			break;
		}
		i++;
	}
	string_iterate_lines(split,free);
	free(split);
	return ((5 == i) && sintaxis);
}

void ejecutarAdd(char* consulta){

	char** split = string_split(consulta," ");
	tMemoria* memAdd = malloc(sizeof(tMemoria));
	*memAdd = *(tMemoria*)generarMem(consulta);
	bool mismoId(void* elemento) {
			tMemoria* mem = malloc(sizeof(tMemoria));
			mem = (tMemoria*) elemento;
			int id = mem->numeroMemoria;
			return (atoi(split[2]) == id);
		}
	if(memAdd->numeroMemoria != -1){
		consistencias cons = obtCons(split[4]);
		switch (cons){
		case sc:
			sem_wait(&mutexSC);
			SC = memAdd;
			log_debug(logger,"La memoria %d se asoció al criterio %s",memAdd->numeroMemoria,split[4]);
			sem_post(&mutexSC);
			break;
		case shc:
			sem_wait(&mutexSHC);
			if(list_is_empty(list_filter(listaMemsSHC, mismoId))){
				list_add(listaMemsSHC, memAdd);
				log_debug(logger,"La memoria %d se asoció al criterio %s",memAdd->numeroMemoria,split[4]);
			}else{
				log_error(loggerError,"Ya se encuentra asociada la memoria %d al criterio",memAdd->numeroMemoria);
				free(memAdd);
			}
			sem_post(&mutexSHC);
			despacharQuery("journal\n",(t_list*)list_get(listaDeLSockets,0));
			break;
		case ec:
			sem_wait(&mutexEC);
			if(list_is_empty(list_filter(listaMemsEC, mismoId))){
				list_add(listaMemsEC, memAdd);
				log_debug(logger,"La memoria %d se asoció al criterio %s",memAdd->numeroMemoria,split[4]);
			}else{
				log_error(loggerError,"Ya se encuentra asociada la memoria %d al criterio",memAdd->numeroMemoria);
				free(memAdd);
			}
			sem_post(&mutexEC);
			break;
		}
	}else{
		log_error(loggerError,"La memoria elegida no se encuentra en el pool");
		free(memAdd);
	}
	string_iterate_lines(split,free);
	free(split);
}

tMemoria* generarMem(char* consulta){
	char** split = string_split(consulta," ");
	tMemoria* mem = malloc(sizeof(tMemoria));
	mem->numeroMemoria = atoi(split[2]);
	bool compararNumeroMem(void* elem){
		tMemoria* mem2 = (tMemoria*) elem;
		return mem2->numeroMemoria == mem->numeroMemoria;
	}
	sem_wait(&mutexMems);
	if(list_filter(memorias,compararNumeroMem)->elements_count != 0){
		mem =(tMemoria*) list_find(memorias,compararNumeroMem);
	}else{
		mem->numeroMemoria = -1;
	}
	sem_post(&mutexMems);
	string_iterate_lines(split,free);
	free(split);
	return mem;
}

consistencias obtCons(char* criterio){
	string_to_upper(criterio);
	if(!strcmp(criterio,"SC\n")){
		return sc;
	}
	if(!strcmp(criterio,"SHC\n")){
			return shc;
	}
	if(!strcmp(criterio,"EC\n")){
			return ec;
	}
	if (!strcmp(criterio, "SC")) {
		return sc;
	}
	if (!strcmp(criterio, "SHC")) {
		return shc;
	}
	if (!strcmp(criterio, "EC")) {
		return ec;
	}
}

void ejecutarDescribe(t_describe *response){

	for(int i = 0; i < response->cant_tablas; i++){
		t_metadata *metadata = malloc(sizeof(t_metadata));
		memcpy(metadata,&(response->tablas[i]),sizeof(t_metadata));
		char nombre[12];
		strcpy(nombre, metadata->nombre_tabla);
		bool mismoNombre(void* elemento) {
						t_metadata* tabla = malloc(sizeof(t_metadata));
						memcpy(tabla,(t_metadata*) elemento,sizeof(t_metadata));
						int res =!(strcmp(tabla->nombre_tabla,nombre));
						free(tabla);
						return res;
					}
		if(list_is_empty(list_filter(listaTablas,mismoNombre))){
			list_add(listaTablas,metadata);
			log_debug(logger, "Se agrego la tabla %s a la lista", nombre);
		}else{
			free(metadata);
		}
	}
}
consistencias consTabla (char* nombre){
	bool mismoNombre(void* elemento) {
		t_metadata* tabla = malloc(sizeof(t_metadata));
		tabla = (t_metadata*) elemento;

		return (!strcmp(tabla->nombre_tabla,nombre));
	}
	if(list_any_satisfy(listaTablas,mismoNombre)){
		t_metadata *tabla = (t_metadata*) list_find(listaTablas,mismoNombre);
		log_debug(logger,"esta es la consistencia de la tabla: %d",tabla->consistencia);
		return tabla->consistencia;
	}else{
		return nada;
	}
}

t_infoMem* devolverSocket(consistencias cons, t_list* sockets, int key){
	int pos = -1;
	tMemoria* mem = malloc(sizeof(tMemoria));
	t_infoMem* ret = malloc(sizeof(t_infoMem));
	bool compararNumeroMem(void* elem){
		t_infoMem* mem2 = (t_infoMem*) elem;

		return mem2->id == mem->numeroMemoria;
	}
	switch(cons){
	case sc:
		*mem = *SC;
		if(mem->numeroMemoria != -1){
		ret = (t_infoMem*)list_find(sockets,compararNumeroMem);
		free(mem);
		return ret;
		}else{
			ret->id = -1;
			free(mem);
			return ret;
		}
		break;
	case shc:
		pos = SHC(key);
		mem->numeroMemoria = pos;
		if(pos !=-1){
			ret = (t_infoMem*)list_find(sockets,compararNumeroMem);
			free(mem);
			return ret;
		}else{
			ret->id = -1;
			free(mem);
			return ret;
		}
		break;
	case ec:
		pos = EC((int) time(NULL));
		if(pos!=-1){
			mem->numeroMemoria = pos;
			ret = (t_infoMem*)list_find(sockets,compararNumeroMem);
			free(mem);
			return ret;
		}else{
			ret->id = -1;
			free(mem);
			return ret;
		}
		break;
	default:
		ret->id = -1;
		free(mem);
		return ret;
		break;
	}
}


int SHC(int key){
	sem_wait(&mutexSHC);
	t_list* memsDisp = list_filter(listaMemsSHC,hayCaida);
	int tamanio = memsDisp->elements_count;
	sem_post(&mutexSHC);
	if(tamanio != 0){
		tMemoria* mem= (tMemoria*)list_get(memsDisp,(key % tamanio));
		return mem->numeroMemoria;
	}else{
		return -1;
	}
}

int EC(int time){
	sem_wait(&mutexEC);
	t_list* memsDisp = list_duplicate(list_filter(listaMemsEC,hayCaida));
	int tamanio = memsDisp->elements_count;
	int devolver = -1;
	sem_post(&mutexEC);
	if(tamanio != 0){
		tMemoria* mem = malloc(sizeof(tMemoria));
		*mem = *(tMemoria*)list_get(memsDisp,(rand() % tamanio));
		devolver = mem->numeroMemoria;
		list_destroy(memsDisp);
		free(mem);
		return devolver;
	}else{
		return -1;
	}
}

void cargarConfig(t_config* config){
	config = config_create("../Kernel.config");
	miConfig->ip_mem = malloc(strlen(config_get_string_value(config,"IP"))+1);
	strcpy(miConfig->ip_mem,config_get_string_value(config,"IP"));
	miConfig->metadata_refresh = config_get_int_value(config, "METADATA_REFRESH");
	miConfig->MULT_PROC = config_get_int_value(config, "MULTIPROCESAMIENTO");
	miConfig->puerto_mem = malloc(strlen(config_get_string_value(config,"PUERTO_MEMORIA"))+1);
	strcpy(miConfig->puerto_mem,config_get_string_value(config,"PUERTO_MEMORIA"));
	miConfig->sleep = config_get_int_value(config,"SLEEP_EJECUCION");
	sem_wait(&mutexQuantum);
	miConfig->quantum = config_get_int_value(config,"QUANTUM");
	sem_post(&mutexQuantum);
	miConfig->t_gossip = config_get_int_value(config,"TIEMPO_GOSSIP");
	log_debug(logger,"este es mi quantum: %d", miConfig->quantum);
	log_debug(logger,"Se tendra un nivel de multiprocesamiento de: %d cpus", miConfig->MULT_PROC);
	config_destroy(config);
}

void inicializarTodo(){
	contScript = 1;
	logger = log_create("Kernel.log","Kernel.c",true,LOG_LEVEL_DEBUG);
	loggerError = log_create("Kernel.log","Kernel.c",true,LOG_LEVEL_ERROR);
	loggerWarning = log_create("Kernel.log","Kernel.c",true,LOG_LEVEL_WARNING);
	sem_init(&mutexQuantum,0,1);
	log_debug(logger,"Cargando configuracion");
	t_config* config;
	cargarConfig(config);
	log_debug(logger,"Configuracion cargada con exito");
	log_debug(logger,"Inicializando semaforos");
	sem_init(&semReady,0,0);
	sem_init(&semNew,0,0);
	sem_init(&mutexNew,0,1);
	sem_init(&mutexReady,0,1);
	sem_init(&mutexEC,0,1);
	sem_init(&mutexSC,0,1);
	sem_init(&mutexSHC,0,1);
	sem_init(&mutexContMult,0,1);
	sem_init(&mutexMems,0,1);
	sem_init(&mutexMetrics,0,1);
	sem_init(&mutexCaido,0,1);
	log_debug(logger,"Semaforos incializados con exito");
	log_debug(logger,"Inicializando sockets");
	metricsEC.acumtInsert = 0;
	metricsEC.acumtSelect = 0;
	metricsEC.contInsert = 0;
	metricsEC.contSelect = 0;
	metricsSC.acumtInsert = 0;
	metricsSC.acumtSelect = 0;
	metricsSC.contInsert = 0;
	metricsSC.contSelect = 0;
	metricsSHC.acumtInsert = 0;
	metricsSHC.acumtSelect = 0;
	metricsSHC.contInsert = 0;
	metricsSHC.contSelect = 0;
	listaMemsEC = list_create();
	listaMemsSHC = list_create();
	listaTablas = list_create();
	memorias = list_create();
	memLoads = list_create();
}

void innotificar() {
	while (1) {
		char buffer[BUF_LEN];
		int file_descriptor = inotify_init();
		if (file_descriptor < 0)
			perror("inotify_init");
		int watch_descriptor = inotify_add_watch(file_descriptor, "../", IN_MODIFY | IN_CREATE | IN_DELETE);
		int length = read(file_descriptor, buffer, BUF_LEN);
		if (length < 0)
			perror("read");
		int offset = 0;
		while (offset < length) {
			struct inotify_event *event = (struct inotify_event *) &buffer[offset];
			if (event->len) {
				if (event->mask & IN_CREATE) {
					if (event->mask & IN_ISDIR)
						printf("The directory %s was created.\n", event->name);
					else
						printf("The file %s was created.\n", event->name);
				} else if (event->mask & IN_DELETE) {
					if (event->mask & IN_ISDIR)
						printf("The directory %s was deleted.\n", event->name);
					else
						printf("The file %s was deleted.\n", event->name);
				} else if (event->mask & IN_MODIFY) {
					if (event->mask & IN_ISDIR)
						printf("The directory %s was modified.\n", event->name);
					else {
						printf("The file %s was modified.\n", event->name);
						t_config* config;
						cargarConfig(config);
					}
				}
			}
			offset += sizeof(struct inotify_event) + event->len;
		}
		inotify_rm_watch(file_descriptor, watch_descriptor);
		close(file_descriptor);
	}
}

void liberar(void* dato){
	free(dato);
}

void finalizarEjecucion() {
	printf("------------------------\n");
	printf("¿chau chau adios?\n");
	printf("------------------------\n");
	log_destroy(logger);
	log_destroy(loggerError);
	free(SC);
	list_destroy_and_destroy_elements(memorias,liberar);
	list_destroy_and_destroy_elements(memLoads,liberar);
	list_destroy_and_destroy_elements(listaTablas,liberar);
	sem_destroy(&semReady);
	sem_destroy(&semNew);
	sem_destroy(&mutexNew);
	sem_destroy(&mutexReady);
	sem_destroy(&mutexEC);
	sem_destroy(&mutexSC);
	sem_destroy(&mutexSHC);
	sem_destroy(&mutexContMult);
	sem_destroy(&mutexMems);
	sem_destroy(&mutexMetrics);
	sem_destroy(&mutexCaido);
	for(int i = 0;i < contMult; i++){
		t_list* lista = malloc(sizeof(t_list));
		lista = (t_list*) list_get(listaDeLSockets,i);
		for(int j = 0; j < list_size(lista);j++){
			int* sock = list_get(lista,j);
			close(sock[0]);
		}
	}

	continuar = false;
	//close(socket_memoria);
	raise(SIGTERM);
}
