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
configKernel *miConfig;
t_config* config;
int contScript;
bool continuar = true;
int contMult = 0;
int cantMemorias = 1;

// Define cual va a ser el size maximo del paquete a enviar


int main(){
	signal(SIGINT, finalizarEjecucion);
	inicializarTodo();
	SC = malloc(sizeof(tMemoria));
	SC->numeroMemoria = -1;
	t_infoMem* socket_memoria = malloc(sizeof(t_infoMem));
	socket_memoria->socket = levantarCliente(miConfig->puerto_mem, miConfig->ip_mem);
	socket_memoria->id = 1;
	tMemoria *mem = malloc(sizeof(tMemoria));
	strcpy(mem->ip,miConfig->ip_mem);
	strcpy(mem->puerto,miConfig->puerto_mem);
	mem->numeroMemoria = 1;
	list_add(memorias,mem);
	log_debug(logger,"Sockets inicializados con exito");
	log_debug(logger,"Se tendra un nivel de multiprocesamiento de: %d cpus", miConfig->MULT_PROC);
	log_debug(logger, "Realizando describe general");
	pthread_t cpus[miConfig->MULT_PROC];
	t_list *sockets = list_create();
	listaDeLSockets = list_create();
	memoriasCaidas = list_create();
	list_add_in_index(sockets,0,socket_memoria);
	list_add_in_index(listaDeLSockets,contMult,sockets);
	despacharQuery("describe\n",sockets);
	if(levantarCpus(cpus)){
		colaReady = queue_create();
		colaNew = queue_create();
		pthread_create(&planificador_t, NULL, (void*) planificador, NULL);
		pthread_create(&hiloGossip,NULL,(void*) gossip,NULL);
		pthread_create(&hiloMetrics,NULL,(void*)metrics,NULL);
		pthread_create(&hiloInnotify, NULL, (void*) innotificar, NULL);
		pthread_create(&hiloDescribe,NULL,describe,NULL);
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
						int info = enviarPaquete(((t_infoMem*)list_get(sockets,0))->socket,serializedPackage,size_to_send);
						if(info == -1){
							list_remove_by_condition(sockets,compararNumeroMem);
						}else{
							list_remove(memoriasCaidas,i);
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

void describe (){
	t_list* sockets = list_create();
	t_infoMem* mem = malloc(sizeof(t_infoMem));
	mem->id =((tMemoria*)list_get(memorias,0))->numeroMemoria;
	mem->socket = levantarClienteNoBloqueante(miConfig->puerto_mem,miConfig->ip_mem);
	list_add_in_index(sockets,0,mem);
	while(1){
		usleep(miConfig->metadata_refresh*1000);
		despacharQuery("DESCRIBE\n",sockets);
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

void metricsInsert(consistencias cons,int time){
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
void metricsSelect(consistencias cons,int time){
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
		sem_post(&mutexMetrics);
		sleep(30);
	}

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
				int comienzo = clock();
				log_debug(logger,"Se recibio un SELECT");
				cargarPaqueteSelect(paqueteSelect, consulta);
				serializado = serializarSelect(paqueteSelect);
				consistencias cons = consTabla(paqueteSelect->nombre_tabla);
				socket_memoria = devolverSocket(cons,sockets,paqueteSelect->key);
				log_debug(logger,"uso este socket: %d",socket_memoria->id);
				if(socket_memoria->id != -1){
					consultaOk = enviarPaquete(socket_memoria->socket, serializado, paqueteSelect->length);
					recv(socket_memoria->socket, &error, sizeof(error), 0);
					if(consultaOk != -1){
						if (error == -1) {
							log_error(logger, "Memoria llena, hago JOURNAL");
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
						desSerializarRegistro(reg,socket_memoria->socket);
						log_debug(logger,"Value: %s",reg->value);
						free(reg->value);
						free(reg);
						metricsSelect(cons,(clock()-comienzo)/CLOCKS_PER_SEC);
						consultaOk = 1;
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
			break;
		case INSERT:
			if(validarInsert(consulta)){
				int error = 0;
				int comienzo = clock();
				log_debug(logger,"Se recibio un INSERT");
				char* sinFin = string_new();
				string_append(&sinFin, string_substring_until(consulta,strlen(consulta)-1 ));
				cargarPaqueteInsert(paqueteInsert, sinFin);

				serializado = serializarInsert(paqueteInsert);
				log_debug(logger,"cargue el paquete y serialize");
				consistencias cons = consTabla(paqueteInsert->nombre_tabla);
				socket_memoria = devolverSocket(cons,sockets,paqueteInsert->key);
				if(socket_memoria->id != -1){
					log_debug(logger,"%s",paqueteInsert->value);
					consultaOk = enviarPaquete(socket_memoria->socket, serializado, paqueteInsert->length);
					recv(socket_memoria->socket, &error, sizeof(int), 0);
					log_warning(loggerWarning, "este es el error: %d",error);
					if(consultaOk != -1){
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
							metricsInsert(cons,(clock()-comienzo)/CLOCKS_PER_SEC);
						}else{
							if(error == -2){
								log_debug(logger,"Tamanio de value demasiado grande");
								consultaOk = 0;
							}
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
			break;
		case CREATE:
			if(validarCreate(consulta)){
				log_debug(logger,"Se recibio un CREATE");
				char* sinFin = string_substring_until(consulta,string_length(consulta)-1 );
				cargarPaqueteCreate(paqueteCreate,sinFin);
				serializado = serializarCreate(paqueteCreate);
				log_debug(logger,"serialize todo");
				socket_memoria = devolverSocket(obtCons(paqueteCreate->consistencia)
						,sockets,1);
				log_debug(logger,"voy a usar este socket: %d",socket_memoria->id);
				if(socket_memoria->id != -1){
					consultaOk = enviarPaquete(socket_memoria->socket, serializado, paqueteCreate->length);
					if(consultaOk == -1){
						consultaOk = socket_memoria->id * -1;
					}else{
						consultaOk = 1;
					}
				}else{
					log_error(loggerError,"No existen memorias disponibles para el criterio de la tabla");
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
			unScript->path = sinFin;
			unScript->pos = 0;
			unScript->estado = new;
			unScript->id = contScript;
			contScript++;
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
			socket_memoria = list_get(sockets,0);
			consultaOk = enviarPaquete(socket_memoria->socket,serializado,paqueteDescribe->length);
			if(consultaOk != -1){
				t_describe* response = malloc(sizeof(t_describe));
				desserializarDescribe_Response(response,socket_memoria->socket);
				if(response->cant_tablas != 0){
					log_debug(logger,"Cant tablas: %d", response->cant_tablas);
					ejecutarDescribe(response);
				}else{
					log_error(loggerWarning, "No se recibio ninguna tabla");
				}
				free(response->tablas);
				free(response);
				consultaOk = 1;
			}else{
				consultaOk = socket_memoria->id * -1;
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
				socket_memoria = list_get(sockets,i);
				enviarPaquete(socket_memoria->socket, serializado, paqueteJournal->length);
			}
			free(serializado);
			break;
		case DROP:
			log_debug(logger, "Se recibio un DROP");
			cargarPaqueteDrop(paqueteDrop,consulta);
			serializado = serializarDrop(paqueteDrop);
			consistencias consis = consTabla(paqueteDrop->nombre_tabla);
			socket_memoria = devolverSocket(consis,sockets,1);
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
			free(serializado);
			free(paqueteDrop->nombre_tabla);
			break;
		case METRICS:
			sem_wait(&mutexMetrics);
			log_warning(loggerWarning,"CRITERIO SC: ");
			int latencyRead = metricsSC.acumtSelect / 30;
			int latencyWrite = metricsSC.acumtInsert / 30;
			log_debug(logger, "READ LATENCY: %d, WRITE LATENCY: %d, READS: %d, WRITES: %d",
					latencyRead,latencyWrite,metricsSC.contSelect,metricsSC.contInsert);
			log_warning(loggerWarning,"CRITERIO EC: ");
			latencyRead = metricsEC.acumtSelect / 30;
			latencyWrite = metricsEC.acumtInsert / 30;
			log_debug(logger, "READ LATENCY: %d, WRITE LATENCY: %d, READS: %d, WRITES: %d",
					latencyRead,latencyWrite,metricsEC.contSelect,metricsEC.contInsert);
			log_warning(loggerWarning,"CRITERIO SHC: ");
			latencyRead = metricsSHC.acumtSelect / 30;
			latencyWrite = metricsSHC.acumtInsert / 30;
			log_debug(logger, "READ LATENCY: %d, WRITE LATENCY: %d, READS: %d, WRITES: %d",
					latencyRead,latencyWrite,metricsSHC.contSelect,metricsSHC.contInsert);
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
		char* consulta = malloc(256);
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
							int info = enviarPaquete(((t_infoMem*)list_get(sockets,0))->socket,serializedPackage,size_to_send);
							if(info == -1){
								list_remove_by_condition(sockets,compararNumeroMem);
							}else{
								list_remove(memoriasCaidas,i);
							}
						}
					}
					free(serializedPackage);
				}
				sem_post(&mutexCaido);
		unScript->estado = exec;
		int info;
		log_debug(logger,"Se esta corriendo el script: %d", unScript->id);
		for(int i = 0 ; i < miConfig->quantum; i++){
			info = leerLinea(unScript->path,unScript->pos,consulta);
			switch(info){
			case 0:
				i = miConfig->quantum;
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
						sem_wait(&mutexCaido);
						if(list_filter(memoriasCaidas,compararNumeroMem)->elements_count == 0){
							list_add(memoriasCaidas,list_find(memorias,compararNumeroMem));
							log_warning(loggerWarning,"Realizando Jounal general para mantener consistencias");
							despacharQuery("journal\n",sockets);
						}
						sem_post(&mutexCaido);
						bool compararNumeroSocket(void* elem){
							t_infoMem* mem2 = (t_infoMem*) elem;
							return mem2->id == mem->id;
						}
						list_remove_by_condition(sockets,compararNumeroSocket);
						i--;
						log_warning(loggerWarning,"Se volvera a ejecutar la consulta: %d",unScript->pos);
						unScript->pos--;
					}else{
						i = miConfig->quantum;
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
				i = miConfig->quantum;
				free(unScript->path);
				free(unScript);
				break;
			}
			unScript->pos++;
		}
		if(info == 1){
			sem_wait(&mutexReady);
			queue_push(colaReady,unScript);
			log_debug(logger,"el script con id: %d ha vuelto a ready",
					unScript->id);
			sem_post(&semReady);
			sem_post(&mutexReady);
		}
		free(consulta);
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
		tMemoria* mem = (tMemoria*) elemento;
		bool compararNumeroMem(void* elem){
			tMemoria* mem2 = (tMemoria*) elem;
			return mem2->numeroMemoria == mem->numeroMemoria;
		}
		sem_wait(&mutexMems);
		if(list_filter(memorias,compararNumeroMem)->elements_count == 0){
			list_add(memorias,mem);
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
	int socket_gossip = levantarCliente(miConfig->puerto_mem,miConfig->ip_mem);
	int size_to_send = sizeof(type);
	char* serializedPackage = malloc(size_to_send);
	type gossip = GOSSIPKERNEL;
	memcpy(serializedPackage,&(gossip),size_to_send);
	while(socket_gossip > 0){
		usleep(miConfig->t_gossip*1000);
		enviarPaquete(socket_gossip,serializedPackage,size_to_send);
		gossip = leerHeader(socket_gossip);
		tGossip *packGossip = malloc(sizeof(tGossip));
		desSerializarGossip(packGossip,socket_gossip);
		actualizarTablaGossip(packGossip);
		log_debug(logger,"cantidad memorias: %d",memorias->elements_count);
		free(packGossip);
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
int validarSelect(char* consulta){
	char** split = string_split(consulta," ");
	int i = 0;
	while(split[i] != NULL){
		i++;
	}
	string_iterate_lines(split,free);
	free(split);
	return 3 == i;
}
int validarInsert(char* consulta){
	char** value = string_split(consulta,"\"");
	char** split = string_split(value[0]," ");
	int i = 0;
	while(split[i] != NULL){
		i++;
	}
	int j = 0;
	while(value[j] != NULL){
			j++;
		}
	string_iterate_lines(split,free);
	free(split);
	string_iterate_lines(value,free);
	free(value);
	return ((3 == i) && (3 == j)) ;
}
int validarCreate(char* consulta){
	char** split = string_split(consulta," ");
	int i = 0;
	while(split[i] != NULL){
		i++;
	}
	string_iterate_lines(split,free);
	free(split);
	return 5 == i;
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
	log_debug(logger,"entre");
	char** split = string_split(consulta," ");
	log_debug(logger,"voy a generar la memoria");
	tMemoria* memAdd;
	memAdd = (tMemoria*)generarMem(consulta);
	bool mismoId(void* elemento) {
			tMemoria* mem = malloc(sizeof(tMemoria));
			mem = (tMemoria*) elemento;
			int id = mem->numeroMemoria;
			log_debug(logger,"%d",id);
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
			}
			sem_post(&mutexEC);
			break;
		}
	}else{
		free(memAdd);
		log_error(loggerError,"La memoria elegida no se encuentra en el pool");
	}
	string_iterate_lines(split,free);
	free(split);
}

tMemoria* generarMem(char* consulta){
	char** split = string_split(consulta," ");
	tMemoria* mem = malloc(sizeof(tMemoria));
	mem->numeroMemoria = atoi(split[2]);
	log_debug(logger,"generar mem: %d",mem->numeroMemoria);
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
						tabla = (t_metadata*) elemento;
						return (!strcmp(tabla->nombre_tabla,nombre));
					}
		if(list_is_empty(list_filter(listaTablas,mismoNombre))){
			list_add(listaTablas,metadata);
			log_debug(logger, "Se agrego la tabla %s a la lista", nombre);
		}
	}
}
consistencias consTabla (char* nombre){
	bool mismoNombre(void* elemento) {
		t_metadata* tabla = malloc(sizeof(t_metadata));
		tabla = (t_metadata*) elemento;
		log_debug(logger,"nombre tabla: %s",tabla->nombre_tabla);
		log_debug(logger,"nombre que quiero: %s", nombre);
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
		log_debug(logger,"este es el socket que me llego: %d", mem2->id);
		return mem2->id == mem->numeroMemoria;
	}
	switch(cons){
	case sc:
		mem = SC;
		if(mem->numeroMemoria != -1){
		return (t_infoMem*)list_find(sockets,compararNumeroMem);
		}else{
			ret->id = -1;
			return ret;
		}
		break;
	case shc:
		pos = SHC(key);
		mem->numeroMemoria = pos;
		if(pos !=-1){
			ret = (t_infoMem*)list_find(sockets,compararNumeroMem);
			return ret;
		}else{
			ret->id = -1;
			return ret;
		}
		break;
	case ec:
		pos = EC((int) time(NULL));
		if(pos!=-1){
			mem->numeroMemoria = pos;
			log_debug(logger,"aca esta el resultado de aplicar la consistencia: %d", mem->numeroMemoria);
			ret = (t_infoMem*)list_find(sockets,compararNumeroMem);
			return ret;
		}else{
			ret->id = -1;
			return ret;
		}
		break;
	default:
		ret->id = -1;
		return ret;
		break;
	}
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

int SHC(int key){
	sem_wait(&mutexSHC);
	int tamanio = list_size(list_filter(listaMemsSHC,hayCaida));
	sem_post(&mutexSHC);
	if(tamanio != 0){
		tMemoria* mem= (tMemoria*)list_get(listaMemsSHC,(key % tamanio));
		log_debug(logger,"lo mando a esta memoria: %d", mem->numeroMemoria);
		return mem->numeroMemoria;
	}else{
		return -1;
	}
}

int EC(int time){
	sem_wait(&mutexEC);
	int tamanio = list_size(list_filter(listaMemsEC,hayCaida));
	log_debug(logger,"este es el tamanio: %d", tamanio);
	sem_post(&mutexEC);
	if(tamanio != 0){
		log_debug(logger,"este es el time: %d", time);
		tMemoria* mem= (tMemoria*)list_get(listaMemsEC,(time % tamanio));
		return mem->numeroMemoria;
	}else{
		return -1;
	}
}

void cargarConfig(t_config* config){
	miConfig = malloc(sizeof(configKernel));
	miConfig->ip_mem = config_get_string_value(config,"IP");
	miConfig->metadata_refresh = config_get_int_value(config, "METADATA_REFRESH");
	miConfig->MULT_PROC = config_get_int_value(config, "MULTIPROCESAMIENTO");
	miConfig->puerto_mem = config_get_string_value(config,"PUERTO_MEMORIA");
	miConfig->sleep = config_get_int_value(config,"SLEEP_EJECUCION");
	miConfig->quantum = config_get_int_value(config,"QUANTUM");
	miConfig->t_gossip = config_get_int_value(config,"TIEMPO_GOSSIP");

}

void inicializarTodo(){
	contScript = 1;
	logger = log_create("Kernel.log","Kernel.c",true,LOG_LEVEL_DEBUG);
	loggerError = log_create("Kernel.log","Kernel.c",true,LOG_LEVEL_ERROR);
	loggerWarning = log_create("Kernel.log","Kernel.c",true,LOG_LEVEL_WARNING);
	log_debug(logger,"Cargando configuracion");
	config = config_create("Kernel.config");
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
	listaMemsEC = list_create();
	listaMemsSHC = list_create();
	listaTablas = list_create();
	memorias = list_create();

}

void innotificar() {
	while (1) {
		char buffer[BUF_LEN];
		int file_descriptor = inotify_init();
		if (file_descriptor < 0)
			perror("inotify_init");
		int watch_descriptor = inotify_add_watch(file_descriptor, "/", IN_MODIFY | IN_CREATE | IN_DELETE);
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

void finalizarEjecucion() {
	printf("------------------------\n");
	printf("¿chau chau adios?\n");
	printf("------------------------\n");
	log_destroy(logger);
	log_destroy(loggerError);
	free(SC);
	list_iterate(listaMemsEC,free);
	list_iterate(listaMemsSHC,free);
	list_iterate(memorias,free);
	//list_iterate(memoriasCaidas,free);
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
			log_warning(loggerWarning,"sandanga");
			close(sock[0]);
		}
	}

	continuar = false;
	//close(socket_memoria);
	raise(SIGTERM);
}
