/*
 * Kernel.c
 *
 *  Created on: 11 abr. 2019
 *      Author: utnso
 */

#include "Kernel.h"

#define IP "127.0.0.1"
#define PUERTOMEM "7878"
#define BACKLOG 5			// Define cuantas conexiones vamos a mantener pendientes al mismo tiempo
#define PACKAGESIZE 1024	// Define cual va a ser el size maximo del paquete a enviar
#define QUANTUM 4
#define MULT_PROC 3

char package[PACKAGESIZE];
struct addrinfo hints;
struct addrinfo *serverInfo;
sem_t mutexSocket;
sem_t semReady;
sem_t mutexReady;
sem_t mutexNew;
sem_t semNew;
pthread_t cpus[MULT_PROC];
pthread_t planificador_t;
t_log *logger;
t_queue *colaReady;
t_queue *colaNew;

// Define cual va a ser el size maximo del paquete a enviar

int main() {
	logger = log_create("Kernel.log","Kernel.c",true,LOG_LEVEL_DEBUG);
	log_debug(logger,"Inicializando semaforos");
	sem_init(&semReady,0,0);
	sem_init(&semNew,0,0);
	sem_init(&mutexNew,0,1);
	sem_init(&mutexReady,0,1);
	sem_init(&mutexSocket,0,1);
	log_debug(logger,"Semaforos incializados con exito");
	log_debug(logger,"Inicializando sockets");
	int socket_memoria = levantarCliente(PUERTOMEM, IP);
	log_debug(logger,"Sockets inicializados con exito");
	log_debug(logger,"Se tendra un nivel de multiprocesamiento de: %d cpus", MULT_PROC);
	if(levantarCpus(socket_memoria)){
		colaReady = queue_create();
		colaNew = queue_create();
		pthread_create(&planificador_t, NULL, (void*) planificador, NULL);
		char consulta[256] = "";
			while (1) {

				fgets(consulta, 256, stdin);
				int consultaOk = despacharQuery(consulta, socket_memoria);
				if (!consultaOk) {
					printf("no entendi tu header \n");
				}
				consultaOk = 0;
			}
			for(int i = 0; i < MULT_PROC; i++){
				pthread_join(cpus[i],NULL);
			}
			pthread_join(planificador_t,NULL);
	}else{
		printf("No se han podido levantar las cpus... \n");
	}
	sem_destroy(&semReady);
	sem_destroy(&semNew);
	sem_destroy(&mutexNew);
	sem_destroy(&mutexReady);
	sem_destroy(&mutexSocket);
	close(socket_memoria);
}

type validarSegunHeader(char* header) {
	if (!strcmp(header, "SELECT")) {
		return SELECT;
	}
	if (!strcmp(header, "INSERT")) {
		return INSERT;
	}
	if (!strcmp(header, "RUN")) {
			return RUN;
	}

	return NIL;
}

void cargarPaqueteSelect(tSelect *pack, char* cons) {
	char** spliteado;
	spliteado = string_n_split(cons, 3, " ");
	if (strcmp(spliteado[1], "") && strcmp(spliteado[2], "")) {
		pack->type = SELECT;
		pack->nombre_tabla = spliteado[1];
		pack->nombre_tabla_long = strlen(spliteado[1]) + 1;
		pack->key = atoi(spliteado[2]);
		pack->length = sizeof(pack->type) + sizeof(pack->nombre_tabla_long)
				+ pack->nombre_tabla_long + sizeof(pack->key);
	} else {
		printf("no entendi tu consulta\n");
	}
}
void cargarPaqueteInsert(tInsert *pack, char* cons) {
	char** spliteado;
	spliteado = string_n_split(cons, 4, " ");
	if (strcmp(spliteado[1], "") && strcmp(spliteado[2], "")
			&& strcmp(spliteado[3], "")) {
		pack->type = INSERT;
		pack->nombre_tabla = spliteado[1];
		pack->nombre_tabla_long = strlen(spliteado[1]) + 1;
		pack->key = atoi(spliteado[2]);
		pack->value = spliteado[3];
		pack->value_long = strlen(spliteado[3])+1;
		pack->length = sizeof(pack->type) + sizeof(pack->nombre_tabla_long)
				+ pack->nombre_tabla_long + sizeof(pack->key)
				+ sizeof(pack->value_long) + pack->value_long;
	} else {
		printf("no entendi tu consulta\n");
	}
}

void cargarPaqueteCreate(tCreate *pack, char* cons) {
	char** spliteado;
	spliteado = string_n_split(cons, 5, " ");
	if (strcmp(spliteado[1], "") && strcmp(spliteado[2], "")
			&& strcmp(spliteado[3], "") && strcmp(spliteado[4], "")) {
		pack->type = INSERT;
		pack->nombre_tabla = spliteado[1];
		pack->nombre_tabla_long = strlen(spliteado[1]) + 1;
		pack->consistencia = atoi(spliteado[2]);
		pack->particiones = atoi(spliteado[3]);
		pack->compaction_time = atoi(spliteado[4]);
		pack->length = sizeof(pack->type) + sizeof(pack->nombre_tabla_long)
				+ pack->nombre_tabla_long + sizeof(pack->consistencia)
				+ sizeof(pack->particiones) + sizeof(pack->compaction_time);
	} else {
		printf("no entendi tu consulta\n");
	}
}


int despacharQuery(char* consulta, int socket_memoria) {
	char** tempSplit;
	tSelect* paqueteSelect=malloc(sizeof(tSelect));
	tInsert* paqueteInsert=malloc(sizeof(tInsert));
	tCreate* paqueteCreate = malloc(sizeof(tCreate));
	type typeHeader;
	char* serializado = "";
	int consultaOk = 0;
	tempSplit = string_n_split(consulta, 2, " ");
	script *unScript = malloc(sizeof(script));
	if (strcmp(tempSplit[0], "")) {
		typeHeader = validarSegunHeader(tempSplit[0]);
		switch (typeHeader) {
		case SELECT:
			if(validarSelect(consulta)){
				log_debug(logger,"Se recibio un SELECT");
				cargarPaqueteSelect(paqueteSelect, consulta);
				serializado = serializarSelect(paqueteSelect);
				sem_wait(&mutexSocket);
				enviarPaquete(socket_memoria, serializado, paqueteSelect->length);
				sem_post(&mutexSocket);
			}else{
				printf("Por favor ingrese la consulta en formato correcto");
			}
			consultaOk = 1;
			break;
		case INSERT:
			if(valiadarInsert(consulta)){
				log_debug(logger,"Se recibio un INSERT");
				cargarPaqueteInsert(paqueteInsert,
						string_substring_until(consulta,string_length(consulta)-1 ) );
				serializado = serializarInsert(paqueteInsert);
				sem_wait(&mutexSocket);
				enviarPaquete(socket_memoria, serializado, paqueteInsert->length);
				sem_post(&mutexSocket);
			}else{
				printf("Por favor ingrese la consulta en formato correcto");
			}
			consultaOk = 1;
			break;
		case CREATE:
			if(validarCreate(consulta)){
				log_debug(logger,"Se recibio un CREATE");
				cargarPaqueteCreate(paqueteCreate,
						string_substring_until(consulta,string_length(consulta)-1 ) );
				serializado = serializarCreate(paqueteCreate);
				sem_wait(&mutexSocket);
				enviarPaquete(socket_memoria, serializado, paqueteCreate->length);
				sem_post(&mutexSocket);
			}else{
				printf("Por favor ingrese la consulta en formato correcto");
			}
			consultaOk = 1;
			break;
		case RUN:
			printf("%s \n", tempSplit[1]);
			unScript->path = string_substring_until(tempSplit[1],
					string_length(tempSplit[1])-1);
			unScript->pos = 0;
			unScript->estado = new;
			sem_wait(&mutexNew);
			queue_push(colaNew, unScript);
			sem_post(&semNew);
			sem_post(&mutexNew);
			/*args.archivoLQL = archivoLQL;
			args.socket_memoria = socket_memoria;
			pthread_create(&threadRun, NULL, (void*) rutinaRUN, &args);*/
			consultaOk = 1;
			break;
		default:
			printf("para macho que no se, estoy aprendiendo\n");
			break;
		}

	}
	free(paqueteCreate);
	free(paqueteInsert);
	free(paqueteSelect);
	free(unScript);
	return consultaOk;
}

void CPU(int socket_memoria){
	while(1){
		script *unScript = malloc(sizeof(script));
		char* consulta = malloc(256);
		sem_wait(&semReady);
		sem_wait(&mutexReady);
		unScript = ( script *) queue_pop(colaReady);
		sem_post(&mutexReady);
		unScript->estado = exec;
		int info;
		for(int i = 0 ; i < QUANTUM; i++){
			info = leerLinea(unScript->path,unScript->pos,consulta);
			unScript->pos++;
			switch(info){
			case 0:
				i = QUANTUM;
				unScript->estado = exit_;
				break;
			case 1:
				despacharQuery(consulta,socket_memoria);
				break;
			case 2:
				unScript->estado = exit_;
				i = QUANTUM;
			}
		}
		if(info == 1){
			sem_wait(&mutexReady);
			queue_push(colaReady,unScript);
			sem_post(&semReady);
			sem_post(&mutexReady);
		}
		free(consulta);
		free(unScript);
	}
}

void planificador(){
	while(1){
		script *unScript = malloc(sizeof(script));
		sem_wait(&semNew);
		sem_wait(&mutexNew);
		unScript = ( script *) queue_pop(colaNew);
		sem_post(&mutexNew);
		unScript->estado = ready;
		sem_wait(&mutexReady);
		queue_push(colaReady, unScript);
		sem_post(&semReady);
		sem_post(&mutexReady);
	}
}

int levantarCpus(int socket_memoria){
	int devolver = 1;
	for(int i = 0; i < MULT_PROC; i++){
		devolver += pthread_create(&cpus[i], NULL, (void*) CPU, (int *) socket_memoria);
	}
	return devolver;
}

int leerLinea(char* path, int linea, char* leido){
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
	return 3 == i;
}
int validarInsert(char* consulta){
	char** split = string_split(consulta," ");
	int i = 0;
	while(split[i] != NULL){
		i++;
	}
	return 4 == i;
}
int validarCreate(char* consulta){
	char** split = string_split(consulta," ");
	int i = 0;
	while(split[i] != NULL){
		i++;
	}
	return 5 == i;
}
