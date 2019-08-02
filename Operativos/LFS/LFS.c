/*
 * LFS.c
 *
 *  Created on: 8 abr. 2019
 *      Author: USUARIO
 */

#include "LFS.h"

// ---------------VARIABLES GLOBALES-----------------

int socket_sv;
int socket_cli;
//int cantidadDeDumpeos = 0;
t_log* logger;
t_list* memtable;
config_FS* configLFS;
metadataFS* configMetadata;
pthread_mutex_t mem_table_mutex;
pthread_mutex_t bitmapMutex;

pthread_t hiloSocket;
pthread_t hiloCompactacion;
pthread_t hiloConsola;
pthread_t hiloInnotify;
pthread_t hiloDumpeo;
//pthread_mutex_t cantidadDeDumpeos_mutex;
t_dictionary* bloqueoTablas;
// ---------------MAIN-----------------

int main(void) {
	configLFS = malloc(sizeof(config_FS));
	configMetadata = malloc(sizeof(metadataFS));


	// ---------------Inicializacion-----------------
	memtable = inicializarMemtable();
	logger = iniciar_logger();
	levantarMetadataFS();
	levantarConfigLFS();
	pthread_mutex_init(&mem_table_mutex, NULL);
	pthread_mutex_init(&bitmapMutex, NULL);
	//pthread_mutex_init(&cantidadDeDumpeos_mutex, NULL);
	bloqueoTablas = dictionary_create();
	// ---------------Pruebas con bitmap propio-----------------
	 crearBitmapNuestro();

	// Escribir inicio en consola

	// Abro hilo de consola
	paramsConsola = malloc(sizeof(tHiloConsola));

	(paramsConsola->header)=malloc(sizeof(type));

	*(paramsConsola->header) = NIL;
	pthread_create(&hiloConsola, NULL, (void*) abrirHiloConsola, paramsConsola);
	pthread_create(&hiloDumpeo, NULL, (void*) hiloDump, NULL);

	// Abro hilo de compactacion
	pthread_create(&hiloCompactacion, NULL, (void*) abrirHiloCompactacion,
	NULL);

// Abro hilo de socket
	pthread_create(&hiloSocket, NULL, (void*) abrirHiloSockets, NULL);

	pthread_create(&hiloInnotify, NULL, (void*) innotificar, NULL);
	//todo: Abrir hilo dumpeo
	signal(SIGINT, finalizarEjecutcion); //Comando de cierre al cortar con Ctrl+C
	pthread_join(hiloConsola, NULL);
	pthread_join(hiloDumpeo, NULL);
	pthread_join(hiloCompactacion, NULL);
	pthread_join(hiloSocket, NULL);
	pthread_join(hiloInnotify, NULL);
	//Rutina de cierre
	log_destroy(logger);
	close(socket_cli);
	close(socket_sv);
	list_destroy_and_destroy_elements(memtable, (void*)liberarRegistro);
	free(configLFS->dirMontaje);
	free(configLFS->puerto);
	free(configLFS);
	free(configMetadata->magicNumber);
	free(configMetadata);
	dictionary_destroy(bloqueoTablas);
	return 0;
}

void levantarMetadataFS() {
	t_config* aux = malloc(sizeof(t_config));
	aux = config_create("../../FS_LISSANDRA/Metadata/Metadata.bin");
	configMetadata->blockSize = config_get_int_value(aux, "BLOCK_SIZE");
	configMetadata->blocks = config_get_int_value(aux, "BLOCKS");
	configMetadata->magicNumber = malloc(strlen(config_get_string_value(aux, "MAGIC_NUMBER"))+1);
	strcpy(configMetadata->magicNumber,config_get_string_value(aux, "MAGIC_NUMBER"));
	config_destroy(aux);
//	log_debug(logger, "%d", configMetadata->blockSize);
//	log_debug(logger, "%d", configMetadata->blocks);
//	log_debug(logger, "%s", configMetadata->magicNumber);
}

void levantarConfigLFS() {
	t_config *aux = config_create("../LFS.config");
	configLFS->puerto = malloc(strlen(config_get_string_value(aux, "PUERTO_ESCUCHA"))+1);
	strcpy(configLFS->puerto, config_get_string_value(aux, "PUERTO_ESCUCHA"));
	configLFS->dirMontaje = malloc(strlen(config_get_string_value(aux, "PUNTO_MONTAJE"))+1);
	strcpy(configLFS->dirMontaje, config_get_string_value(aux, "PUNTO_MONTAJE"));
	configLFS->retardo = config_get_int_value(aux, "RETARDO");
	configLFS->tamanioValue = config_get_int_value(aux, "TAMANIO_VALUE");
	configLFS->tiempoDumpeo = config_get_int_value(aux, "TIEMPO_DUMP");
	config_destroy(aux);
//	log_debug(logger, "%s", configLFS->puerto);
//	log_debug(logger, "%s", configLFS->dirMontaje);
//	log_debug(logger, "%d", configLFS->retardo);
//	log_debug(logger, "%d", configLFS->tamanioValue);
//	log_debug(logger, "%d", configLFS->tiempoDumpeo);
}

void hiloDump() {
	while (1) {
		usleep(configLFS->tiempoDumpeo * 1000);
		if (!list_is_empty(memtable))
			dumpeoMemoria();
	}
}

void receptorDeSockets(int* socket) {
	type header;
	int errorHandler = 0;
	while (1) {
		errorHandler = todoJoya;
		int bytes = recv(*socket, &header, sizeof(type), MSG_WAITALL);
		usleep((configLFS->retardo) * 1000);
		if (0 >= bytes) {
			close(*socket);
			log_debug(logger, "Cerre el socket: %d", *socket);
			return;
		}

		switch (header) {
		case SELECT:
			log_debug(logger, "Comando SELECT recibido por socket: %d", *socket);
			tSelect* packSelect = malloc(sizeof(tSelect));
			if (!packSelect){
				errorHandler = errorDeMalloc;
				logeoDeErroresLFS(errorHandler, logger);
			}
			else {
				desSerializarSelect(packSelect, *socket);
				//TODO: handlear error de desSerializarSelect
//				log_debug(logger, "TABLA %s",packSelect->nombre_tabla);
				packSelect->type = header;
				registro* reg = malloc(sizeof(registro));
				if (!reg)
					errorHandler = errorDeMalloc;
				else {
					//TODO: handlear error de Y hacer failfast
					errorHandler = Select(reg, packSelect->nombre_tabla, packSelect->key);
					tRegistroRespuesta* registro = malloc(sizeof(tRegistroRespuesta));
					if (!errorHandler) {
						if (!registro)
							errorHandler = errorDeMalloc;
						else {
//							log_debug(logger, "ENTRO AL SEGUNDO IF");
							registro->tipo = REGISTRO;
//							log_debug(logger, "Tipo: %d", registro->tipo);
							registro->timestamp = reg->timestamp;
//							log_debug(logger, "Timestamp: %llu", registro->timestamp);
							registro->value = malloc(strlen(reg->value)+1);
							strcpy(registro->value, reg->value);
//							log_debug(logger, "Value: %s", registro->value);
							registro->key = reg->key;
//							log_debug(logger, "Key: %d", registro->key);
							registro->value_long = strlen(reg->value) + 1;
//							log_debug(logger, "Value_long: %d", registro->value_long);
							char* registroSerializado = serializarRegistro(registro);
							//TODO: handlear error de serializarRegistro
							enviarPaquete(*socket, registroSerializado, registro->length);
							//TODO: handlear error de enviarPaquete
							free(registroSerializado);
						}
						free(registro);
					} else if (errorHandler == noExisteKey || errorHandler == noExisteTabla) {
						registro->tipo = REGISTRO;
						registro->timestamp = 0;
						registro->value = "";
						registro->key = 0;
						registro->value_long = 1;
						char* registroSerializado = serializarRegistro(
								registro);
						//TODO: handlear error de serializarRegistro
						enviarPaquete(*socket, registroSerializado,
								registro->length);
						//TODO: handlear error de enviarPaquete
						free(registroSerializado);
					}
				}
				free(reg);
			}
			free(packSelect);
			break;

		case INSERT:
			log_debug(logger, "Comando INSERT recibido por socket: %d", *socket);
			tInsert* packInsert = malloc(sizeof(tInsert));
			if (!packInsert)
				errorHandler = errorDeMalloc;
			else {
				desSerializarInsert(packInsert, *socket);
				//TODO: handlear error de desSerializarInsert
				packInsert->type = header;
				log_debug(logger, "Tabla %s con key %d y value %s y time %llu\n",
						packInsert->nombre_tabla, packInsert->key,
						packInsert->value, packInsert->timestamp);
				errorHandler = insertarEnMemtable(packInsert);
			}
			free(packInsert->nombre_tabla);
			free(packInsert->value);
			free(packInsert);
			break;

		case CREATE:
			log_debug(logger, "Comando CREATE recibido por socket: %d", *socket);
			tCreate* packCreate = malloc(sizeof(tCreate));
			if (!packCreate) {
				errorHandler = errorDeMalloc;
				break;
			}
			desSerializarCreate(packCreate, *socket);
			//TODO: handlear error de desSerializarCreate
			packCreate->type = header;
			errorHandler = Create(packCreate->nombre_tabla,
					packCreate->consistencia, packCreate->particiones,
					packCreate->compaction_time);
			free(packCreate);
			break;

		case ADD:
			break;

		case DESCRIBE:
			log_debug(logger, "Comando DESCRIBE recibido por socket: %d", *socket);
			tDescribe* packDescribe = malloc(sizeof(tDescribe));
			t_metadata* metadata = malloc(sizeof(t_metadata));
			if (!packDescribe || !metadata)
				logeoDeErroresLFS(errorDeMalloc, logger);
			t_list* metadatas;
			desSerializarDescribe(packDescribe, *socket);
			if (!strcmp(packDescribe->nombre_tabla, ""))
				metadatas = Describe();
			else
				metadatas = DESCRIBEespecifico(packDescribe->nombre_tabla);
			if (metadatas != NULL) {
				t_describe* describe = malloc(sizeof(t_describe));
				if (!describe)
					logeoDeErroresLFS(errorDeMalloc, logger);
				int cantidad_de_tablas = metadatas->elements_count;
				describe->cant_tablas = cantidad_de_tablas;
				describe->tablas = malloc(
						cantidad_de_tablas * sizeof(t_metadata));
				for (int i = 0; i < cantidad_de_tablas; i++) {
					Metadata* a_metadata = (Metadata*) list_get(metadatas, i);
					t_metadata meta;
					meta.consistencia = a_metadata->consistency;
					strcpy(meta.nombre_tabla, a_metadata->nombre_tabla);
					describe->tablas[i] = meta;
				}
				char* serializedPackage;
				serializedPackage = serializarDescribe_Response(describe);
				char* cantidad_de_tablas_string = string_itoa(
						cantidad_de_tablas);
				free(cantidad_de_tablas_string);
				int b = send(*socket, serializedPackage, cantidad_de_tablas * sizeof(t_metadata) + sizeof(describe->cant_tablas), 0);
//				log_debug(logger, "envie %d bytes", b);
				dispose_package(&serializedPackage);
				free(describe->tablas);
				free(describe);
				for (int i = 0; i < metadatas->elements_count; i++) {
					free(list_get(metadatas, i));
				}
				list_destroy(metadatas);
			} else {
				t_describe* describe = malloc(sizeof(t_describe));
				if (!describe)
					logeoDeErroresLFS(errorDeMalloc, logger);
				describe->tablas = malloc(sizeof(t_metadata));
				if (!describe->tablas)
					logeoDeErroresLFS(errorDeMalloc, logger);
				describe->cant_tablas = 0;
				char* serializedPackage;
				serializedPackage = serializarDescribe_Response(describe);
				send(*socket, serializedPackage,
						sizeof(describe->cant_tablas), 0);
				dispose_package(&serializedPackage);
				//list_destroy(metadatas);
			}
			free(packDescribe);
			free(metadata);
			break;

		case DROP:
		 log_debug(logger, "Comando DROP recibido por socket: %d", *socket);
		 tDrop* packDrop = malloc(sizeof(tDrop));
		 if (!packDrop) {
			 errorHandler = errorDeMalloc;
			 break;
		 }
		 errorHandler = desSerializarDrop(packDrop, socket_cli);
		 if (errorHandler)
			 packDrop->type = header;
		 errorHandler = Drop(packDrop->nombre_tabla);
		 free(packDrop);
		 break;

		default:
			log_debug(logger, "Comando no reconocido");
			break;
		}
		logeoDeErroresLFS(errorHandler, logger);
	}
}

//-----------------HILOS-------------------
void abrirHiloConsola(void* params) {
	while (1) {
		tHiloConsola* parametros = (tHiloConsola*) params;
		fgets(parametros->consulta, 256, stdin);
		char** tempSplit;
		tempSplit = string_n_split(parametros->consulta, 2, " ");
		int errorHandler = 0;
		usleep((configLFS->retardo) * 1000);
		type header = stringToHeader(tempSplit[0]);
		switch (header) {
		//TODO: handlear en cada case que se ingresen mal las cosas. En el inicio de cada uno splitear tempSplit[1] y ver con string_spĺit que tenga la cantidad de campos necesarios
		// y que el siguiente se NULL. Ejemplo: en SELECT verificar que tempSplit[1] se divida en dos campos diferentes a NULL y un tercero que sea NULL.

		case SELECT:
			log_debug(logger, "Recibi un SELECT por consola");
			tSelect* packSelectConsola;
			registro* reg = malloc(sizeof(registro));
			if (!reg)
				logeoDeErroresLFS(errorDeMalloc, logger);
			packSelectConsola = malloc(sizeof(tSelect));
			cargarPaqueteSelect(packSelectConsola, paramsConsola->consulta);
			errorHandler = Select(reg, packSelectConsola->nombre_tabla, packSelectConsola->key);
			free(reg);
			free(packSelectConsola->nombre_tabla);
			free(packSelectConsola);
			break;

		case INSERT:
			log_debug(logger, "Recibi un INSERT por consola");
			tInsert* packInsertConsola = malloc(sizeof(tInsert));
			if (!packInsertConsola)
				logeoDeErroresLFS(errorDeMalloc, logger);
			cargarPaqueteInsertLFS(packInsertConsola, paramsConsola->consulta);
//			log_debug(logger, "%llu", packInsertConsola->timestamp);
			errorHandler = insertarEnMemtable(packInsertConsola);
			free(packInsertConsola->value);
			free(packInsertConsola->nombre_tabla);
			free(packInsertConsola);
			break;

		case CREATE:
			log_debug(logger, "Recibi un CREATE por consola");
			tCreate* packCreateConsola = malloc(sizeof(tCreate));
			if (!packCreateConsola)
				errorHandler = errorDeMalloc;
			cargarPaqueteCreate(packCreateConsola, paramsConsola->consulta);

			errorHandler = Create(packCreateConsola->nombre_tabla,
					packCreateConsola->consistencia,
					packCreateConsola->particiones,
					packCreateConsola->compaction_time);

			free(packCreateConsola->nombre_tabla);
			free(packCreateConsola->consistencia);
			free(packCreateConsola);
			break;

		case DROP:
			log_debug(logger, "Recibi un DROP por consola");
			tDrop* packDropConsola = malloc(sizeof(tDrop));
			cargarPaqueteDrop(packDropConsola, paramsConsola->consulta);
			packDropConsola->type = DROP;
			if (!packDropConsola) {
				errorHandler = errorDeMalloc;
				break;
			}
			if (errorHandler)
				packDropConsola->type = header;
			errorHandler = Drop(packDropConsola->nombre_tabla);
			free(packDropConsola);
			break;

		case DESCRIBE:
			log_debug(logger, "Recibi un DESCRIBE por consola");
			t_list* lista;
			tDescribe* packDescribeConsola = malloc(sizeof(tDescribe));
			cargarPaqueteDescribe(packDescribeConsola, string_substring_until(paramsConsola->consulta, strlen(paramsConsola->consulta) - 1));
			if (!tempSplit[1])
				lista = Describe();
			else
				lista = DESCRIBEespecifico(packDescribeConsola->nombre_tabla);
//			if (lista)
//				for (int i = 0; i < lista->elements_count; i++) {
//					Metadata* m = list_get(lista, i);
//				}
			free(packDescribeConsola->nombre_tabla);
			free(packDescribeConsola);
			break;

		case NIL:
			errorHandler = comandoMalIngresado;
			break;
		}

		logeoDeErroresLFS(errorHandler, logger);
		free(tempSplit[0]);
		free(tempSplit[1]);
		free(tempSplit);
	}
}

void abrirHiloSockets() {
//	int errorHandler;
	pthread_t threadPrincipal;
	socket_sv = levantarServidor(configLFS->puerto);
	if (socket_sv < 0)
		logeoDeErroresLFS(noLevantoServidor, logger);
	socket_cli = aceptarCliente(socket_sv);
	log_debug(logger, "levante un cliente con socket: %d", socket_cli);
	send(socket_cli, &configLFS->tamanioValue, 4, 0);
	pthread_create(&threadPrincipal, NULL, (void*) receptorDeSockets,
			&socket_cli);
	while (1) {
		int *socketNuevo = malloc(sizeof(int));
		pthread_t threadNuevo;
		*socketNuevo = aceptarCliente(socket_sv);
		send(*socketNuevo, &configLFS->tamanioValue, 4, 0);
		pthread_create(&threadNuevo, NULL, (void*) receptorDeSockets, socketNuevo);
		log_debug(logger, "levante otro cliente con socket: %d", *socketNuevo);
	}
}

type stringToHeader(char* str) {
	if (!strcmp(str, "SELECT"))
		return SELECT;
	if (!strcmp(str, "INSERT"))
		return INSERT;
	if (!strcmp(str, "CREATE"))
		return CREATE;
	if (!strcmp(str, "DROP"))
		return DROP;
	if (!strcmp(str, "DESCRIBE"))
		return DESCRIBE;
	if (!strcmp(str, "DESCRIBE\n"))
		return DESCRIBE;
	return NIL;
}

void abrirHiloCompactacion() {
	DIR *tables_directory;
	struct dirent *a_directory;
	char* tablas_path = malloc(strlen(configLFS->dirMontaje)+strlen("Tablas/")+1);
	strcpy(tablas_path, configLFS->dirMontaje);
	strcat(tablas_path, "Tablas/");
	tables_directory = opendir(tablas_path);
	if (tables_directory) {
		while ((a_directory = readdir(tables_directory)) != NULL) {
			if (strcmp(a_directory->d_name, ".") && strcmp(a_directory->d_name, "..")) {
				char* table_name = malloc(strlen(a_directory->d_name)+1);
				memcpy(table_name, a_directory->d_name, strlen(a_directory->d_name) + 1);
				pthread_t threadComp;
				pthread_create(&threadComp, NULL, (void*) hiloCompactar, (void*) table_name);
				//free(table_name);
			}
		}
		closedir(tables_directory);
	}
	free(tablas_path);
}

void hiloCompactar(void* nombre_tabla) {

	pthread_mutex_t mutexTabla;
	dictionary_put(bloqueoTablas, nombre_tabla, &mutexTabla);
	pthread_mutex_init(&mutexTabla, NULL);
	while (1) {
		if (!verificadorDeTabla((char*) nombre_tabla)) {
			pthread_exit(NULL);
		}
		char* tablas_path = malloc(strlen(configLFS->dirMontaje)+strlen("Tablas/")+strlen(nombre_tabla)+strlen("/metadata")+1);
		strcpy(tablas_path, configLFS->dirMontaje);
		strcat(tablas_path, "Tablas/");
		strcat(tablas_path, (char*) nombre_tabla);
		strcat(tablas_path, "/metadata");
		t_config* configComp = malloc(sizeof(t_config));
		configComp = config_create(tablas_path);
		free(tablas_path);
		int retardoCompac = config_get_int_value(configComp, "COMPACTION_TIME");
		config_destroy(configComp);
		usleep(retardoCompac * 1000);
		if (!verificadorDeTabla((char*) nombre_tabla)) {
			pthread_exit(NULL);
		}



		int dumps = contadorDeArchivos(nombre_tabla, "tmp");
		if (dumps > 0) {
			compactacion((char*) nombre_tabla);
//			log_debug(logger,"termine");
		}
	}
}
// ---------------MEMTABLE-----------------

t_list* inicializarMemtable() {
	return list_create();
}

int insertarEnMemtable(tInsert *packinsert) {
	int errorHandler;
	errorHandler = verificadorDeTabla(packinsert->nombre_tabla);
// ChequeO que la tabla este creada
	if (!errorHandler)
		return noExisteTabla;
// Chequeo si existe la tabla en la memtable
	errorHandler = existe_tabla_en_memtable(packinsert->nombre_tabla);
	if (!errorHandler) {
		pthread_mutex_lock(&mem_table_mutex);
		errorHandler = agregar_tabla_a_memtable(packinsert->nombre_tabla);
		pthread_mutex_unlock(&mem_table_mutex);
		// Si no existe la agrego
		if (!errorHandler)
			return noSeAgregoTabla;
	}
	registro* registro_insert = malloc(sizeof(registro));
	if (!registro_insert)
		return errorDeMalloc;
	registro_insert->key = packinsert->key;
//	log_debug(logger, string_itoa(registro_insert->key));
	registro_insert->timestamp = packinsert->timestamp;
//	log_debug(logger, (packinsert->value));
	char* value = malloc(packinsert->value_long);
	if (!value) {
		free(value);
		return errorDeMalloc;
	}
	strcpy(value, packinsert->value);
	registro_insert->value = malloc(strlen(value) + 1);
	if (!registro_insert->value) {
		free(registro_insert->value);
		free(value);
		return errorDeMalloc;
	}
	strcpy(registro_insert->value, value);
	errorHandler = insertarRegistro(registro_insert, packinsert->nombre_tabla);
	free(value);
	return errorHandler;
}

int insertarRegistro(registro* registro, char* nombre_tabla) {
	bool es_tabla(t_tabla* tabla) {
		if (strcmp(tabla->nombreTabla, nombre_tabla) == 0)
			return true;
		return false;
	}
	pthread_mutex_lock(&mem_table_mutex);
	t_tabla* tabla = (t_tabla*) list_find(memtable, (void*) &es_tabla);
	pthread_mutex_unlock(&mem_table_mutex);
	list_add(tabla->registros, registro);
	return todoJoya;
}

int agregar_tabla_a_memtable(char* tabla) {
	t_tabla* tabla_nueva = malloc(sizeof(t_tabla));
	if (!tabla_nueva) {
		logeoDeErroresLFS(errorDeMalloc, logger);
		return 0;
	}
	int cantidad_anterior;
	int indice_agregado;
	tabla_nueva->nombreTabla = malloc(strlen(tabla)+1);
	strcpy(tabla_nueva->nombreTabla, tabla);
	tabla_nueva->registros = list_create();

	cantidad_anterior = memtable->elements_count;
	indice_agregado = list_add(memtable, tabla_nueva);

	return indice_agregado + 1 > cantidad_anterior;
}

int existe_tabla_en_memtable(char* posible_tabla) {
	bool es_tabla(t_tabla* UnaTabla) {
		if (strcmp(UnaTabla->nombreTabla, posible_tabla) == 0)
			return true;
		return false;
	}
	pthread_mutex_lock(&mem_table_mutex);
	t_tabla* tabla_encontrada = (t_tabla*) list_find(memtable,
			(void*) &es_tabla);
	pthread_mutex_unlock(&mem_table_mutex);
	if (tabla_encontrada)
		return 1;
	return 0;
}

// ---------------APIs-----------------

int Create(char* NOMBRE_TABLA, char* TIPO_CONSISTENCIA, int NUMERO_PARTICIONES, int COMPACTATION_TIME) {

// ---- Verifico que la tabla no exista ----
	int errorHandler = verificadorDeTabla(NOMBRE_TABLA);
	if (errorHandler)
		return tablaExistente;
	char* ruta = direccionarTabla(NOMBRE_TABLA);

// ---- Creo directorio de la tabla ----
	errorHandler = mkdir(ruta, 0700);
	if (errorHandler < 0) {
		free(ruta);
		return carpetaNoCreada;
	}
// ---- Creo y grabo Metadata de la tabla ----
	errorHandler = crearMetadata(ruta, TIPO_CONSISTENCIA, NUMERO_PARTICIONES, COMPACTATION_TIME);
	if (errorHandler) {
		free(ruta);
		return metadataNoCreada;
	}
// ---- Creo los archivos binarios ----
	crearBinarios(ruta, NUMERO_PARTICIONES);
	free(ruta);
	pthread_t threadComp;
	char* table_name = malloc(strlen(NOMBRE_TABLA)+1);
	memcpy(table_name, NOMBRE_TABLA, strlen(NOMBRE_TABLA) + 1);

	pthread_create(&threadComp, NULL, (void*) hiloCompactar,
			(void*) table_name);
	return todoJoya;
}

/* TODO: ver si esto lo borramos o lo usamos para mantener orden y formato uniformes
 int Insert(char* NOMBRE_TABLA, int KEY, char* VALUE, int Timestamp) {
 int particiones;
 // ---- Verifico que la tabla exista ----
 if (verificadorDeTabla(NOMBRE_TABLA) != 0)
 return noExisteTabla;

 // ---- Obtengo la metadata ----
 particiones = buscarEnMetadata(NOMBRE_TABLA, "PARTITIONS");
 if (particiones < 0)
 return particiones;

 // ---- Verifico si existe memoria para dumpeo ----

 // ---- Manejo de Timestamp opcional ----

 // ---- Inserto datos en memoria temporal ----

 return todoJoya;
 }
 */

int Drop(char* NOMBRE_TABLA) {
// ---- Verifico que la tabla exista ----
	int errorHandler = verificadorDeTabla(NOMBRE_TABLA);
	if (!errorHandler) {
		return noExisteTabla;
	}

	bool es_tabla(t_tabla* t) {
		if (!strcmp(t->nombreTabla, NOMBRE_TABLA)) {
			return true;
		}
		return false;
	}

	// Eliminar de la memtable
	pthread_mutex_lock(&mem_table_mutex);
	list_remove_by_condition(memtable, (void*)&es_tabla);
	pthread_mutex_unlock(&mem_table_mutex);
	char* ruta = direccionarTabla(NOMBRE_TABLA);

	char* rutametadata = malloc(strlen(ruta)+strlen("/metadata")+1);
	strcpy(rutametadata, ruta);
	strcat(rutametadata, "/metadata");
	t_config* metadataTabla = malloc(sizeof(t_config));
	metadataTabla = config_create(rutametadata);
	int cantParticiones = (config_get_int_value(metadataTabla, "PARTITIONS") - 1);
	config_destroy(metadataTabla);
	free(rutametadata);
	t_bitarray* bitmap = levantarBitmap();
	for (int i = 0; i <= cantParticiones; i++) {
		char* auxStr = malloc(strlen(string_itoa(i))+1);
		auxStr = string_itoa(i);
		char* binario_a_limpiar = malloc(strlen(ruta)+strlen("/")+strlen(auxStr)+strlen(".bin")+1);
		strcpy(binario_a_limpiar, ruta);
		strcat(binario_a_limpiar, "/");
		strcat(binario_a_limpiar, auxStr);
		strcat(binario_a_limpiar, ".bin");
		t_config* binario = malloc(sizeof(t_config));
		binario = config_create(binario_a_limpiar);
		char** bloques_a_limpiar = config_get_array_value(binario, "BLOCKS");
		int i = 0;
		while (bloques_a_limpiar[i] != NULL) {
			bitarray_clean_bit(bitmap, atoi(bloques_a_limpiar[i]));

			char* bloque_a_limpiar = malloc(strlen("rm ")+strlen(configLFS->dirMontaje)+strlen("Bloques/")+strlen(bloques_a_limpiar[i])+strlen(".bin")+1);
			strcpy(bloque_a_limpiar, "rm ");
			strcat(bloque_a_limpiar, configLFS->dirMontaje);
			strcat(bloque_a_limpiar, "Bloques/");
			strcat(bloque_a_limpiar, bloques_a_limpiar[i]);
			strcat(bloque_a_limpiar, ".bin");
			system(bloque_a_limpiar);
			free(bloque_a_limpiar);

			i++;
			//TODO: ELIMINAR EL BLOQUE PROPIAMENTE DICHO
		}
		string_iterate_lines(bloques_a_limpiar, (void*) free);
		free(bloques_a_limpiar);
		free(auxStr);
		free(binario_a_limpiar);
		config_destroy(binario);
	}
	int dump=contadorDeArchivos(NOMBRE_TABLA, "tmp");
	if (dump >= 1) {
		for (int j = 1; j <= dump; j++) {
			char* auxStr2 = malloc(strlen(string_itoa(j - 1))+1);
			auxStr2 = string_itoa(j - 1);
			char* temporal_a_limpiar = malloc(strlen(ruta)+strlen("/")+strlen(auxStr2)+strlen(".tmp")+1);
			strcpy(temporal_a_limpiar, ruta);
			strcat(temporal_a_limpiar, "/");
			strcat(temporal_a_limpiar, auxStr2);
			strcat(temporal_a_limpiar, ".tmp");
			t_config* temporal = malloc(sizeof(t_config));
			temporal = config_create(temporal_a_limpiar);
			char** bloques_a_limpiar = config_get_array_value(temporal, "BLOCKS");
			int i = 0;
			while (bloques_a_limpiar[i] != NULL) {
				bitarray_clean_bit(bitmap, atoi(bloques_a_limpiar[i]));
				char* bloque_a_limpiar = malloc(strlen("rm ")+strlen(configLFS->dirMontaje)+strlen("Bloques/")+strlen(bloques_a_limpiar[i])+strlen(".bin")+1);
				strcpy(bloque_a_limpiar, "rm ");
				strcat(bloque_a_limpiar, configLFS->dirMontaje);
				strcat(bloque_a_limpiar, "Bloques/");
				strcat(bloque_a_limpiar, bloques_a_limpiar[i]);
				strcat(bloque_a_limpiar, ".bin");
				system(bloque_a_limpiar);
				free(bloque_a_limpiar);
				i++;
			}
			string_iterate_lines(bloques_a_limpiar, (void*) free);
			free(bloques_a_limpiar);
			free(auxStr2);
			free(temporal_a_limpiar);
			config_destroy(temporal);
		}
	}
	free(bitmap);
	char* comando_a_ejecutar = malloc(strlen("rm -rf ")+strlen(ruta)+1);
	errorHandler = system(comando_a_ejecutar);
	if (!errorHandler)
		return errorAlBorrarDirectorio;
	free(comando_a_ejecutar);
	free(ruta);
	return todoJoya;
}

int Select(registro* reg, char* NOMBRE_TABLA, uint16_t KEY) {

	bool es_mayor(registro* unReg, registro* otroReg) {
		if (unReg->timestamp >= otroReg->timestamp)
			return true;
		return false;
	}
	char* ruta = direccionarTabla(NOMBRE_TABLA);
	int errorHandler = verificadorDeTabla(ruta);
	log_debug(logger, "Se ha solicitado la key %d de la tabla %s.", KEY, NOMBRE_TABLA);
	if (!errorHandler)
		return noExisteTabla;
	t_list* registros = list_create();

//	 A MEMTABLE
//	log_debug(logger, "VOY A MEMTABLE");
	t_list* memtable = selectEnMemtable(KEY, NOMBRE_TABLA);

	pthread_mutex_lock(&mem_table_mutex);
	if (memtable->elements_count > 0){
		list_add_all(registros, memtable);
//		log_debug(logger, "Encontre elementos en memtable. elemnts_counts ahora es: %d", registros->elements_count);
	}
	pthread_mutex_unlock(&mem_table_mutex);

	registro* registry = malloc(sizeof(registro));
	if (!registry)
		return errorDeMalloc;

	pthread_mutex_t* mutex = dictionary_get(bloqueoTablas,NOMBRE_TABLA);
	if (mutex != NULL)
		pthread_mutex_lock(mutex);
	//	VOY A FS
//	log_debug(logger, "VOY A FS");
	errorHandler = SelectFS(ruta, KEY, registry);
	if (!errorHandler){
		list_add(registros, registry);
//		log_debug(logger, "Encontre elementos en FS. elemnts_counts ahora es: %d", registros->elements_count);
	} else
		free(registry);

	//	VOY A TEMP
//	log_debug(logger, "VOY A TEMP");
	t_list* temporales = SelectTemp(ruta, KEY,NOMBRE_TABLA);

	//	VOY A TEMPC
//	log_debug(logger, "VOY A TEMPC");
	t_list* temporalesC = SelectTempc(ruta, KEY,NOMBRE_TABLA);
	if(mutex!=NULL)
		pthread_mutex_unlock(mutex);

	//signal
	if (temporales->elements_count > 0){
		list_add_all(registros, temporales);
//		log_debug(logger, "Encontre elementos en temporales. elemnts_counts ahora es: %d", registros->elements_count);
	} else
		list_destroy(temporales);
	if (temporalesC->elements_count > 0){
		list_add_all(registros, temporalesC);
//		log_debug(logger, "Encontre elementos en temporalesC. elemnts_counts ahora es: %d", registros->elements_count);
	} else
		list_destroy(temporalesC);
	free(ruta);
//	log_debug(logger, "Termine de buscar el SELECT, tomo decision");
	if (registros->elements_count > 0) {
		list_sort(registros, (void*) &es_mayor);
		*reg = *(registro*) list_get(registros, 0);
		log_debug(logger, "El registro encontrado es: %llu;%d;%s", reg->timestamp, reg->key, reg->value);
		errorHandler = todoJoya;
	} else
		errorHandler = noExisteKey;
	return errorHandler;
}

t_list* DESCRIBEespecifico(char* NOMBRE_TABLA) {
	char* ruta = malloc(strlen(configLFS->dirMontaje) + strlen("Tablas/") + strlen(NOMBRE_TABLA) + 1);
	log_debug(logger, "Ha solicitado un DESCRIBE de la tabla: %s",NOMBRE_TABLA);
	strcpy(ruta, configLFS->dirMontaje);
	strcat(ruta, "Tablas/");
	strcat(ruta, NOMBRE_TABLA);
	t_list* metadatas = list_create();
	if (verificadorDeTabla(NOMBRE_TABLA)){
		Metadata* metadata = obtener_metadata(ruta);
		strcpy(metadata->nombre_tabla, NOMBRE_TABLA);
		list_add(metadatas, metadata);
	} else {
		logeoDeErroresLFS(noExisteTabla, logger);
		metadatas = NULL;
	}
	free(ruta);
	return metadatas;
}

t_list* Describe() {
	t_list* metadatas = list_create();
	DIR *tables_directory;
	struct dirent *a_directory;
	char* tablas_path = malloc(strlen(configLFS->dirMontaje)+strlen("Tablas/")+1);
	strcpy(tablas_path, configLFS->dirMontaje);
	strcat(tablas_path, "Tablas/");
	tables_directory = opendir(tablas_path);
	if (tables_directory) {
		while ((a_directory = readdir(tables_directory)) != NULL) {
			if (strcmp(a_directory->d_name, ".") && strcmp(a_directory->d_name, "..")) {
				char* table_name = malloc(strlen(a_directory->d_name)+1);
				memcpy(table_name, a_directory->d_name, strlen(a_directory->d_name) + 1);
				char* a_table_path = malloc(strlen(configLFS->dirMontaje)+strlen("Tablas/")+strlen(a_directory->d_name)+1);
				strcpy(a_table_path, tablas_path);
				strcat(a_table_path, table_name);
				Metadata* metadata = obtener_metadata(a_table_path);
				strcpy(metadata->nombre_tabla, table_name);
				log_debug(logger, "DESCRIBE general, tabla: %s", table_name);
				list_add(metadatas, metadata);
				free(table_name);
				free(a_table_path);
			}
		}
		if (!metadatas->elements_count)
			metadatas = NULL;
		closedir(tables_directory);
	} else {
		metadatas = NULL;
	}
	free(tablas_path);
	return metadatas;
}

Metadata* obtener_metadata(char* ruta) {
	Metadata* metadata = NULL;
	char* mi_ruta = malloc(strlen(ruta)+strlen("/metadata")+1);
	strcpy(mi_ruta, ruta);
	strcat(mi_ruta, "/metadata");
	if(verificadorDeArchivo(mi_ruta)){
		t_config* config_metadata = malloc(sizeof(t_config));
		config_metadata = config_create(mi_ruta);
		metadata = malloc(sizeof(Metadata));
		long tiempoDeCompactacion = config_get_long_value(config_metadata, "COMPACTION_TIME");
		metadata->tiempo_compactacion = tiempoDeCompactacion;
		char* consistencia = config_get_string_value(config_metadata, "CONSISTENCY");
		metadata->consistency = consistency_to_int(consistencia);
		config_destroy(config_metadata);
	}
	free(mi_ruta);
	return metadata;
}

int consistency_to_int(char* cons) {
	if (!strcmp(cons, "SC"))
		return 0;
	else if (!strcmp(cons, "SHC"))
		return 1;
	else if (!strcmp(cons, "EC"))
		return 2;
	return errorDeConsistencia;
}

// ---------------AUXILIARES DE SELECT-----------------
t_list* selectEnMemtable(uint16_t key, char* tabla) {
	bool es_tabla(t_tabla* UnaTabla) {
		if (strcmp(UnaTabla->nombreTabla, tabla) == 0)
			return true;
		return false;
	}
	bool es_registro(registro* unregistro) {
		if (unregistro->key == key)
			return true;
		return false;
	}
	t_tabla* tablaSelect;
	t_list* list_reg = list_create();
	pthread_mutex_lock(&mem_table_mutex);
	tablaSelect = list_find(memtable, (void*) &es_tabla);
	if (tablaSelect != NULL)
		list_add_all(list_reg,list_filter(tablaSelect->registros, (void*) &es_registro));
	pthread_mutex_unlock(&mem_table_mutex);
	return list_reg; //devuelvo valor del select
}

int SelectFS(char* ruta, uint16_t KEY, registro* reg) {
	int errorHandler = 1;
	int size = 0;
//	log_debug(logger, "Voy a buscar las particiones en: %s", ruta);
	int particiones = buscarEnMetadata(ruta, "PARTITIONS");
//	log_debug(logger, "las particiones de la tabla son: %d", particiones);
	if (particiones <= 0)
		return particionesInvalidas;
// ---- Calculo particion del KEY ----
	particiones = (KEY % particiones);
	char* auxStr = malloc(strlen(string_itoa(particiones))+1);
	auxStr = string_itoa(particiones);
	char* rutaFS = malloc(strlen(ruta)+strlen("/")+strlen(auxStr)+strlen(".bin")+1);
	strcpy(rutaFS, ruta);
	strcat(rutaFS, "/");
	strcat(rutaFS, auxStr);
	strcat(rutaFS, ".bin");
//wait
//	log_debug(logger, "la ruta del config es: %s", rutaFS);
	if (verificadorDeArchivo(rutaFS)){
		t_config* particion = malloc(sizeof(t_config));
		particion = config_create(rutaFS);
		size = config_get_int_value(particion, "SIZE");
		char** bloquesABuscar = config_get_array_value(particion, "BLOCKS");
		config_destroy(particion);
//		log_debug(logger, "Consegui el valor size: %d", size);
		if (size > 0) {
	//		log_debug(logger, "Size es mayor a 0, entre al if");
			int i = 0;
			char* bloquesUnificados = malloc(size+1);
			bloquesUnificados[0] = '\0';
			while (bloquesABuscar[i] != NULL) {
//				log_debug(logger, "bloque %s", bloquesABuscar[i]);
				char* bloque = malloc(strlen(configLFS->dirMontaje)+strlen("Bloques/")+strlen(bloquesABuscar[i])+strlen(".bin")+1);
				strcpy(bloque, configLFS->dirMontaje);
				strcat(bloque, "Bloques/");
				strcat(bloque, bloquesABuscar[i]);
				strcat(bloque, ".bin");
				int fd = open(bloque, O_RDONLY, S_IRUSR | S_IWUSR);
				struct stat s;
				fstat(fd, &s);
				size = s.st_size;
				if ((fd != -1) && (size > 0)){
//					log_debug(logger, "mapeo %d", size);
					char* f = malloc(configMetadata->blockSize + 1);
					f = mmap(NULL,size, PROT_READ, MAP_PRIVATE, fd, 0);
//					log_debug(logger, "acabo de mapear: %s", f);
					strcat(bloquesUnificados, f);
//					log_debug(logger, "cierro");
					munmap(f, size);
				}
				close(fd);
				free(bloque);
				i++;
			}
	//signal
			int j = 0;
//			log_debug(logger, "Sali del while 1 y encontre esto: %s", bloquesUnificados);
			char** registros = string_split(bloquesUnificados, "\n");
			while (registros[j] != NULL) {
//				log_debug(logger, "Entre con este valor: %s", registros[j]);
				char** datos_registro = string_split(registros[j], ";");
				if (atoi(datos_registro[1]) == KEY) {
//					log_debug(logger, "Entre al if del while 2");
					reg->timestamp = strtoull(datos_registro[0],NULL,10);
//					log_debug(logger, "El timestamp encontrado fue: %llu", reg->timestamp);
					reg->key = atoi(datos_registro[1]);
//					log_debug(logger, "La key encontrada fue: %d", reg->key);
					reg->value = malloc(strlen(datos_registro[2]) + 1);
//					log_debug(logger, "El value encontrado fue: %s", reg->value);
					if (!reg->value)
						return errorDeMalloc;
					strcpy(reg->value, datos_registro[2]);
					errorHandler = 0;
				}
				string_iterate_lines(datos_registro, (void*) free);
				free(datos_registro);
				j++;
			}
			string_iterate_lines(registros, (void*) free);
			free(registros);
			free(bloquesUnificados);
		}
		string_iterate_lines(bloquesABuscar, (void*) free);
		free(bloquesABuscar);
	}
	free(auxStr);
	free(rutaFS);
	return errorHandler;
}


t_list* SelectTemp(char* ruta, uint16_t KEY,char* nombre_tabla) {
	t_list* listRegistros = list_create();
//	Ejecuto lo siguiento por cada temporal creado (uno por dumpeo)
	int dump=contadorDeArchivos(nombre_tabla, "tmp");
	for (int aux = 1; aux <= dump; aux++) {
//		log_debug(logger, "entre al for");
		//wait
		char* auxStr = malloc(strlen(string_itoa(aux - 1))+1);
		auxStr = string_itoa(aux - 1);
		char* rutaTemporal = malloc(strlen(ruta)+strlen("/")+strlen(auxStr)+strlen(".tmp")+1);
		strcpy(rutaTemporal, ruta);
		strcat(rutaTemporal, "/");
		strcat(rutaTemporal, auxStr);
		strcat(rutaTemporal, ".tmp");
		t_config* particion = malloc(sizeof(t_config));
		particion = config_create(rutaTemporal);
		char** bloquesABuscar = config_get_array_value(particion, "BLOCKS");
		int size = config_get_int_value(particion, "SIZE");
		config_destroy(particion);
//		log_debug(logger, "El size es: %d y la tabla %s", size, rutaTemporal);
		if (size > 0) {
			int i = 0;
//		Unifico la informacion de todos los bloques en los que esta dividido el archivo .tmp
			char* bloquesUnificados = malloc(size+1);
			bloquesUnificados[0] = '\0';
			while (bloquesABuscar[i] != NULL) {
//				log_debug(logger, "entre al while 1 con bloque: %s", bloquesABuscar[i]);
				char* bloque = malloc(strlen(configLFS->dirMontaje)+strlen("Bloques/")+strlen(bloquesABuscar[i])+strlen(".bin")+1);
				strcpy(bloque, configLFS->dirMontaje);
				strcat(bloque, "Bloques/");
				strcat(bloque, bloquesABuscar[i]);
				strcat(bloque, ".bin");
				int fd = open(bloque, O_RDONLY, S_IRUSR | S_IWUSR);
				struct stat s;
				fstat(fd, &s);
				size = s.st_size;
				if ((fd != -1) && (size > 0)){
	//				log_debug(logger, "mapeo %d", size);
					char* f = malloc(configMetadata->blockSize + 1);
					f = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);
	//				log_debug(logger, "acabo de mapear: %s", f);
					strcat(bloquesUnificados, f);
	//				log_debug(logger, "cierro");
					munmap(f, size);
				}
				close(fd);
				free(bloque);
				i++;
			}
//signal
//			log_debug(logger, "sali del while 1 con bloquesUnificados de tamaño: %d", strlen(bloquesUnificados));
			int j = 0;
			char** registros = string_split(bloquesUnificados, "\n");
//		Recorro y divido los datos unificado del archivos temporal, almacenando solo las keys que coincidan con la solicitada
			while (registros[j] != NULL) {
//				log_debug(logger, "entre al while con: %s", registros[j]);
//				log_debug(logger, "entre al while 2");
				if(string_length(registros[j]) > 5){
					char** datos_registro = string_split(registros[j], ";");
					if (atoi(datos_registro[1]) == KEY) {
						registro* reg = malloc(sizeof(registro));
						reg->timestamp = strtoull(datos_registro[0], NULL, 10);
//						log_debug(logger, "En SelectTemp %s", datos_registro[0]);
//						log_debug(logger, "En SelectTemp el timestamp es: %llu", reg->timestamp);
						reg->key = atoi(datos_registro[1]);
//						log_debug(logger, "En SelectTemp la key es: %d", reg->key);
						reg->value = malloc(strlen(datos_registro[2]) + 1);
						strcpy(reg->value, datos_registro[2]);
//						log_debug(logger, "En SelectTemp el value es: %s", reg->value);
						list_add(listRegistros, reg);
					}
					string_iterate_lines(datos_registro, (void*) free);
					free(datos_registro);
				}
				j++;
			}
			string_iterate_lines(registros, (void*) free);
			free(registros);
			free(bloquesUnificados);
		}
		free(auxStr);
		free(rutaTemporal);
		string_iterate_lines(bloquesABuscar, (void*) free);
		free(bloquesABuscar);
	}
	return listRegistros;
}

t_list* SelectTempc(char* ruta, uint16_t KEY,char* nombre_tabla) {
	t_list* listRegistros = list_create();
	//	Ejecuto lo siguiento por cada temporal creado (uno por dumpeo)
		int dump=contadorDeArchivos(nombre_tabla, "tmpc");
		for (int aux = 1; aux <= dump; aux++) {
			//wait
			char* auxStr = malloc(strlen(string_itoa(aux - 1))+1);
			auxStr = string_itoa(aux - 1);
			char* rutaTemporal = malloc(strlen(ruta)+strlen("/")+strlen(auxStr)+strlen(".tmpc")+1);
			strcpy(rutaTemporal, ruta);
			strcat(rutaTemporal, "/");
			strcat(rutaTemporal, auxStr);
			strcat(rutaTemporal, ".tmpc");
			t_config* particion = malloc(sizeof(t_config));
			particion = config_create(rutaTemporal);
			int size = config_get_int_value(particion, "SIZE");
			char** bloquesABuscar = config_get_array_value(particion, "BLOCKS");
			config_destroy(particion);
			if (size > 0) {
				int i = 0;
	//		Unifico la informacion de todos los bloques en los que esta dividido el archivo .tmpc
				char* bloquesUnificados = malloc(size+1);
				bloquesUnificados[0] = '\0';
				while (bloquesABuscar[i] != NULL) {
					char* bloque = malloc(strlen(configLFS->dirMontaje)+strlen("Bloques/")+strlen(bloquesABuscar[i])+strlen(".bin")+1);
					strcpy(bloque, configLFS->dirMontaje);
					strcat(bloque, "Bloques/");
					strcat(bloque, bloquesABuscar[i]);
					strcat(bloque, ".bin");
					int fd = open(bloque, O_RDONLY, S_IRUSR | S_IWUSR);
					struct stat s;
					fstat(fd, &s);
					size = s.st_size;
					if ((fd != -1) && (size > 0)){
						char* f = malloc(configMetadata->blockSize + 1);
						f = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);
						strcat(bloquesUnificados, f);
						munmap(f, size);
					}
					close(fd);
					free(bloque);
					i++;
				}
	//signal
				int j = 0;
				char** registros = string_split(bloquesUnificados, "\n");
	//		Recorro y divido los datos unificado del archivos temporal, almacenando solo las keys que coincidan con la solicitada
				while (registros[j] != NULL) {
					registro* reg;
					reg= malloc(sizeof(registro));
					char** datos_registro = string_split(registros[j], ";");
					if (atoi(datos_registro[1]) == KEY) {
						reg->timestamp = strtoull(datos_registro[0],NULL,10);
//						log_debug(logger, "En SelectTempC %s", datos_registro[0]);
//						log_debug(logger, "En SelectTempC el timestamp es: %llu", reg->timestamp);
						reg->key = atoi(datos_registro[1]);
						reg->value = malloc(strlen(datos_registro[2]) + 1);
						strcpy(reg->value, datos_registro[2]);
						list_add(listRegistros, reg);
					} else
						free(reg);
					string_iterate_lines(datos_registro, (void*) free);
					free(datos_registro);
					j++;
				}
				string_iterate_lines(registros, (void*) free);
				free(registros);
				free(bloquesUnificados);
			}
			free(auxStr);
			free(rutaTemporal);
			string_iterate_lines(bloquesABuscar, (void*) free);
			free(bloquesABuscar);
		}
		return listRegistros;
}

// ---------------TABLAS-----------------

int crearMetadata(char* NOMBRE_TABLA, char* TIPO_CONSISTENCIA, int NUMERO_PARTICIONES, int COMPACTATION_TIME) {
	char* metadata = malloc(strlen(NOMBRE_TABLA)+strlen("/metadata")+1);
	strcpy(metadata, NOMBRE_TABLA);
	strcat(metadata, "/metadata");
	FILE *fp;
	fp = fopen(metadata, "w+");
	if (fp == NULL)
		return -1;
	fprintf(fp, "CONSISTENCY=%s\n", TIPO_CONSISTENCIA);
	fprintf(fp, "PARTITIONS=%i\n", NUMERO_PARTICIONES);
	fprintf(fp, "COMPACTION_TIME=%i", COMPACTATION_TIME);
	fclose(fp);
	return todoJoya;
}

void crearBinarios(char* NOMBRE_TABLA, int NUMERO_PARTICIONES) {
//Opitimizacion1B: Se puede hace que fp se pase por parámetro para no abrirlo en crearMetadata y crearBIN
	int aux=0;
	while (aux < NUMERO_PARTICIONES) {
		char* auxStr = malloc(strlen(string_itoa(aux))+1);
		auxStr = string_itoa(aux);
		char* nuevaParticion = malloc(strlen(NOMBRE_TABLA)+strlen("/")+strlen(auxStr)+strlen(".bin")+1);
		strcpy(nuevaParticion, NOMBRE_TABLA);
		strcat(nuevaParticion, "/");
		strcat(nuevaParticion, auxStr);
		strcat(nuevaParticion, ".bin");
		int fd = open(nuevaParticion, O_RDWR | O_CREAT | O_TRUNC, (mode_t) 0600);
		close(fd);
		t_config* particion = malloc(sizeof(t_config));
		particion = config_create(nuevaParticion);
		off_t bitLibre = obtener_bit_libre();
		char* bloques = malloc(strlen("[")+strlen(string_itoa(bitLibre))+strlen("]")+1);
		strcpy(bloques, "[");
		strcat(bloques, string_itoa(bitLibre));
		strcat(bloques, "]");
		config_set_value(particion, "BLOCKS", bloques);
		config_set_value(particion, "SIZE", "0");
		config_save(particion);
		config_destroy(particion);
		char* nuevoBloque = malloc(strlen(configLFS->dirMontaje)+strlen("Bloques/")+strlen(string_itoa(bitLibre))+strlen(".bin")+1);
		strcpy(nuevoBloque, configLFS->dirMontaje);
		strcat(nuevoBloque, "Bloques/");
		strcat(nuevoBloque, string_itoa(bitLibre));
		strcat(nuevoBloque, ".bin");
		int fd2 = open(nuevoBloque, O_RDWR | O_CREAT | O_TRUNC, (mode_t) 0600);;
		close(fd2);
		free(bloques);
		free(nuevoBloque);
		aux++;
		free(auxStr);
		free(nuevaParticion);
	}
}

int verificadorDeTabla(char* tabla) {
	char* ruta = direccionarTabla(tabla);
	int status = 1;
	DIR *dirp;
	dirp = opendir(ruta);
	if (dirp == NULL)
		status = 0;
	closedir(dirp);
	free(ruta);
	return status;
}

int verificadorDeArchivo(char* ruta) {
	int status = 1;
	int file;
	file = open(ruta, O_RDONLY, S_IRUSR | S_IWUSR);
	if (file == -1)
		status = 0;
	else {
		struct stat s;
		fstat(file, &s);
		if (s.st_size < 1)
			status = 0;
	}
	close(file);
	return status;
}

int contadorDeArchivos(char* tabla, char* ext) {
	int i = 0;
	while (1) {
		char* auxStr = malloc(strlen(string_itoa(i))+1);
		auxStr = string_itoa(i);
		char* ruta = malloc(strlen(configLFS->dirMontaje)+strlen("Tablas/")+strlen(tabla)+strlen("/")+strlen(auxStr)+strlen(".")+strlen(ext)+1);
		strcpy(ruta, configLFS->dirMontaje);
		strcat(ruta, "Tablas/");
		strcat(ruta, tabla);
		strcat(ruta, "/");
		strcat(ruta, auxStr);
		strcat(ruta, ".");
		strcat(ruta, ext);
		int file;
		file = open(ruta, O_RDONLY, S_IRUSR | S_IWUSR);
		if (file == -1)
			break;
		i++;
		close(file);
		free(auxStr);
		free(ruta);
	}
	return i;
}

int buscarEnMetadata(char* NOMBRE_TABLA, char* objetivo) {
	int particiones = 0;
	char* ruta = malloc(strlen(NOMBRE_TABLA)+strlen("/metadata")+1);
	strcpy(ruta, NOMBRE_TABLA);
	strcat(ruta, "/metadata");
//	log_debug(logger, "la ruta para buscar metadata es: %s", ruta);
	if(verificadorDeArchivo(ruta)){
		t_config* metadata = malloc(sizeof(t_config));
		metadata = config_create(ruta);
		particiones = config_get_int_value(metadata, objetivo);
		config_destroy(metadata);
	}
	free(ruta);
	return particiones;
}

t_bitarray* levantarBitmap() {
	pthread_mutex_lock(&bitmapMutex);
	char* ruta = malloc(strlen(configLFS->dirMontaje)+strlen("Metadata/Bitmap.bin")+1);
	strcpy(ruta, configLFS->dirMontaje);
	strcat(ruta, "Metadata/Bitmap.bin");
	int size = configMetadata->blocks / 8;
	if (configMetadata->blocks % 8 != 0)
		size++;
	int fd = open(ruta, O_RDWR, S_IRUSR | S_IWUSR);
	int errorHandler = ftruncate(fd, size);
	if ((errorHandler < 0) || (fd < 0))
		log_debug(logger, "Error al levantar bitmap");
	char* bitarray = malloc(size);
	bitarray = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	t_bitarray* structBitarray = bitarray_create_with_mode(bitarray, size, MSB_FIRST);
	close(fd);
	free(ruta);
	pthread_mutex_unlock(&bitmapMutex);
	return structBitarray;
}

off_t obtener_bit_libre() {
	t_bitarray* bitmap = levantarBitmap();
	off_t bit_index = 0;
//leer el primero libre y ponerle 1
	while (bitarray_test_bit(bitmap, bit_index)){
		bit_index++;
	}
	bitarray_set_bit(bitmap, bit_index);
	bitarray_destroy(bitmap);

	return bit_index;

}

// ---------------DUMPEO-----------------

int dumpeoMemoria() {
	void paraDumpearTabla(t_tabla* tabla) {
		int dump=contadorDeArchivos(tabla->nombreTabla, "tmp");
		char* auxStr = malloc(strlen(string_itoa(dump))+1);
		auxStr = string_itoa(dump);
		char* tablaParaDumpeo = malloc(strlen(configLFS->dirMontaje)+strlen("Tablas/")+strlen(tabla->nombreTabla)+strlen("/")+strlen(auxStr)+strlen(".tmp")+1);
		strcpy(tablaParaDumpeo, configLFS->dirMontaje);
		strcat(tablaParaDumpeo, "Tablas/");
		strcat(tablaParaDumpeo, tabla->nombreTabla);
		strcat(tablaParaDumpeo, "/");
		strcat(tablaParaDumpeo, auxStr);
		strcat(tablaParaDumpeo, ".tmp");
		pthread_mutex_t* mutex = dictionary_get(bloqueoTablas,tabla->nombreTabla);
		if(mutex!=NULL)
			pthread_mutex_lock(mutex);
		int fd = open(tablaParaDumpeo, O_RDWR | O_CREAT | O_TRUNC, (mode_t) 0600);;
		close(fd);
		if(!list_is_empty(tabla->registros))
			dumpearTabla(tabla->registros, tablaParaDumpeo);
		if(mutex!=NULL)
			pthread_mutex_unlock(mutex);
		free(auxStr);
		free(tablaParaDumpeo);
	}

	if (list_is_empty(memtable)) {
//		TODO: poner define para memtable vacia
		return 1;
	}
	pthread_mutex_lock(&mem_table_mutex);
	t_list* dumpMem = list_duplicate(memtable);
	memtable = list_create();
	pthread_mutex_unlock(&mem_table_mutex);

	list_iterate(dumpMem, (void*) &paraDumpearTabla);
//	log_debug(logger, "Sali de dumpear todo");
	list_clean_and_destroy_elements(dumpMem, (void*) free);
//	log_debug(logger,"termine");

	//pthread_mutex_lock(&cantidadDeDumpeos_mutex);
	//cantidadDeDumpeos++;
	//pthread_mutex_unlock(&cantidadDeDumpeos_mutex);
	return 0;
}

void dumpearTabla(t_list* registros, char* ruta) {
	char* registroParaEscribir = malloc(1);
	registroParaEscribir[0] = '\0';

	void dumpearRegistros(registro* UnRegistro) {
		char* auxstr = malloc(14);
		sprintf(auxstr, "%llu", (UnRegistro->timestamp));
		char* auxStr2 = malloc(strlen(string_itoa(UnRegistro->key))+1);
		auxStr2 = string_itoa(UnRegistro->key);
		registroParaEscribir = realloc(registroParaEscribir, (strlen(registroParaEscribir)+13+strlen(";")+strlen(auxStr2)+strlen(";")+strlen(UnRegistro->value)+strlen("\n")+1));
		strcat(registroParaEscribir, auxstr);
		strcat(registroParaEscribir, ";");
		strcat(registroParaEscribir, auxStr2);
		strcat(registroParaEscribir, ";");
		strcat(registroParaEscribir, UnRegistro->value);
		strcat(registroParaEscribir, "\n");
		free(auxStr2);
		free(auxstr);
	}
	list_iterate(registros, (void*) &dumpearRegistros);
	off_t bit_index = obtener_bit_libre();
	t_config* tmp = malloc(sizeof(t_config));
	tmp = config_create(ruta);
//	log_debug(logger, "Entre a dumpearTabla");
	char* auxStr3 = malloc(strlen(string_itoa(bit_index))+1);
	auxStr3 = string_itoa(bit_index);
	char* bloque = malloc(strlen("[")+strlen(auxStr3)+strlen("]")+1);
	strcpy(bloque, "[");
	strcat(bloque, auxStr3);
	strcat(bloque, "]");
	config_set_value(tmp, "SIZE", "0");
	config_set_value(tmp, "BLOCKS", bloque);
//	log_debug(logger, "El bloque libre a escribir es: %s", bloque);
	config_save(tmp);
	char* bloqueDumpeo = malloc(strlen(configLFS->dirMontaje)+strlen("Bloques/")+strlen(string_itoa(bit_index))+strlen(".bin")+1);
	strcpy(bloqueDumpeo, configLFS->dirMontaje);
	strcat(bloqueDumpeo, "Bloques/");
	strcat(bloqueDumpeo, string_itoa(bit_index));
	strcat(bloqueDumpeo, ".bin");
	//wait
	int fd2 = open(bloqueDumpeo, O_RDWR | O_CREAT | O_TRUNC, (mode_t) 0600);
	//signal
	bajarAMemoria(fd2, registroParaEscribir, tmp);
	close(fd2);
	config_destroy(tmp);
	list_destroy_and_destroy_elements(registros, (void*)liberarRegistro);
	free(registroParaEscribir);
}

void bajarAMemoria(int fd2, char* registroParaEscribir, t_config* tmp) {
	size_t textsize = strlen(registroParaEscribir) + 1; // + \0 null character
	char* strsize = malloc(strlen(string_itoa(textsize))+1);
	strcpy(strsize, string_itoa(textsize - 1));
	config_set_value(tmp, "SIZE", strsize);
	//wait
	config_save(tmp);
	//signal
	free(strsize);
	char* map = mapearBloque(fd2, configMetadata->blockSize);
	int i = 0;
	int posmap = 0;
	while (i < textsize) {
		if (posmap < configMetadata->blockSize)
			map[posmap] = registroParaEscribir[i];
		else {
//			TODO: armar lo que sigue como subfuncion, esta en dumpeo memoria repetido
			msync(map, posmap-1, MS_SYNC);
			munmap(map, posmap-1);
			off_t bit_index = obtener_bit_libre();
			actualizarBloquesEnTemporal(tmp, bit_index);
			char* auxStr = malloc(strlen(string_itoa(bit_index))+1);
			auxStr = string_itoa(bit_index);
			char* bloqueDumpeoNuevo = malloc(strlen(configLFS->dirMontaje)+strlen("Bloques/")+strlen(auxStr)+strlen(".bin")+1);
			strcpy(bloqueDumpeoNuevo, configLFS->dirMontaje);
			strcat(bloqueDumpeoNuevo, "Bloques/");
			strcat(bloqueDumpeoNuevo, auxStr);
			strcat(bloqueDumpeoNuevo, ".bin");
			//log_debug(logger, "Creo bloque: %s", bloqueDumpeoNuevo);
			//wait
			close(fd2);
			fd2 = open(bloqueDumpeoNuevo, O_RDWR | O_CREAT | O_TRUNC, (mode_t) 0600);
			if(fd2<0)
				log_warning(logger,"ROMPI EN EL BLOQUE %d",bit_index);
			//signal
//			Hasta aca
			if ((textsize - i) > (configMetadata->blockSize - 1))
				map = mapearBloque(fd2, posmap-1);
			else
				map = mapearBloque(fd2, textsize-i);
			posmap = 0;
			map[posmap] = registroParaEscribir[i];
			free(bloqueDumpeoNuevo);
			free(auxStr);
		}
		i++;
		posmap++;
	}
	//wait
	msync(map, posmap-1, MS_SYNC);
	//signal
	munmap(map, posmap-1);
}


char* mapearBloque(int fd2, size_t textsize) {
	lseek(fd2, textsize, SEEK_SET);
	int errorHandler = write(fd2, "", 1);
	if (errorHandler < 0)
		log_debug(logger, "error al mapear el bloque");
	//wait
	char* map = malloc(textsize + 1);
	map = mmap(0, textsize, PROT_READ | PROT_WRITE, MAP_SHARED, fd2, 0);
	//signal
	return map;
}


void actualizarBloquesEnTemporal(t_config* tmp, off_t bloque) {
	char* aux = malloc(strlen(config_get_string_value(tmp, "BLOCKS")+1));
	aux = config_get_string_value(tmp, "BLOCKS");
	aux = string_substring_until(aux, (strlen(aux) - 1));
	char* auxStr = malloc(strlen(string_itoa(bloque))+1);
	auxStr = string_itoa(bloque);
	char* bloques = malloc(strlen(aux)+strlen(",")+strlen(auxStr)+strlen("]")+1);
	strcpy(bloques, aux);
	strcat(bloques, ",");
	strcat(bloques, auxStr);
	strcat(bloques, "]");
	config_set_value(tmp, "BLOCKS", bloques);
	//wait
	config_save(tmp);
	//signal
	free(aux);
	free(auxStr);
	free(bloques);
}

//
int compactacion(char* nombre_tabla) {
	int dumpeosCompactacion = 0;
	pthread_mutex_t* mutex = dictionary_get(bloqueoTablas, nombre_tabla);
	if(mutex!=NULL)
		pthread_mutex_lock(mutex);
	int dump=contadorDeArchivos(nombre_tabla, "tmp");
	for (int j = 0; j < dump; j++) {
		char* auxStr = malloc(strlen(string_itoa(j))+1);
		auxStr = string_itoa(j);
		char* rutaARenombrar = malloc(strlen("mv ")+
				strlen(configLFS->dirMontaje)+strlen("Tablas/")+strlen(nombre_tabla)+strlen("/")+strlen(auxStr)+strlen(".tmp ")+
				strlen(configLFS->dirMontaje)+strlen("Tablas/")+strlen(nombre_tabla)+strlen("/")+strlen(auxStr)+strlen(".tmpc")+1);
		strcpy(rutaARenombrar, "mv ");
		strcat(rutaARenombrar, configLFS->dirMontaje);
		strcat(rutaARenombrar, "Tablas/");
		strcat(rutaARenombrar, nombre_tabla);
		strcat(rutaARenombrar, "/");
		strcat(rutaARenombrar, auxStr);
		strcat(rutaARenombrar, ".tmp ");
		strcat(rutaARenombrar, configLFS->dirMontaje);
		strcat(rutaARenombrar, "Tablas/");
		strcat(rutaARenombrar, nombre_tabla);
		strcat(rutaARenombrar, "/");
		strcat(rutaARenombrar, auxStr);
		strcat(rutaARenombrar, ".tmpc");
		system(rutaARenombrar);
		free(auxStr);
		free(rutaARenombrar);
		dumpeosCompactacion++;
	}
	if(mutex!=NULL)
		pthread_mutex_unlock(mutex);

	char* temporales = malloc(1);
	temporales[0] = '\0';
	char* binarios = malloc(1);
	binarios[0] = '\0';
	t_list* lista_bin = list_create();
	t_list* lista_Temp = list_create();
	t_list* listaCompactada = list_create();
	temporales = obtener_temporales(nombre_tabla, temporales);
	pthread_mutex_lock(mutex);
	binarios = levantarbinarios(nombre_tabla, binarios);
	pthread_mutex_unlock(mutex);
//	log_debug(logger, "Sali de obtener levantar binarios");
	if (!string_is_empty(temporales)){
//		log_debug(logger, "La lista de temporales es: %s", temporales);
		crearListaRegistros(temporales, lista_Temp);
	}
//	log_debug(logger, "Sali de crearListaRegistros1");
	if (!string_is_empty(binarios))
		crearListaRegistros(binarios, lista_bin);
//	log_debug(logger, "Sali de crearListaRegistros2");
	free(temporales);
	free(binarios);

		void compactar(registro* reg) {
			bool esta_registro(registro* registroBin) {
				if (reg->key == registroBin->key)
					return true;
				return false;
			}
			bool hay_registro_mayor(registro* registrotmp) {
				if (reg->key == registrotmp->key
						&& reg->timestamp < registrotmp->timestamp)
					return true;
				return false;
			}
			registro* encontrado = list_find(lista_bin, (void*) &esta_registro);
			if (encontrado != NULL) { //replace
				if (encontrado->timestamp < reg->timestamp) {
					registro* encontrado1 = list_find(lista_Temp, (void*) &hay_registro_mayor);
					if (encontrado1 == NULL)
						list_add(listaCompactada, reg);
				} else {
					registro* encontrado2 = list_find(lista_Temp, (void*) &hay_registro_mayor);
					if (encontrado2 == NULL)
						list_add(listaCompactada, encontrado);
				}
			} else {
				registro* encontrado3 = list_find(lista_Temp, (void*) &hay_registro_mayor);
				if (encontrado3 == NULL)
					list_add(listaCompactada, reg);
			}
		}

		void agregarFaltantes(registro* reg) {
			bool esta_registro(registro* registroBin) {
				if (reg->key == registroBin->key)
					return true;
				return false;
			}
			registro* encontrado = list_find(lista_Temp, (void*) &esta_registro);

			if (encontrado == NULL) { //replace
				list_add(listaCompactada, reg);
			}
		}

//  TODO: no se porque las dos veces compacta el segundo elemento de la lista, crearListaRegistros se arma bien.
//	Probado con dos INSERTS

//	log_debug(logger, "Hago los iterates");
	list_iterate(lista_bin, (void*) &agregarFaltantes);
	list_iterate(lista_Temp, (void*) &compactar);
//	log_debug(logger, "Termine los iterates");

	char* tabla = malloc(strlen(configLFS->dirMontaje)+strlen("Tablas/")+strlen(nombre_tabla)+strlen("/metadata")+1);
	strcpy(tabla, configLFS->dirMontaje);
	strcat(tabla, "Tablas/");
	strcat(tabla, nombre_tabla);
	strcat(tabla, "/metadata");
	t_config* t = malloc(sizeof(t_config));
	t = config_create(tabla);
	int part = config_get_int_value(t, "PARTITIONS");
	config_destroy(t);
//	log_debug(logger, "Arranco a liberar bloques");
	unsigned long long tiempoLock = obtenerTimestamp();
	pthread_mutex_lock(mutex);
	liberar_bloques(nombre_tabla, part, dumpeosCompactacion);
	guardar_en_disco(listaCompactada, part, nombre_tabla);
	pthread_mutex_unlock(mutex);
	unsigned long long tiempoUnlock = obtenerTimestamp();
	log_debug(logger, "Se tardaron %llu milisegundos en realizar la compactacion", (tiempoUnlock - tiempoLock));
	dumpeosCompactacion = 0;
	list_destroy_and_destroy_elements(lista_bin, (void*)liberarRegistro);
	list_destroy_and_destroy_elements(lista_Temp, (void*)liberarRegistro);
	list_destroy(listaCompactada);
	return 0;
}

void liberar_bloques(char* nombre_tabla, int cantParticiones, int dumpeosCompactacion) {
	t_bitarray* bitmap = levantarBitmap();
	///////////////////liberoTMPC
	int dumpeosCompac=contadorDeArchivos(nombre_tabla, "tmpc");
	for (int aux = 1; aux <= dumpeosCompac; aux++) {
		char* auxStr = malloc(strlen(string_itoa(aux - 1))+1);
		auxStr = string_itoa(aux - 1);
		char* rutaTemporal = malloc(strlen(configLFS->dirMontaje)+strlen("Tablas/")+strlen(nombre_tabla)+strlen("/")+strlen(auxStr)+strlen(".tmpc")+1);
		strcpy(rutaTemporal, configLFS->dirMontaje);
		strcat(rutaTemporal, "Tablas/");
		strcat(rutaTemporal, nombre_tabla);
		strcat(rutaTemporal, "/");
		strcat(rutaTemporal, auxStr);
		strcat(rutaTemporal, ".tmpc");
		t_config* particion = malloc(sizeof(t_config));
		particion = config_create(rutaTemporal);
		char** bloquesABuscar = config_get_array_value(particion, "BLOCKS");
		config_set_value(particion, "BLOCKS","[]");
		config_save(particion);
		config_destroy(particion);
		int r = 0;
		//		Unifico la informacion de todos los bloques en los que esta dividido el archivo .tmp
		while (bloquesABuscar[r]) {
			char* bloque_a_limpiar = malloc(strlen("rm ")+strlen(configLFS->dirMontaje)+strlen("Bloques/")+strlen(bloquesABuscar[r])+strlen(".bin")+1);
			strcpy(bloque_a_limpiar, "rm ");
			strcat(bloque_a_limpiar, configLFS->dirMontaje);
			strcat(bloque_a_limpiar, "Bloques/");
			strcat(bloque_a_limpiar, bloquesABuscar[r]);
			strcat(bloque_a_limpiar, ".bin");
			//log_warning(logger, "voy a ejecutar rm %sBloques/%s.bin", configLFS->dirMontaje, bloquesABuscar[r]);
			system(bloque_a_limpiar);
			bitarray_clean_bit(bitmap, atoi(bloquesABuscar[r]));
			free(bloque_a_limpiar);
			r++;
		}
		string_iterate_lines(bloquesABuscar, (void*) free);
		free(bloquesABuscar);
		free(rutaTemporal);

		char* rutaARenombrar = malloc(strlen("rm ")+strlen(configLFS->dirMontaje)+strlen("Tablas/")+strlen(nombre_tabla)+strlen("/")+strlen(auxStr)+strlen(".tmpc")+1);
		strcpy(rutaARenombrar, "rm ");
		strcat(rutaARenombrar, configLFS->dirMontaje);
		strcat(rutaARenombrar, "Tablas/");
		strcat(rutaARenombrar, nombre_tabla);
		strcat(rutaARenombrar, "/");
		strcat(rutaARenombrar, auxStr);
		strcat(rutaARenombrar, ".tmpc");
		system(rutaARenombrar);
		free(auxStr);
		free(rutaARenombrar);
	}

	/////////////////////liberoBIN
	for (int p = 0; p <= cantParticiones - 1; p++) {
		char* auxStr2 = malloc(strlen(string_itoa(p))+1);
		auxStr2 = string_itoa(p);
		char* tablaParaDumpeo = malloc(strlen(configLFS->dirMontaje)+strlen("Tablas/")+strlen(nombre_tabla)+strlen("/")+strlen(auxStr2)+strlen(".bin")+1);
		strcpy(tablaParaDumpeo, configLFS->dirMontaje);
		strcat(tablaParaDumpeo, "Tablas/");
		strcat(tablaParaDumpeo, nombre_tabla);
		strcat(tablaParaDumpeo, "/");
		strcat(tablaParaDumpeo, auxStr2);
		strcat(tablaParaDumpeo, ".bin");
		t_config* tmp = malloc(sizeof(t_config));
		tmp = config_create(tablaParaDumpeo);
		free(auxStr2);
		free(tablaParaDumpeo);
		char** bloques = config_get_array_value(tmp, "BLOCKS");
		config_set_value(tmp, "BLOCKS","[]");
		config_save(tmp);
		config_destroy(tmp);
		int s = 0;
		while (bloques[s] != NULL) {
			char* bloque_a_limpiar = malloc(strlen("rm ")+strlen(configLFS->dirMontaje)+strlen("Bloques/")+strlen(bloques[s])+strlen(".bin")+1);
			strcpy(bloque_a_limpiar, "rm ");
			strcat(bloque_a_limpiar, configLFS->dirMontaje);
			strcat(bloque_a_limpiar, "Bloques/");
			strcat(bloque_a_limpiar, bloques[s]);
			strcat(bloque_a_limpiar, ".bin");
			//log_debug(logger, "PARTE 2 voy a ejecutar rm %sBloques/%s.bin", configLFS->dirMontaje, bloques[s]);
			system(bloque_a_limpiar);
			free(bloque_a_limpiar);
			bitarray_clean_bit(bitmap, atoi(bloques[s]));
			s++;
		}
		string_iterate_lines(bloques, (void*) free);
		free(bloques);
	}
	bitarray_destroy(bitmap);
}

void liberarRegistro(registro* UnRegistro){
	free(UnRegistro->value);
	free(UnRegistro);
}

void guardar_en_disco(t_list* binarios, int cantParticiones, char* nombre_tabla) {
	t_list* duplicada = list_create();
	for (int i = 0; i < cantParticiones; i++) {
		bool filtrarPorParticion(registro* reg) {
			if ((reg->key % cantParticiones) == i)
				return true;
			return false;
		}
		if (i != 0)
			list_clean(duplicada);

		list_add_all(duplicada, binarios);
		t_list* listaParticionada = list_filter(duplicada, (void*) &filtrarPorParticion);
		char* auxStr = malloc(strlen(string_itoa(i))+1);
		auxStr = string_itoa(i);
		char* tablaParaDumpeo = malloc(strlen(configLFS->dirMontaje)+strlen("Tablas/")+strlen(nombre_tabla)+strlen("/")+strlen(auxStr)+strlen(".bin")+1);
		strcpy(tablaParaDumpeo, configLFS->dirMontaje);
		strcat(tablaParaDumpeo, "Tablas/");
		strcat(tablaParaDumpeo, nombre_tabla);
		strcat(tablaParaDumpeo, "/");
		strcat(tablaParaDumpeo, auxStr);
		strcat(tablaParaDumpeo, ".bin");
		char* registroParaEscribir = malloc(1);
		registroParaEscribir[0] = '\0';

		void dumpearRegistros(registro* UnRegistro) {
			char* auxstr = malloc(14);
			sprintf(auxstr, "%llu", (UnRegistro->timestamp));
			registroParaEscribir = realloc(registroParaEscribir, (strlen(registroParaEscribir)+ 13 + strlen(";") + strlen(string_itoa(UnRegistro->key)) + strlen(";") + strlen(UnRegistro->value) + strlen("\n")+1));
			strcat(registroParaEscribir, auxstr);
			strcat(registroParaEscribir, ";");
			strcat(registroParaEscribir, string_itoa(UnRegistro->key));
			strcat(registroParaEscribir, ";");
			strcat(registroParaEscribir, UnRegistro->value);
			strcat(registroParaEscribir, "\n");
			free(auxstr);
		}

		list_iterate(listaParticionada, (void*) &dumpearRegistros);
		t_config* tmp = malloc(sizeof(t_config));
		tmp = config_create(tablaParaDumpeo);
		off_t bit = obtener_bit_libre();
		char* nuevoBloque = malloc(strlen("[")+strlen(string_itoa(bit))+strlen("]")+1);
		strcpy(nuevoBloque, "[");
		strcat(nuevoBloque, string_itoa(bit));
		strcat(nuevoBloque, "]");
		config_set_value(tmp, "BLOCKS", nuevoBloque);
		config_save(tmp);
		char* bloque = malloc(strlen(configLFS->dirMontaje)+strlen("Bloques/")+strlen(string_itoa(bit))+strlen(".bin")+1);
		strcpy(bloque, configLFS->dirMontaje);
		strcat(bloque, "Bloques/");
		strcat(bloque, string_itoa(bit));
		strcat(bloque, ".bin");
		int fd2 = open(bloque, O_RDWR | O_CREAT | O_TRUNC, (mode_t) 0600);
		bajarAMemoria(fd2, registroParaEscribir, tmp);
		config_save(tmp);
		config_destroy(tmp);
		close(fd2);
		free(bloque);
		free(nuevoBloque);
		free(registroParaEscribir);
		free(auxStr);
		free(tablaParaDumpeo);
		list_destroy(listaParticionada);
	}
	list_destroy(duplicada);
}

void crearListaRegistros(char* string, t_list* lista) {
//	log_debug(logger, "Entre a crearListaRegistros");
	int j = 0;
	if (!string_is_empty(string)){
		char** registros = string_split(string, "\n");
//		Recorro y divido los datos unificado del archivos temporal, almacenando solo las keys que coincidan con la solicitada
		while (registros[j] != NULL) {
			registro* reg = malloc(sizeof(registro));
			char** datos_registro = string_split(registros[j], ";");
			reg->timestamp = strtoull(datos_registro[0],NULL,10);
//			log_debug(logger, "En ListaDeRegistros %s", datos_registro[0]);
//			log_debug(logger, "En ListaDeRegistros el timestamp es: %llu", reg->timestamp);
			reg->key = atoi(datos_registro[1]);
			reg->value = malloc(strlen(datos_registro[2]) + 1);
			strcpy(reg->value, datos_registro[2]);
			list_add(lista, reg);
			string_iterate_lines(datos_registro, (void*) free);
			free(datos_registro);
			j++;
		}
		string_iterate_lines(registros, (void*) free);
		free(registros);
	}
}

char* levantarbinarios(char* nombre_tabla, char* bloquesUnificados) {
//	log_debug(logger, "Entre a levantar binarios");
	char* rutametadata = malloc(strlen(configLFS->dirMontaje)+strlen("Tablas/")+strlen(nombre_tabla)+strlen("/metadata")+1);
	strcpy(rutametadata, configLFS->dirMontaje);
	strcat(rutametadata, "Tablas/");
	strcat(rutametadata, nombre_tabla);
	strcat(rutametadata, "/metadata");
	t_config* metadataTabla = malloc(sizeof(t_config));
	metadataTabla = config_create(rutametadata);
	int cantParticiones = (config_get_int_value(metadataTabla, "PARTITIONS") - 1);
	config_destroy(metadataTabla);
	for (int aux = 0; aux <= cantParticiones; aux++) {
		char* auxStr = malloc(strlen(string_itoa(aux))+1);
		auxStr = string_itoa(aux);
		char* rutaBinario = malloc(strlen(configLFS->dirMontaje)+strlen("Tablas/")+strlen(nombre_tabla)+strlen("/")+strlen(auxStr)+strlen(".bin")+1);
		strcpy(rutaBinario, configLFS->dirMontaje);
		strcat(rutaBinario, "Tablas/");
		strcat(rutaBinario, nombre_tabla);
		strcat(rutaBinario, "/");
		strcat(rutaBinario, auxStr);
		strcat(rutaBinario, ".bin");
		t_config* particion = malloc(sizeof(t_config));
		particion = config_create(rutaBinario);
		int size = config_get_int_value(particion, "SIZE");
		char** bloquesABuscar = config_get_array_value(particion, "BLOCKS");
		config_destroy(particion);
		if (size > 0) {
			int i = 0;
			while (bloquesABuscar[i] != NULL) {
				char* bloque = malloc(strlen(configLFS->dirMontaje) + strlen("Bloques/") + strlen(bloquesABuscar[i]) + strlen(".bin") + 1);
				strcpy(bloque, configLFS->dirMontaje);
				strcat(bloque, "Bloques/");
				strcat(bloque, bloquesABuscar[i]);
				strcat(bloque, ".bin");
				int fd = open(bloque, O_RDONLY, S_IRUSR | S_IWUSR);
				struct stat s;
				fstat(fd, &s);
				size = s.st_size;
				if ((fd != -1) && (size > 0)){
					char* f = malloc(configMetadata->blockSize + 1);
					f = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);
					bloquesUnificados = realloc(bloquesUnificados, (strlen(bloquesUnificados) + string_length(f) + 1));
					strcat(bloquesUnificados, f);
					munmap(f, size);
				}
				close(fd);
				free(bloque);
				i++;
			}
		}
		string_iterate_lines(bloquesABuscar, (void*) free);
		free(bloquesABuscar);
		free(auxStr);
		free(rutaBinario);
	}
	return bloquesUnificados;
}

char* obtener_temporales(char* nombre_tabla, char* bloquesUnificados) {
	int dumpeosCompac=contadorDeArchivos(nombre_tabla, "tmpc");
	for (int aux = 1; aux <= dumpeosCompac; aux++) {
		char* auxStr = malloc(strlen(string_itoa(aux - 1))+1);
		auxStr = string_itoa(aux - 1);
		char* rutaTemporal = malloc(strlen(configLFS->dirMontaje)+strlen("Tablas/")+strlen(nombre_tabla)+strlen("/")+strlen(auxStr)+strlen(".tmpc")+1);
		strcpy(rutaTemporal, configLFS->dirMontaje);
		strcat(rutaTemporal, "Tablas/");
		strcat(rutaTemporal, nombre_tabla);
		strcat(rutaTemporal, "/");
		strcat(rutaTemporal, auxStr);
		strcat(rutaTemporal, ".tmpc");
		t_config* particion = malloc(sizeof(t_config));
		particion = config_create(rutaTemporal);
		int size = config_get_int_value(particion, "SIZE");
		char** bloquesABuscar = config_get_array_value(particion, "BLOCKS");
		config_destroy(particion);
		if (size > 0) {
			int i = 0;
			while (bloquesABuscar[i] != NULL) {
				char* bloque = malloc(strlen(configLFS->dirMontaje)+strlen("Bloques/")+strlen(bloquesABuscar[i])+strlen(".bin")+1);
				strcpy(bloque, configLFS->dirMontaje);
				strcat(bloque, "Bloques/");
				strcat(bloque, bloquesABuscar[i]);
				strcat(bloque, ".bin");
				int fd = open(bloque, O_RDONLY, S_IRUSR | S_IWUSR);
				struct stat s;
				fstat(fd, &s);
				size = s.st_size;
				if ((fd != -1) && (size > 0)){
					char* f = malloc(configMetadata->blockSize + 1);
					f = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);
	//				log_debug(logger, "el tamaño de bloquesUnificados antes es: %d", strlen(bloquesUnificados));
	//				log_debug(logger, "el tamaño de f antes es: %d", strlen(f));
					bloquesUnificados = realloc(bloquesUnificados, (strlen(bloquesUnificados) + strlen(f) + 1));
	//				log_debug(logger, "el tamaño de bloquesUnificados despues es: %d", strlen(bloquesUnificados));
					strcat(bloquesUnificados, f);
					munmap(f, size);
				}
				close(fd);
				free(bloque);
				i++;
			}
		}
		string_iterate_lines(bloquesABuscar, (void*) free);
		free(bloquesABuscar);
		free(auxStr);
		free(rutaTemporal);
	}
	return bloquesUnificados;
}

///

// ---------------OTROS-----------------

char* direccionarTabla(char* tabla) {
	char* ruta = malloc(strlen(configLFS->dirMontaje)+strlen("Tablas/")+strlen(tabla)+1);
	strcpy(ruta, configLFS->dirMontaje);
	strcat(ruta, "Tablas/");
	strcat(ruta, tabla);
	return ruta;
}

void crearBitmapNuestro() {

	int size = configMetadata->blocks / 8;
	if (configMetadata->blocks % 8 != 0)
		size++;
	char* bitarray = calloc(configMetadata->blocks / 8, sizeof(char));
	t_bitarray* structBitarray = bitarray_create_with_mode(bitarray, size, MSB_FIRST);
	for (int i = 0; i < configMetadata->blocks; i++) {
			bitarray_clean_bit(structBitarray, i);
	}
	char* path = string_from_format("%s/Metadata/Bitmap.bin", configLFS->dirMontaje);
	FILE* file = fopen(path, "wb+");
	fwrite(structBitarray->bitarray, sizeof(char), configMetadata->blocks / 8, file);
	fclose(file);
	bitarray_destroy(structBitarray);
	free(bitarray);
	free(path);
}

int borrarDirectorio(char *dir) {
	FTS* ftsp;
	FTSENT* curr;
	char* directorios[] = { (char *) dir, NULL };
	ftsp = fts_open(directorios, FTS_NOCHDIR | FTS_PHYSICAL | FTS_XDEV, NULL);
	if (!ftsp)
		return -1;
	while ((curr = fts_read(ftsp))) {
		switch (curr->fts_info) {
		case FTS_NS:
		case FTS_DNR:
		case FTS_ERR:
			return -1;
			break;
		case FTS_DP:
		case FTS_F:
		case FTS_SL:
		case FTS_SLNONE:
		case FTS_DEFAULT:
			if (remove(curr->fts_accpath) < 0)
				return -1;
			break;
		}
	}
	fts_close(ftsp);
	return todoJoya;
}

// ---------------CIERRE-----------------

void finalizarEjecutcion() {
	log_debug(logger, "------------------------");
	log_debug(logger, "¿chau chau adios?");
	log_debug(logger, "------------------------");

	log_destroy(logger);
	close(socket_cli);
	close(socket_sv);
	list_destroy_and_destroy_elements(memtable, (void*)liberarRegistro);
	free(configLFS->dirMontaje);
	free(configLFS->puerto);
	free(configLFS);
	free(configMetadata->magicNumber);
	free(configMetadata);
	dictionary_destroy(bloqueoTablas);
//	pthread_cancel(hiloConsola);
//	pthread_cancel(hiloDumpeo);
//	pthread_cancel(hiloCompactacion);
//	pthread_cancel(hiloSocket);
//	pthread_cancel(hiloInnotify);
	raise(SIGTERM);
}

void innotificar() {
//	log_debug(logger, "entre al hilo");
	while (1) {
		char buffer[BUF_LEN];
		int file_descriptor = inotify_init();
		if (file_descriptor < 0)
			perror("inotify_init");
		int watch_descriptor = inotify_add_watch(file_descriptor, "../",
		IN_MODIFY | IN_CREATE | IN_DELETE);
		int length = read(file_descriptor, buffer, BUF_LEN);
		if (length < 0)
			perror("read");
		int offset = 0;
		while (offset < length) {
			struct inotify_event *event =
					(struct inotify_event *) &buffer[offset];
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
						levantarConfigLFS();
					}
				}
			}
			offset += sizeof(struct inotify_event) + event->len;
		}
		inotify_rm_watch(file_descriptor, watch_descriptor);
		close(file_descriptor);
	}
}
