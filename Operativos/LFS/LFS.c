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
int cantidadDeDumpeos = 0;
t_log* logger;
t_list* memtable;
config_FS* configLFS;
metadataFS* configMetadata;
pthread_mutex_t mem_table_mutex;
// ---------------MAIN-----------------

int main(void) {
	configLFS = malloc(sizeof(config_FS));
	configMetadata = malloc(sizeof(metadataFS));
	pthread_t hiloSocket;
	pthread_t hiloCompactacion;
	pthread_t hiloConsola;
	pthread_t hiloInnotify;
	pthread_t hiloDumpeo;

	// ---------------Inicializacion-----------------
	memtable = inicializarMemtable();
	logger = iniciar_logger();
	levantarMetadataFS();
	levantarConfigLFS();
	pthread_mutex_init(&mem_table_mutex, NULL);
	// ---------------Pruebas con bitmap propio-----------------
	crearBitmapNuestro();

	// Escribir inicio en consola

	// Abro hilo de consola
	paramsConsola = malloc(sizeof(tHiloConsola));
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
	while (1) {
	}
//Rutina de cierre
	log_destroy(logger);
	close(socket_cli);
	close(socket_sv);
	list_destroy_and_destroy_elements(memtable, free);
	free(configLFS->dirMontaje);
	return 0;
}

void levantarMetadataFS() {
	t_config* aux = config_create("../../FS_LISSANDRA/Metadata/Metadata.bin");
	configMetadata->blockSize = config_get_int_value(aux, "BLOCK_SIZE");
	configMetadata->blocks = config_get_int_value(aux, "BLOCKS");
	configMetadata->magicNumber = malloc(
			strlen(config_get_string_value(aux, "MAGIC_NUMBER")));
	strcpy(configMetadata->magicNumber,
			config_get_string_value(aux, "MAGIC_NUMBER"));
	config_destroy(aux);
	log_debug(logger, "%d", configMetadata->blockSize);
	log_debug(logger, "%d", configMetadata->blocks);
	log_debug(logger, "%s", configMetadata->magicNumber);
}

void levantarConfigLFS() {
	t_config *aux = config_create("../LFS.config");
	configLFS->puerto = malloc(
			strlen(config_get_string_value(aux, "PUERTO_ESCUCHA")));
	strcpy(configLFS->puerto, config_get_string_value(aux, "PUERTO_ESCUCHA"));
	configLFS->dirMontaje = malloc(
			strlen(config_get_string_value(aux, "PUNTO_MONTAJE")));
	strcpy(configLFS->dirMontaje,
			config_get_string_value(aux, "PUNTO_MONTAJE"));
	configLFS->retardo = config_get_int_value(aux, "RETARDO");
	configLFS->tamanioValue = config_get_int_value(aux, "TAMANIO_VALUE");
	configLFS->tiempoDumpeo = config_get_int_value(aux, "TIEMPO_DUMP");
	config_destroy(aux);
	log_debug(logger, "%s", configLFS->puerto);
	log_debug(logger, "%s", configLFS->dirMontaje);
	log_debug(logger, "%d", configLFS->retardo);
	log_debug(logger, "%d", configLFS->tamanioValue);
	log_debug(logger, "%d", configLFS->tiempoDumpeo);
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
		int bytes = recv(*socket, &header, sizeof(type), MSG_WAITALL);
		usleep((configLFS->retardo) * 1000);
		if (0 > bytes) {
			close(*socket);
			log_debug(logger, "cerre los socket");
			return;
		}

		switch (header) {
		case SELECT:
			log_debug(logger, "Comando SELECT recibido");
			tSelect* packSelect = malloc(sizeof(tSelect));
			if (!packSelect)
				errorHandler = errorDeMalloc;
			else {
				desSerializarSelect(packSelect, *socket);
				//TODO: handlear error de desSerializarSelect
				packSelect->type = header;
				registro* reg = malloc(sizeof(registro));
				if (!reg)
					errorHandler = errorDeMalloc;
				else {
					//TODO: handlear error de Y hacer failfast
					errorHandler = Select(&reg, packSelect->nombre_tabla,
							packSelect->key);
					log_debug(logger, "sali del Select");
					if (!errorHandler) {
						tRegistroRespuesta* registro = malloc(
								sizeof(tRegistroRespuesta));
						if (!registro)
							errorHandler = errorDeMalloc;
						else {
							registro->tipo = REGISTRO;
							registro->timestamp = reg->timestamp;
							registro->value = reg->value;
							registro->key = reg->key;
							registro->value_long = strlen(reg->value) + 1;
							log_debug(logger, "guarde valores en el registro");
							char* registroSerializado = serializarRegistro(
									registro);
							//TODO: handlear error de serializarRegistro
							enviarPaquete(*socket, registroSerializado,
									registro->length);
							log_debug(logger, "mande pack");
							//TODO: handlear error de enviarPaquete
							free(registroSerializado);
						}
						free(registro);
					}
				}
				free(reg);
			}
			free(packSelect);
			break;

		case INSERT:
			log_debug(logger, "Comando INSERT recibido");
			tInsert* packInsert = malloc(sizeof(tInsert));
			if (!packInsert)
				errorHandler = errorDeMalloc;
			else {
				desSerializarInsert(packInsert, *socket);
				//TODO: handlear error de desSerializarInsert
				packInsert->type = header;
				log_debug(logger, "Tabla %s con key %d y value %s \n",
						packInsert->nombre_tabla, packInsert->key,
						packInsert->value);
				errorHandler = insertarEnMemtable(packInsert);
			}
			free(packInsert);
			break;

		case CREATE:
			log_debug(logger, "Comando CREATE recibido");
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
			log_debug(logger, "Comando ADD recibido");
			break;

		case DESCRIBE:
			log_debug(logger, "Comando DESCRIBE recibido");
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
				int b = send(*socket, serializedPackage,
						cantidad_de_tablas * sizeof(t_metadata)
								+ sizeof(describe->cant_tablas), 0);
				log_debug(logger, "envie %d bytes", b);
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
				t_metadata meta;
				meta.consistencia = SC;
				strcpy(meta.nombre_tabla, "NO_TABLE");
				describe->tablas[0] = meta;
				char* serializedPackage;
				serializedPackage = serializarDescribe_Response(describe);
				send(*socket, serializedPackage,
						sizeof(t_metadata) + sizeof(describe->cant_tablas), 0);
				dispose_package(&serializedPackage);
				//list_destroy(metadatas);
			}
			free(packDescribe);
			free(metadata);
			break;

		case DROP:
			/*
			 log_debug(logger, "Comando DROP recibido");
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
			 */
			dumpeoMemoria();
			compactacion("FELO");
			break;

		case JOURNAL:
			log_debug(logger, "Comando JOURNAL recibido");
			break;

		case NIL:
			log_debug(logger, "Comando NIL recibido");
			break;

		case REGISTRO:
			log_debug(logger, "Comando REGISTRO recibido");
			break;

		case RUN:
			log_debug(logger, "Comando RUN recibido");
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
			errorHandler = Select(&reg, packSelectConsola->nombre_tabla,
					packSelectConsola->key);
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
			log_debug(logger, "%d", packInsertConsola->timestamp);
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
			log_debug(logger, "Comando DROP recibido");

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
			cargarPaqueteDescribe(packDescribeConsola,
					string_substring_until(paramsConsola->consulta,
							strlen(paramsConsola->consulta) - 1));
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
	log_debug(logger, "entre al primer hilo");
//	int errorHandler;
	pthread_t threadPrincipal;
	socket_sv = levantarServidor(configLFS->puerto);
	if (socket_sv < 0)
		logeoDeErroresLFS(noLevantoServidor, logger);
	log_debug(logger, "%d", socket_sv);
	socket_cli = aceptarCliente(socket_sv);
	log_debug(logger, "%d", socket_cli);
	send(socket_cli, &configLFS->tamanioValue, 4, 0);
	pthread_create(&threadPrincipal, NULL, (void*) receptorDeSockets,
			&socket_cli);
	while (1) {
		log_debug(logger, "entre al while para crear una nueva memoria");
		int socketNuevo;
		pthread_t threadNuevo;
		socketNuevo = aceptarCliente(socket_sv);
		send(socketNuevo, &configLFS->tamanioValue, 4, 0);
		pthread_create(&threadNuevo, NULL, (void*) receptorDeSockets,
				&socketNuevo);
		log_debug(logger, "levante cliente");
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
	char* tablas_path = string_new();
	string_append(&tablas_path, configLFS->dirMontaje);
	string_append(&tablas_path, "Tablas/");
	log_debug(logger, tablas_path);
	tables_directory = opendir(tablas_path);
	if (tables_directory) {
		log_debug(logger, "Entre al if");
		while ((a_directory = readdir(tables_directory)) != NULL) {
			log_debug(logger, "Entre al while");
			if (strcmp(a_directory->d_name, ".")
					&& strcmp(a_directory->d_name, "..")) {

				char* table_name = malloc(strlen(a_directory->d_name));
				memcpy(table_name, a_directory->d_name,
						strlen(a_directory->d_name) + 1);

				pthread_t threadComp;
				pthread_create(&threadComp, NULL, (void*) hiloCompactar,
						(void*) table_name);
				//free(table_name);
			}
		}
		closedir(tables_directory);
	}
	free(tablas_path);
}

void hiloCompactar(void* nombre_tabla) {

	while (1) {
		log_debug(logger, "intento compactar %s", (char*) nombre_tabla);
		if (!verificadorDeTabla((char*) nombre_tabla)) {
			log_debug(logger, "cieroo compactar");
			pthread_exit(NULL);
		}
		char* tablas_path = string_new();
		string_append(&tablas_path, configLFS->dirMontaje);
		string_append(&tablas_path, "Tablas/");
		string_append(&tablas_path, (char*) nombre_tabla);
		string_append(&tablas_path, "/metadata");
		t_config* configComp = config_create(tablas_path);
		free(tablas_path);
		int retardoCompac = config_get_int_value(configComp, "COMPACTION_TIME");
		config_destroy(configComp);
		log_debug(logger, "%d", retardoCompac);
		usleep(retardoCompac * 1000);
		if (!verificadorDeTabla((char*) nombre_tabla)) {
			log_debug(logger, "intento cierro");
			pthread_exit(NULL);
		}
		if (cantidadDeDumpeos > 0) {
			log_debug(logger, "compacto");
			compactacion((char*) nombre_tabla);

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
		errorHandler = agregar_tabla_a_memtable(packinsert->nombre_tabla);
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
	tabla_nueva->nombreTabla = string_new();
	strcpy(tabla_nueva->nombreTabla, tabla);
	tabla_nueva->registros = list_create();
	pthread_mutex_lock(&mem_table_mutex);
	cantidad_anterior = memtable->elements_count;
	indice_agregado = list_add(memtable, tabla_nueva);
	pthread_mutex_unlock(&mem_table_mutex);
	return indice_agregado + 1 > cantidad_anterior;
}

int existe_tabla_en_memtable(char* posible_tabla) {
	bool es_tabla(t_tabla* UnaTabla) {
		if (strcmp(UnaTabla->nombreTabla, posible_tabla) == 0)
			return true;
		return false;
	}pthread_mutex_lock(&mem_table_mutex);
	t_tabla* tabla_encontrada = (t_tabla*) list_find(memtable,
			(void*) &es_tabla);
	pthread_mutex_unlock(&mem_table_mutex);
	if (tabla_encontrada)
		return 1;
	return 0;
}

// ---------------APIs-----------------

int Create(char* NOMBRE_TABLA, char* TIPO_CONSISTENCIA, int NUMERO_PARTICIONES,
		int COMPACTATION_TIME) {

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
	errorHandler = crearMetadata(ruta, TIPO_CONSISTENCIA, NUMERO_PARTICIONES,
			COMPACTATION_TIME);
	if (errorHandler) {
		free(ruta);
		return metadataNoCreada;
	}
// ---- Creo los archivos binarios ----
	crearBinarios(ruta, NUMERO_PARTICIONES);
	free(ruta);
	pthread_t threadComp;
	log_debug(logger, "%s", NOMBRE_TABLA);
	char* table_name = malloc(strlen(NOMBRE_TABLA));
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
	list_remove_by_condition(memtable, &es_tabla);
	pthread_mutex_unlock(&mem_table_mutex);
	char* ruta = direccionarTabla(NOMBRE_TABLA);

	char* rutametadata = string_new();
	string_append(&rutametadata, ruta);
	string_append(&rutametadata, "/metadata");
	t_config* metadataTabla = config_create(rutametadata);
	int cantParticiones =
			(config_get_int_value(metadataTabla, "PARTITIONS") - 1);
	config_destroy(metadataTabla);
	free(rutametadata);
	t_bitarray* bitmap = levantarBitmap();
	for (int i = 0; i <= cantParticiones; i++) {
		char* binario_a_limpiar = string_new();
		string_append_with_format(&binario_a_limpiar, "%s/%d.bin", ruta, i);
		t_config* binario = config_create(binario_a_limpiar);
		char** bloques_a_limpiar = config_get_array_value(binario, "BLOCKS");
		int i = 0;
		while (bloques_a_limpiar[i] != NULL) {
			bitarray_clean_bit(bitmap, atoi(bloques_a_limpiar[i]));

			char* bloque_a_limpiar = string_new();
			string_append_with_format(&bloque_a_limpiar, "rm %sBloques/%s.bin",
					configLFS->dirMontaje, bloques_a_limpiar[i]);
			system(bloque_a_limpiar);
			free(bloque_a_limpiar);

			i++;
			//TODO: ELIMINAR EL BLOQUE PROPIAMENTE DICHO
		}
		free(bloques_a_limpiar);
		free(binario_a_limpiar);
		config_destroy(binario);
	}
	if (cantidadDeDumpeos > 1) {
		for (int j = 1; j <= cantidadDeDumpeos; j++) {
			char* temporal_a_limpiar = string_new();
			string_append_with_format(&temporal_a_limpiar, "%s/%d.tmp", ruta,
					j - 1);
			t_config* temporal = config_create(temporal_a_limpiar);
			char** bloques_a_limpiar = config_get_array_value(temporal,
					"BLOCKS");
			int i = 0;
			while (bloques_a_limpiar[i] != NULL) {
				bitarray_clean_bit(bitmap, atoi(bloques_a_limpiar[i]));
				char* bloque_a_limpiar = string_new();
				string_append_with_format(&bloque_a_limpiar,
						"rm %sBloques/%s.bin", configLFS->dirMontaje,
						bloques_a_limpiar[i]);
				system(bloque_a_limpiar);
				free(bloque_a_limpiar);
				i++;
			}
			free(bloques_a_limpiar);
			free(temporal_a_limpiar);
			config_destroy(temporal);
		}
	}
	free(bitmap);
	char* comando_a_ejecutar = string_new();

	string_append_with_format(&comando_a_ejecutar, "rm -rf %s", ruta);
	errorHandler = system(comando_a_ejecutar);
	if (!errorHandler)
		return errorAlBorrarDirectorio;
	free(comando_a_ejecutar);
	free(ruta);
	return todoJoya;
}

int Select(registro** reg, char* NOMBRE_TABLA, int KEY) {
	bool es_mayor(registro* unReg, registro* otroReg) {
		if (unReg->timestamp >= otroReg->timestamp)
			return true;
		return false;
	}
	char* ruta = direccionarTabla(NOMBRE_TABLA);
	int errorHandler = verificadorDeTabla(ruta);
	log_debug(logger, "Se ha solicitado la key %d de la tabla %s.", KEY,
			NOMBRE_TABLA);
	if (!errorHandler)
		return noExisteTabla;
	t_list* registros = list_create();
//	log_debug(logger, "me voy a fijar a memtable");
	t_list* memtable = selectEnMemtable(KEY, NOMBRE_TABLA);
	pthread_mutex_lock(&mem_table_mutex);
	if (memtable->elements_count > 0)
		list_add_all(registros, memtable);
	pthread_mutex_unlock(&mem_table_mutex);
//	log_debug(logger, "me voy a fijar a FS");
	registro* registry = malloc(sizeof(registro));
	if (!registry)
		return errorDeMalloc;
	errorHandler = SelectFS(ruta, KEY, &registry);
	if (errorHandler)
		return errorHandler;
	if (registry != NULL)
		list_add(registros, registry);
//	log_debug(logger, "me voy a fijar a Temp");
	t_list* temporales = SelectTemp(ruta, KEY);
	if (temporales->elements_count > 0)
		list_add_all(registros, temporales);
	free(ruta);
	if (registros->elements_count > 0) {
		list_sort(registros, (void*) &es_mayor);
		*reg = (registro*) list_get(registros, 0);
		log_debug(logger, "El value encontrado es: %s", (*reg)->value);
		errorHandler = todoJoya;
	} else
		errorHandler = noExisteKey;
	return errorHandler;
}

t_list* DESCRIBEespecifico(char* NOMBRE_TABLA) {
	char* ruta = string_new();
	log_debug(logger, "Ha solicitado un DESCRIBE de la tabla: %s",
			NOMBRE_TABLA);
	string_append_with_format(&ruta, "%sTablas/%s", configLFS->dirMontaje,
			NOMBRE_TABLA);
	t_list* metadatas = list_create();
	if (verificadorDeTabla(NOMBRE_TABLA)) {
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
	char* tablas_path = string_new();
	string_append(&tablas_path, configLFS->dirMontaje);
	string_append(&tablas_path, "Tablas/");
	tables_directory = opendir(tablas_path);
	if (tables_directory) {
		while ((a_directory = readdir(tables_directory)) != NULL) {
			if (strcmp(a_directory->d_name, ".")
					&& strcmp(a_directory->d_name, "..")) {
				char* a_table_path = string_new();
				char* table_name = malloc(strlen(a_directory->d_name));
				memcpy(table_name, a_directory->d_name,
						strlen(a_directory->d_name) + 1);
				string_append(&a_table_path, tablas_path);
				string_append(&a_table_path, table_name);
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
	char* mi_ruta = string_new();
	string_append(&mi_ruta, ruta);
	char* mi_metadata = "/metadata";
	string_append(&mi_ruta, mi_metadata);
	t_config* config_metadata = config_create(mi_ruta);
	Metadata* metadata = malloc(sizeof(Metadata));
	long tiempoDeCompactacion = config_get_long_value(config_metadata,
			"COMPACTION_TIME");
	metadata->tiempo_compactacion = tiempoDeCompactacion;
	char* consistencia = config_get_string_value(config_metadata,
			"CONSISTENCY");
	metadata->consistency = consistency_to_int(consistencia);
	config_destroy(config_metadata);
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
	pthread_mutex_unlock(&mem_table_mutex);
	if (tablaSelect != NULL)
		list_add_all(list_reg,
				list_filter(tablaSelect->registros, (void*) &es_registro));
	return list_reg; //devuelvo valor del select
}

int SelectFS(char* ruta, int KEY, registro** reg) {
	int particiones = buscarEnMetadata(ruta, "PARTITIONS");
	if (particiones < 0)
		return particionesInvalidas;
// ---- Calculo particion del KEY ----
	particiones = (KEY % particiones);
	char* rutaFS = string_new();
	string_append(&rutaFS, ruta);
	string_append(&rutaFS, ("/"));
	string_append(&rutaFS, string_itoa(particiones));
	string_append(&rutaFS, ".bin");
	t_config* particion = config_create(rutaFS);
	int size = config_get_int_value(particion, "SIZE");
	if (size > 0) {
		char** bloquesABuscar = config_get_array_value(particion, "BLOCKS");
		int i = 0;
		char* bloquesUnificados = string_new();
		while (bloquesABuscar[i] != NULL) {
			char* bloque = string_new();
			string_append(&bloque, configLFS->dirMontaje);
			string_append(&bloque, "Bloques/");
			string_append(&bloque, bloquesABuscar[i]);
			string_append(&bloque, ".bin");
			int fd = open(bloque, O_RDONLY, S_IRUSR | S_IWUSR);
			struct stat s;
			fstat(fd, &s);
			size = s.st_size;
			char* f = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);
			string_append(&bloquesUnificados, f);
			close(fd);
			munmap(f, size);
			free(bloque);
			i++;
		}
		int j = 0;
		char** registros = string_split(bloquesUnificados, "\n");
		while (registros[j] != NULL) {
			char** datos_registro = string_split(registros[j], ";");
			if (atoi(datos_registro[1]) == KEY) {
				(*reg)->timestamp = atol(datos_registro[0]);
				(*reg)->key = atoi(datos_registro[1]);
				(*reg)->value = malloc(strlen(datos_registro[2]) + 1);
				if ((*reg)->value == NULL)
					return errorDeMalloc;
				strcpy((*reg)->value, datos_registro[2]);
			}
			string_iterate_lines(datos_registro, (void*) free);
			free(datos_registro);
			j++;
		}
		string_iterate_lines(registros, (void*) free);
		free(registros);
		i++;
		free(bloquesUnificados);
		free(bloquesABuscar);

	} else
		*reg = NULL;
	free(rutaFS);
	config_destroy(particion);
	return todoJoya;
}

t_list* SelectTemp(char* ruta, int KEY) {
	t_list* listRegistros = list_create();
//	Ejecuto lo siguiento por cada temporal creado (uno por dumpeo)
	for (int aux = 1; aux <= cantidadDeDumpeos; aux++) {
		char* rutaTemporal = string_new();
		string_append(&rutaTemporal, ruta);
		string_append(&rutaTemporal, ("/"));
		string_append_with_format(&rutaTemporal, "%d", aux);
		string_append(&rutaTemporal, ".tmp");
		t_config* particion = config_create(rutaTemporal);
		int size = config_get_int_value(particion, "SIZE");
		if (size > 0) {
			char** bloquesABuscar = config_get_array_value(particion, "BLOCKS");
			int i = 0;
//		Unifico la informacion de todos los bloques en los que esta dividido el archivo .tmp
			char* bloquesUnificados = string_new();
			while (bloquesABuscar[i] != NULL) {
				char* bloque = string_new();
				string_append(&bloque, configLFS->dirMontaje);
				string_append(&bloque, "Bloques/");
				string_append(&bloque, bloquesABuscar[i]);
				string_append(&bloque, ".bin");
				int fd = open(bloque, O_RDONLY, S_IRUSR | S_IWUSR);
				struct stat s;
				fstat(fd, &s);
				size = s.st_size;
				char* f = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);
				string_append(&bloquesUnificados, f);
				close(fd);
				free(bloque);
				i++;
			}

			int j = 0;
			char** registros = string_split(bloquesUnificados, "\n");
//		Recorro y divido los datos unificado del archivos temporal, almacenando solo las keys que coincidan con la solicitada
			while (registros[j] != NULL) {
				registro* registro = malloc(sizeof(registro));
				char** datos_registro = string_split(registros[j], ";");
				if (atoi(datos_registro[1]) == KEY) {
					registro->timestamp = atoi(datos_registro[0]);
					registro->key = atoi(datos_registro[1]);
					registro->value = malloc(strlen(datos_registro[2]) + 1);
					strcpy(registro->value, datos_registro[2]);
					list_add(listRegistros, registro);
				}
				string_iterate_lines(datos_registro, (void*) free);
				free(datos_registro);
				j++;
			}
			string_iterate_lines(registros, (void*) free);
			free(registros);
			free(bloquesABuscar);
			free(bloquesUnificados);
		}
		free(rutaTemporal);
		config_destroy(particion);
	}
	return listRegistros;
}

// ---------------TABLAS-----------------

int crearMetadata(char* NOMBRE_TABLA, char* TIPO_CONSISTENCIA,
		int NUMERO_PARTICIONES, int COMPACTATION_TIME) {
	char aux[strlen(NOMBRE_TABLA) + 10];
//Opitimizacion1A: Se puede hace que fp se pase por parámetro para no abrirlo en crearMetadata y crearBIN
	FILE *fp;
	snprintf(aux, sizeof(aux), "%s/metadata", NOMBRE_TABLA);
	fp = fopen(aux, "w+");
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
	NUMERO_PARTICIONES--;
	while (NUMERO_PARTICIONES >= 0) {
		char* nuevaParticion = string_new();
		string_append(&nuevaParticion, NOMBRE_TABLA);
		string_append(&nuevaParticion, "/");
		string_append_with_format(&nuevaParticion, "%d", NUMERO_PARTICIONES);
		string_append(&nuevaParticion, ".bin");
		int fd = creat(nuevaParticion, (mode_t) 0600);
		close(fd);
		t_config* particion = config_create(nuevaParticion);
		char* bloques = string_new();
		off_t bitLibre = obtener_bit_libre();
		string_append_with_format(&bloques, "[%d]", bitLibre);
		config_set_value(particion, "BLOCKS", bloques);
		config_set_value(particion, "SIZE", "0");
		config_save(particion);
		config_destroy(particion);
		char* nuevoBloque = string_new();
		string_append_with_format(&nuevoBloque, "%sBloques/%d.bin",
				configLFS->dirMontaje, bitLibre);
		int fd2 = creat(nuevoBloque, (mode_t) 0600);
		close(fd2);
		free(bloques);
		NUMERO_PARTICIONES--;
		free(nuevaParticion);
	}
}

int verificadorDeTabla(char* tabla) {
	char* ruta = direccionarTabla(tabla);
	int status = 1;
	DIR *dirp;
	dirp = opendir(ruta);
	if (dirp == NULL) {
		status = 0;
	}
	closedir(dirp);
	free(ruta);
	return status;
}

int buscarEnMetadata(char* NOMBRE_TABLA, char* objetivo) {
	char* ruta = string_new();
	string_append(&ruta, NOMBRE_TABLA);
	string_append(&ruta, "/metadata");
	t_config* metadata = config_create(ruta);
	int obj = config_get_int_value(metadata, objetivo);
	config_destroy(metadata);
	free(ruta);
	return obj;
}

t_bitarray* levantarBitmap() {
	char* ruta = string_new();
	string_append(&ruta, configLFS->dirMontaje);
	string_append(&ruta, "Metadata/Bitmap.bin");
	int size = configMetadata->blocks / 8;
	if (configMetadata->blocks % 8 != 0)
		size++;
	int fd = open(ruta, O_RDWR, S_IRUSR | S_IWUSR);
	int errorHandler = ftruncate(fd, size);
	if ((errorHandler < 0) || (fd < 0))
		log_debug(logger, "Error al levantar bitmap");
	char* bitarray = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd,
			0);
	t_bitarray* structBitarray = bitarray_create_with_mode(bitarray, size,
			MSB_FIRST);

	free(ruta);
	return structBitarray;
}

off_t obtener_bit_libre() {
	t_bitarray* bitmap = levantarBitmap();
	off_t bit_index = 0;
//leer el primero libre y ponerle 1
	while (bitarray_test_bit(bitmap, bit_index))
		bit_index++;
	bitarray_set_bit(bitmap, bit_index);
	return bit_index;
}

// ---------------DUMPEO-----------------

int dumpeoMemoria() {
	void paraDumpearTabla(t_tabla* tabla) {
		char* tablaParaDumpeo = string_new();
		string_append_with_format(&tablaParaDumpeo, "%sTablas/%s/%d.tmp",
				configLFS->dirMontaje, tabla->nombreTabla, cantidadDeDumpeos);
		int fd = creat(tablaParaDumpeo, (mode_t) 0600);
		close(fd);
		dumpearTabla(tabla->registros, tablaParaDumpeo);
	}
	void destruidor(t_tabla *tabla) {
		free(tabla->nombreTabla);

		void destruidor2(registro* reg) {
			free(reg->value);
			free(reg);
		}

		list_destroy_and_destroy_elements(tabla->registros,
				(void*) destruidor2);
	}
	if (list_is_empty(memtable))
	{
//		TODO: poner define para memtable vacia
		return 1;
	}pthread_mutex_lock(&mem_table_mutex);
	t_list* dumpMem=list_duplicate(memtable);
	pthread_mutex_unlock(&mem_table_mutex);


	list_iterate(dumpMem, (void*) &paraDumpearTabla);
	list_clean_and_destroy_elements(dumpMem, (void*) destruidor);

	cantidadDeDumpeos++;
	log_debug(logger, "termine el dump");
	return 0;
}

void dumpearTabla(t_list* registros, char* ruta) {
	off_t bit_index = obtener_bit_libre();
	t_config* tmp = config_create(ruta);
	log_debug(logger, "Entre a dumpearTabla");
	char* bloque = string_new();
	string_append_with_format(&bloque, "[%d]", bit_index);
	config_set_value(tmp, "SIZE", "0");
	config_set_value(tmp, "BLOCKS", bloque);
	log_debug(logger, "El bloque libre a escribir es: %s", bloque);
	config_save(tmp);
	char* bloqueDumpeo = string_new();
	string_append_with_format(&bloqueDumpeo, "%sBloques/%d.bin",
			configLFS->dirMontaje, bit_index);

	int fd2 = open(bloqueDumpeo, O_RDWR | O_CREAT | O_TRUNC, (mode_t) 0600);
	char* registroParaEscribir = string_new();
	void dumpearRegistros(registro* UnRegistro) {
		log_debug(logger, "Entre a dumpearRegistros");
		log_debug(logger, "voy a dumpear unRegistro con: %d;%d;%s\n",
				UnRegistro->timestamp, UnRegistro->key, UnRegistro->value);
		string_append_with_format(&registroParaEscribir, "%d;%d;%s\n",
				UnRegistro->timestamp, UnRegistro->key, UnRegistro->value);
		log_debug(logger, "voy a escribir esto: %s", registroParaEscribir);
	}
	log_debug(logger, "Arranco con el list_iterate");
	list_iterate(registros, (void*) &dumpearRegistros);
	bajarAMemoria(&fd2, registroParaEscribir, tmp);
	close(fd2);
	config_destroy(tmp);
	free(registroParaEscribir);
}

void bajarAMemoria(int* fd2, char* registroParaEscribir, t_config* tmp) {
//TODO: levantar un struct metadata global del metadata general
	size_t textsize = strlen(registroParaEscribir) + 1; // + \0 null character
	int size = config_get_int_value(tmp, "SIZE");
	char* strsize = string_new();
	string_append_with_format(&strsize, "%d", (textsize - 1));
	log_debug(logger, strsize);
	config_set_value(tmp, "SIZE", strsize);
	config_save(tmp);
	free(strsize);
	char* map = mapearBloque(*fd2, textsize);
	int i = 0;
	int posmap = 0;
	while (i < textsize) {
		if (posmap < configMetadata->blockSize)
			map[posmap] = registroParaEscribir[i];
		else {
//			TODO: armar lo que sigue como subfuncion, esta en dumpeo memoria repetido
			off_t bit_index = obtener_bit_libre();
			actualizarBloquesEnTemporal(tmp, bit_index);
			char* bloqueDumpeoNuevo = string_new();
			string_append(&bloqueDumpeoNuevo, configLFS->dirMontaje);
			string_append(&bloqueDumpeoNuevo, "Bloques/");
			string_append_with_format(&bloqueDumpeoNuevo, "%d", bit_index);
			string_append(&bloqueDumpeoNuevo, ".bin");
			*fd2 = open(bloqueDumpeoNuevo, O_RDWR | O_CREAT | O_TRUNC,
					(mode_t) 0600);
//			Hasta aca
			map = mapearBloque(*fd2, textsize - i);
			posmap = 0;
			map[posmap] = registroParaEscribir[i];
		}
		i++;
		posmap++;
	}
	msync(map, textsize, MS_SYNC);
	munmap(map, textsize);
}

char* mapearBloque(int fd2, size_t textsize) {
	lseek(fd2, textsize, SEEK_SET);
	int errorHandler = write(fd2, "", 1);
	if (errorHandler < 0)
		log_debug(logger, "error al mapear el bloque");
	char *map = mmap(0, textsize, PROT_READ | PROT_WRITE, MAP_SHARED, fd2, 0);
	return map;
}

void actualizarBloquesEnTemporal(t_config* tmp, off_t bloque) {
	char* bloques = config_get_string_value(tmp, "BLOCKS");
	bloques = string_substring_until(bloques, string_length(bloques) - 1);
	string_append_with_format(&bloques, ",%d]", bloque);
	config_set_value(tmp, "BLOCKS", bloques);
	config_save(tmp);
	log_debug(logger, "grabe los bloques");
	free(bloques);
}

//
int dumpeosCompactacion = 0;
int compactacion(char* nombre_tabla) {
//TODO: RENOMBRAR TODOS//SEMAFOROS!!!!!!!!!

	for (int j = 0; j < cantidadDeDumpeos; j++) {
		char* rutaARenombrar = string_new();
		string_append_with_format(&rutaARenombrar,
				"mv %sTablas/%s/%d.tmp %sTablas/%s/%d.tmpc",
				configLFS->dirMontaje, nombre_tabla, j, configLFS->dirMontaje,
				nombre_tabla, j);
		system(rutaARenombrar);
		free(rutaARenombrar);
		dumpeosCompactacion++;
	}

	cantidadDeDumpeos = 0;

	char* temporales = string_new();
	char* binarios = string_new();
	t_list* lista_bin = list_create();
	t_list* lista_Temp = list_create();
	t_list* listaCompactada = list_create();
	int error = obtener_temporales(nombre_tabla, &temporales);
	int errorbinario = levantarbinarios(nombre_tabla, &binarios);
	if (!string_is_empty(temporales))
		crearListaRegistros(&temporales, lista_Temp);
	if (!string_is_empty(binarios))
		crearListaRegistros(&binarios, lista_bin);
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
			if (encontrado->timestamp < reg->timestamp)
				list_add(listaCompactada, reg);
			else
				list_add(listaCompactada, encontrado);
		} else {

			registro* encontrado = list_find(lista_Temp,
					(void*) &hay_registro_mayor);
			if (encontrado == NULL) {
				log_debug(logger, "Llego un registro: %s", reg->value);
				list_add(listaCompactada, reg);
			}

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
	list_iterate(lista_bin, (void*) &agregarFaltantes);
	list_iterate(lista_Temp, (void*) &compactar);

	char* tabla = string_new();
	string_append_with_format(&tabla, "%sTablas/%s/metadata",
			configLFS->dirMontaje, nombre_tabla);
	t_config* t = config_create(tabla);
	int part = config_get_int_value(t, "PARTITIONS");
	liberar_bloques(nombre_tabla, part);
	log_debug(logger, "%d", listaCompactada->elements_count);
	guardar_en_disco(listaCompactada, part, nombre_tabla);

	dumpeosCompactacion = 0;
	return 0;
}

void liberar_bloques(char* nombre_tabla, int cantParticiones) {

	t_bitarray* bitmap = levantarBitmap();
	log_debug(logger, "Empiezo con tmp");
	///////////////////liberoTMPC
	for (int aux = 1; aux <= dumpeosCompactacion; aux++) {
		char* rutaTemporal = string_new();
		string_append_with_format(&rutaTemporal, "%sTablas/%s/%d.tmpc",
				configLFS->dirMontaje, nombre_tabla, (aux - 1));

		t_config* particion = config_create(rutaTemporal);
		int size = config_get_int_value(particion, "SIZE");

		char** bloquesABuscar = config_get_array_value(particion, "BLOCKS");
		int r = 0;

		//		Unifico la informacion de todos los bloques en los que esta dividido el archivo .tmp
		while (bloquesABuscar[r] != NULL) {
			bitarray_clean_bit(bitmap, atoi(bloquesABuscar[r]));
			log_debug(logger, "Empiezo con tmp");
			char* bloque_a_limpiar = string_new();
			string_append_with_format(&bloque_a_limpiar, "rm %sBloques/%s.bin",
					configLFS->dirMontaje, bloquesABuscar[r]);
			log_debug(logger, "Empiezo con tmp");
			system(bloque_a_limpiar);
			free(bloque_a_limpiar);
			r++;
		}

		free(bloquesABuscar);

		free(rutaTemporal);
		config_destroy(particion);

		char* rutaARenombrar = string_new();

		string_append_with_format(&rutaARenombrar, "rm %sTablas/%s/%d.tmpc",
				configLFS->dirMontaje, nombre_tabla, aux - 1);
		log_debug(logger, "voy a borrar");

		system(rutaARenombrar);
		log_debug(logger, " borre");
		free(rutaARenombrar);

	}

	/////////////////////liberoBIN
	for (int i = 0; i <= cantParticiones - 1; i++) {

		char* tablaParaDumpeo = string_new();
		string_append_with_format(&tablaParaDumpeo, "%sTablas/%s/%d.bin",
				configLFS->dirMontaje, nombre_tabla, i);
		t_config* tmp = config_create(tablaParaDumpeo);
		free(tablaParaDumpeo);
		char** bloques = config_get_array_value(tmp, "BLOCKS");
		int s = 0;
		while (bloques[s] != NULL) {

			char* bloque = string_new();

			bitarray_clean_bit(bitmap, atoi(bloques[s]));
			char* bloque_a_limpiar = string_new();
			string_append_with_format(&bloque_a_limpiar, "rm %sBloques/%s.bin",
					configLFS->dirMontaje, bloques[s]);
			log_debug(logger, "voy a borrar");
			system(bloque_a_limpiar);
			free(bloque_a_limpiar);
			s++;
		}

	}

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
		t_list* listaParticionada = list_filter(duplicada,
				(void*) &filtrarPorParticion);
		char* tablaParaDumpeo = string_new();
		string_append_with_format(&tablaParaDumpeo, "%sTablas/%s/%d.bin",
				configLFS->dirMontaje, nombre_tabla, i);

		char* registroParaEscribir = string_new();
		void dumpearRegistros(registro* UnRegistro) {
			string_append_with_format(&registroParaEscribir, "%d;%d;%s\n",
					UnRegistro->timestamp, UnRegistro->key, UnRegistro->value);
		}

		list_iterate(listaParticionada, (void*) &dumpearRegistros);
		t_config* tmp = config_create(tablaParaDumpeo);

		char* bloque = string_new();

//		if (!list_is_empty(listaParticionada)) {
		off_t bit = obtener_bit_libre();
		char* nuevoBloque = string_new();
		string_append_with_format(&nuevoBloque, "[%d]", bit);
		config_set_value(tmp, "BLOCKS", nuevoBloque);

		string_append_with_format(&bloque, "%sBloques/%d.bin",
				configLFS->dirMontaje, bit);

		int fd2 = open(bloque, O_RDWR | O_CREAT | O_TRUNC, (mode_t) 0600);
		log_debug(logger, "bajo a memoria en esta ruta %s", bloque);
		bajarAMemoria(&fd2, registroParaEscribir, tmp);
		close(fd2);
//		}
		free(registroParaEscribir);

	}
	list_destroy_and_destroy_elements(binarios, free);
}

void crearListaRegistros(char** string, t_list* lista) {
	int j = 0;

	if (*string != "") {
		char** registros = string_split(*string, "\n");
		char** datos_registro;
//		Recorro y divido los datos unificado del archivos temporal, almacenando solo las keys que coincidan con la solicitada
		while (registros[j] != NULL) {
			registro* reg = malloc(sizeof(registro));
			datos_registro = string_split(registros[j], ";");
			reg->timestamp = atoi(datos_registro[0]);
			reg->key = atoi(datos_registro[1]);
			reg->value = malloc(strlen(datos_registro[2]) + 1);
			strcpy(reg->value, datos_registro[2]);
			log_debug(logger, "%d;%d;%s", reg->timestamp, reg->key, reg->value);
			list_add(lista, reg);
			j++;
		}

		string_iterate_lines(datos_registro, (void*) free);
		free(datos_registro);
	}
}

int levantarbinarios(char* nombre_tabla, char** bloquesUnificados) {

	char* rutametadata = string_new();
	string_append_with_format(&rutametadata, "%sTablas/%s/metadata",
			configLFS->dirMontaje, nombre_tabla);
	t_config* metadataTabla = config_create(rutametadata);
	int cantParticiones =
			(config_get_int_value(metadataTabla, "PARTITIONS") - 1);
	for (int aux = 0; aux <= cantParticiones; aux++) {

		char* rutaBinario = string_new();
		string_append_with_format(&rutaBinario, "%sTablas/%s/%d.bin",
				configLFS->dirMontaje, nombre_tabla, aux);
		t_config* particion = config_create(rutaBinario);

		int size = config_get_int_value(particion, "SIZE");
		log_debug(logger, "levante bin con size %d", size);
		if (size > 0) {
			char** bloquesABuscar = config_get_array_value(particion, "BLOCKS");
			int i = 0;

			//		Unifico la informacion de todos los bloques en los que esta dividido el archivo .tmp
			while (bloquesABuscar[i] != NULL) {

				char* bloque = string_new();
				string_append_with_format(&bloque, "%sBloques/%s.bin",
						configLFS->dirMontaje, bloquesABuscar[i]);
				int fd = open(bloque, O_RDONLY, S_IRUSR | S_IWUSR);

				struct stat s;
				fstat(fd, &s);
				size = s.st_size;
				char* f = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);

				string_append(bloquesUnificados, f);

				close(fd);
				free(bloque);
				i++;
			}
			free(bloquesABuscar);
		} else

			free(rutaBinario);
		config_destroy(particion);
	}
	log_debug(logger, "en bin encontre %s", *bloquesUnificados);
	return todoJoya;
}

int obtener_temporales(char* nombre_tabla, char** bloquesUnificados) {

	for (int aux = 1; aux <= dumpeosCompactacion; aux++) {
		char* rutaTemporal = string_new();
		string_append_with_format(&rutaTemporal, "%sTablas/%s/%d.tmpc",
				configLFS->dirMontaje, nombre_tabla, (aux - 1));

		t_config* particion = config_create(rutaTemporal);
		int size = config_get_int_value(particion, "SIZE");

		if (size > 0) {
			char** bloquesABuscar = config_get_array_value(particion, "BLOCKS");
			int i = 0;

			//		Unifico la informacion de todos los bloques en los que esta dividido el archivo .tmp
			while (bloquesABuscar[i] != NULL) {
				char* bloque = string_new();
				string_append(&bloque, configLFS->dirMontaje);
				string_append(&bloque, "Bloques/");
				string_append(&bloque, bloquesABuscar[i]);
				string_append(&bloque, ".bin");
				int fd = open(bloque, O_RDONLY, S_IRUSR | S_IWUSR);
				struct stat s;
				fstat(fd, &s);
				size = s.st_size;
				char* f = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);
				string_append(bloquesUnificados, f);

				close(fd);
				free(bloque);
				i++;
			}
			log_debug(logger, "en tmp encontre %s", *bloquesUnificados);
			free(bloquesABuscar);
		}
		free(rutaTemporal);
		config_destroy(particion);

	}
	return todoJoya;
}

///

// ---------------OTROS-----------------

char* direccionarTabla(char* tabla) {
	char* ruta = string_new();
	string_append(&ruta, configLFS->dirMontaje);
	string_append(&ruta, "Tablas/");
	string_append(&ruta, tabla);
	return ruta;
}

void crearBitmapNuestro() {

	int size = configMetadata->blocks / 8;
	if (configMetadata->blocks % 8 != 0)
		size++;
	char* bitarray = calloc(configMetadata->blocks / 8, sizeof(char));
	t_bitarray* structBitarray = bitarray_create_with_mode(bitarray, size,
			MSB_FIRST);
	for (int i = 0; i < configMetadata->blocks; i++) {
		if (i == 10)
			bitarray_set_bit(structBitarray, i);
		else
			bitarray_clean_bit(structBitarray, i);
	}
	char* path = string_from_format("%s/Metadata/Bitmap.bin",
			configLFS->dirMontaje);
	FILE* file = fopen(path, "wb+");
	fwrite(structBitarray->bitarray, sizeof(char), configMetadata->blocks / 8,
			file);
	fclose(file);
	bitarray_destroy(structBitarray);
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
	list_destroy_and_destroy_elements(memtable, free);
	free(configLFS->dirMontaje);
	raise(SIGTERM);
}

void innotificar() {
	log_debug(logger, "entre al hilo");
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

