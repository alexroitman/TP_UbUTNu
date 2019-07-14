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

// ---------------MAIN-----------------

int main(void) {
	configLFS = malloc(sizeof(config_FS));
	configMetadata = malloc(sizeof(metadataFS));
	pthread_t hiloSocket;
	pthread_t hiloCompactacion;
	pthread_t hiloConsola;
	pthread_t hiloInnotify;

	// ---------------Inicializacion-----------------
	memtable = inicializarMemtable();
	logger = iniciar_logger();
	levantarMetadataFS();
	log_debug(logger, "levante configLFS");
	levantarConfigLFS();
	log_debug(logger, "levante configLFS");

	// ---------------Pruebas con bitmap propio-----------------
	crearBitmapNuestro();
	log_debug(logger, "cree Bitmap");

	// Escribir inicio en consola

	// Abro hilo de consola
	paramsConsola = malloc(sizeof(tHiloConsola));
	paramsConsola->header = NIL;
	pthread_create(&hiloConsola, NULL, (void*) abrirHiloConsola, paramsConsola);

	// Abro hilo de compactacion
//	pthread_create(&hiloCompactacion, NULL, (void*) abrirHiloCompactacion,NULL);

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
	configMetadata->magicNumber = config_get_string_value(aux, "MAGIC_NUMBER");
	config_destroy(aux);
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
	log_debug(logger,"%d",configLFS->retardo);
	configLFS->tamanioValue = config_get_int_value(aux, "TAMANIO_VALUE");
	configLFS->tiempoDumpeo = config_get_int_value(aux, "TIEMPO_DUMP");
	config_destroy(aux);
}

void receptorDeSockets(int* socket) {
	type header;
	int errorHandler;
	while (1) {
		int bytes = recv(*socket, &header, sizeof(type), MSG_WAITALL);
		usleep((configLFS->retardo)*1000);
		if (0 > bytes) {
			close(*socket);
			log_debug(logger, "cerre los socket");
			return;
		}
		log_debug(logger, "RECIBO HEADER %d", header);
		switch (header) {

		case SELECT:
			log_debug(logger, "Comando SELECT recibido");
			tSelect* packSelect = malloc(sizeof(tSelect));
			if (!packSelect)
				logeoDeErroresLFS(errorDeMalloc, logger);
			else {
				desSerializarSelect(packSelect, *socket);
				//TODO: handlear error de desSerializarSelect
				packSelect->type = header;
				registro* reg = malloc(sizeof(registro));
				if (!reg)
					logeoDeErroresLFS(errorDeMalloc, logger);
				//TODO: handlear error de Y hacer failfast
				errorHandler = Select(reg, packSelect->nombre_tabla,
						packSelect->key);
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
					char* registroSerializado = serializarRegistro(registro);
					//TODO: handlear error de serializarRegistro
					enviarPaquete(*socket, registroSerializado,
							registro->length);
					//TODO: handlear error de enviarPaquete
					free(registroSerializado);
				}
				free(registro);
				free(reg);
			}
			free(packSelect);
			break;

		case INSERT:
			log_debug(logger, "Comando INSERT recibido");
			tInsert* packInsert = malloc(sizeof(tInsert));
			if (!packInsert)
				logeoDeErroresLFS(errorDeMalloc, logger);
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
			if (!packCreate || !metadata)
				logeoDeErroresLFS(errorDeMalloc, logger);
			t_list* metadatas = (t_list*) Describe();
			if (metadatas != NULL) {
				t_describe* describe = malloc(sizeof(t_describe));
				if (!describe)
					logeoDeErroresLFS(errorDeMalloc, logger);
				int cantidad_de_tablas = metadatas->elements_count;
				describe->cant_tablas = cantidad_de_tablas;
				describe->tablas = malloc(cantidad_de_tablas * sizeof(t_metadata));
				for (int i = 0; i < cantidad_de_tablas; i++) {
					Metadata* a_metadata = (Metadata*) list_get(metadatas, i);
					t_metadata meta;
					meta.consistencia = a_metadata->consistency;
					strcpy(meta.nombre_tabla, a_metadata->nombre_tabla);
					describe->tablas[i] = meta;
				}
				char* serializedPackage;
				serializedPackage = serializarDescribe_Response(describe);
				char* cantidad_de_tablas_string = string_itoa(cantidad_de_tablas);
				log_debug(logger, serializedPackage);
				log_debug(logger, cantidad_de_tablas_string);
				free(cantidad_de_tablas_string);
				int b = send(*socket, serializedPackage, cantidad_de_tablas * sizeof(t_metadata) + sizeof(describe->cant_tablas), 0);
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
			/*	else {
			 desSerializarDescribe(packDescribe, socket_cli);
			 if (string_equals_ignore_case(packDescribe->nombre_tabla, " "))
			 errorHandler = DESCRIBEmultiple(metadata);
			 else
			 errorHandler = DESCRIBEespecifico(packDescribe->nombre_tabla, metadata);
			 }*/
			free(packDescribe);
			free(metadata);
			break;

		case DROP:/*
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
		int errorHandler;
		bool leyoConsola = true;
		usleep((configLFS->retardo)*1000);
		type header = stringToHeader(tempSplit[0]);
		switch (header) {
		case SELECT:
			log_debug(logger, "Recibi un SELECT por consola");
			tSelect* packSelectConsola;
			registro* reg = malloc(sizeof(registro));
			if (!reg)
				logeoDeErroresLFS(errorDeMalloc, logger);
			packSelectConsola = malloc(sizeof(tSelect));
			cargarPaqueteSelect(packSelectConsola, paramsConsola->consulta);
			errorHandler = Select(reg, packSelectConsola->nombre_tabla,
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

			/* log_debug(logger, "Comando DROP recibido");
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
			free(packDropConsola);

			break;
		case DESCRIBE:

			log_debug(logger, "Recibi un DESCRIBE por consola");
			t_list* lista;
			tDescribe* packDescribeConsola = malloc(sizeof(tDescribe));
			cargarPaqueteDescribe(packDescribeConsola, paramsConsola->consulta);
			log_debug(logger, packDescribeConsola->nombre_tabla);

			if (!tempSplit[1]) {
				log_debug(logger, "entre al global");
				lista = Describe();
			}

			else
				lista = DESCRIBEespecifico(packDescribeConsola->nombre_tabla);

			if(lista){
				for (int i = 0; i < lista->elements_count; i++) {
					Metadata* m = list_get(lista, i);
					log_debug(logger, "Voy a logear");
					log_debug(logger, "%d", m->consistency);
				}
			}
			free(packDescribeConsola->nombre_tabla);
			free(packDescribeConsola);
			break;
		case NIL:
			log_error(logger, "No entendi la consulta");
			break;

		}

		free(tempSplit[0]);
		free(tempSplit[1]);
		free(tempSplit);

	}
}

void abrirHiloSockets() {
	log_debug(logger, "entre al primer hilo");
//	int errorHandler;
	pthread_t* threadPrincipal;
	socket_sv = levantarServidor(configLFS->puerto);
	if (socket_sv < 0)
		logeoDeErroresLFS(noLevantoServidor, logger);
	socket_cli = aceptarCliente(socket_sv);
	send(socket_cli, &configLFS->tamanioValue, 4, 0);
	pthread_create(&threadPrincipal, NULL, (void*) receptorDeSockets,
			&socket_cli);

	while (1) {
		log_debug(logger, "entre al while para crear una nueva memoria");
		int socketNuevo;
		pthread_t* threadNuevo;
		socketNuevo = aceptarCliente(socket_sv);
		send(socketNuevo, &configLFS->tamanioValue, 4, 0);
		pthread_create(&threadNuevo, NULL, (void*) receptorDeSockets,
				&socketNuevo);
		log_debug(logger, "levante cliente");

		//		if (errorHandler < 0)
		//			logeoDeErroresLFS(errorTamanioValue, logger);
	}
}

type stringToHeader(char* str) {
	if (!strcmp(str, "SELECT")) {
		return SELECT;
	}
	if (!strcmp(str, "INSERT")) {
		return INSERT;
	}
	if (!strcmp(str, "CREATE")) {
		return CREATE;
	}
	if (!strcmp(str, "DROP")) {
		return DROP;
	}
	if (!strcmp(str, "DESCRIBE")) {
		return DESCRIBE;
	}
	if (!strcmp(str, "DESCRIBE\n")) {
		return DESCRIBE;
	}
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
				char* a_table_path = string_new();
				char* table_name = malloc(strlen(a_directory->d_name));
				memcpy(table_name, a_directory->d_name,
						strlen(a_directory->d_name) + 1);
				pthread_t* threadComp;
				pthread_create(&threadComp, NULL, (void*) hiloCompactar,
						&table_name);
				free(table_name);

			}
		}
		closedir(tables_directory);
	}
	free(tablas_path);
}
void hiloCompactar(char*nombre_tabla) {

	while (1) {
		if (!verificadorDeTabla(nombre_tabla)) {
			pthread_exit(NULL);
		}
		char* tablas_path = string_new();
		string_append(&tablas_path, configLFS->dirMontaje);
		string_append(&tablas_path, "Tablas/");
		string_append(&tablas_path, nombre_tabla);
		string_append(&tablas_path, "/metadata");
		t_config* configComp = config_create(tablas_path);
		int retardoCompac = config_get_int_value(configComp, "COMPACTION_TIME");

		usleep(retardoCompac * 1000);
		if (!verificadorDeTabla(nombre_tabla)) {
			pthread_exit(NULL);
		}
		compactacion(nombre_tabla);

	}
}
// ---------------MEMTABLE-----------------

t_list* inicializarMemtable() {
	return list_create();
}

int insertarEnMemtable(tInsert *packinsert) {
	int errorHandler;
	errorHandler = verificadorDeTabla(packinsert->nombre_tabla);
// Cheque que la tabla este creada
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
	t_tabla* tabla = (t_tabla*) list_find(memtable, (void*) &es_tabla);
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
	t_tabla* tabla_encontrada = (t_tabla*) list_find(memtable,
			(void*) &es_tabla);
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
	pthread_t* threadComp;
	pthread_create(&threadComp, NULL, (void*) hiloCompactar, &NOMBRE_TABLA);
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

	char* ruta = direccionarTabla(NOMBRE_TABLA);

// Eliminar de la memtable
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
					j);
			t_config* temporal = config_create(temporal_a_limpiar);
			char** bloques_a_limpiar = config_get_array_value(temporal,
					"BLOCKS");
			int i = 0;
			while (bloques_a_limpiar[i] != NULL) {
				bitarray_clean_bit(bitmap, atoi(bloques_a_limpiar[i]));
				//TODO: ELIMINAR EL BLOQUE PROPIAMENTE DICHO
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

int Select(registro* reg, char* NOMBRE_TABLA, int KEY) {
	bool es_mayor(registro* unReg, registro* otroReg) {
		if (unReg->timestamp >= otroReg->timestamp)
			return true;
		return false;
	}
	char* ruta = direccionarTabla(NOMBRE_TABLA);
	int errorHandler = verificadorDeTabla(ruta);
	if (!errorHandler)
		return noExisteTabla;
	t_list* registros = list_create();
	log_debug(logger, "me voy a fijar a memtable");
	list_add_all(registros, selectEnMemtable(KEY, NOMBRE_TABLA));
	log_debug(logger, "me voy a fijar a FS");
	registro* registro = malloc(sizeof(registro));
	if (!registro)
		return errorDeMalloc;
	errorHandler = SelectFS(ruta, KEY, registro);
	if (errorHandler)
		return errorHandler;
	if (registro != NULL)
		list_add(registros, registro);
	log_debug(logger, "me voy a fijar a Temp");
	t_list* temporales = SelectTemp(ruta, KEY);
	log_debug(logger, "VOLVI DEL TEMP");
	if (temporales != NULL)
		list_add_all(registros, temporales);
	list_sort(registros, (void*) &es_mayor);
	reg = list_get(registros, 0);
	log_debug(logger, "encontre %s", reg->value);
	free(ruta);
	if (!reg)
		return noExisteKey;
	return todoJoya;
}

t_list* DESCRIBEespecifico(char* NOMBRE_TABLA) {
	char* ruta = string_new();
	string_append_with_format(&ruta, "%sTablas/%s", configLFS->dirMontaje,NOMBRE_TABLA);
	t_list* metadatas = list_create();
	if(verificadorDeTabla(NOMBRE_TABLA)){
		log_debug(logger, "CREE LISTA");
		Metadata* metadata = obtener_metadata(ruta);
		log_debug(logger, "VOY A COPIAR");
		strcpy(metadata->nombre_tabla, NOMBRE_TABLA);
		log_debug(logger, "COPIE");
		list_add(metadatas, metadata);
	}
	else{
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
	log_debug(logger, "TABLAS_PATH: %s", tablas_path);
	tables_directory = opendir(tablas_path);
	if (tables_directory) {
		while ((a_directory = readdir(tables_directory)) != NULL) {
			if (strcmp(a_directory->d_name, ".") && strcmp(a_directory->d_name, "..")) {
				char* a_table_path = string_new();
				char* table_name = malloc(strlen(a_directory->d_name));
				memcpy(table_name, a_directory->d_name, strlen(a_directory->d_name) + 1);
				string_append(&a_table_path, tablas_path);
				string_append(&a_table_path, table_name);
				log_debug(logger, "%s", a_table_path);
				Metadata* metadata = obtener_metadata(a_table_path);
				strcpy(metadata->nombre_tabla, table_name);
				list_add(metadatas, metadata);
				free(table_name);
				free(a_table_path);
			}
		}
		if(!metadatas->elements_count)
			metadatas = NULL;
		closedir(tables_directory);
	}
	else{
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
	log_debug(logger, mi_ruta);
	t_config* config_metadata = config_create(mi_ruta);
	Metadata* metadata = malloc(sizeof(Metadata));
	log_debug(logger, "Voy a levantar data", metadata->consistency);
	long tiempoDeCompactacion = config_get_long_value(config_metadata,
			"COMPACTION_TIME");
	metadata->tiempo_compactacion = tiempoDeCompactacion;
	char* consistencia = config_get_string_value(config_metadata,
			"CONSISTENCY");
	log_debug(logger, consistencia);
	metadata->consistency = consistency_to_int(consistencia);
	log_debug(logger, "%d", metadata->consistency);
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
	tablaSelect = list_find(memtable, (void*) &es_tabla);
	if (tablaSelect != NULL)
		list_add_all(list_reg,
				list_filter(tablaSelect->registros, (void*) &es_registro));
	return list_reg; //devuelvo valor del select
}

int SelectFS(char* ruta, int KEY, registro* registro) {
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
	log_debug(logger, rutaFS);
	t_config* particion = config_create(rutaFS);
	int size = config_get_int_value(particion, "SIZE");
	log_debug(logger, "%d", size);
	if (size > 0) {
		char** bloquesABuscar = config_get_array_value(particion, "BLOCKS");
		int i = 0;
		log_debug(logger, "Voy a entrar al while");

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
		log_debug(logger, "ENTRO A WHILE");
		while (registros[j] != NULL) {
			char** datos_registro = string_split(registros[j], ";");
			if (atoi(datos_registro[1]) == KEY) {
				//	free(registro->value);
				registro->timestamp = atol(datos_registro[0]);
				registro->key = atoi(datos_registro[1]);
				registro->value = malloc(strlen(datos_registro[2]) + 1);
				if (!registro->value)
					return errorDeMalloc;
				strcpy(registro->value, datos_registro[2]);
			}
			string_iterate_lines(datos_registro, (void*) free);
			free(datos_registro);
			j++;
		}
		log_debug(logger, "SALGO DE WHILE");
		string_iterate_lines(registros, (void*) free);
		free(registros);
		i++;
		free(bloquesUnificados);
		free(bloquesABuscar);

	} else
		registro = NULL;
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
	if (listRegistros->elements_count == 0)
		return NULL;
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
	if (list_is_empty(memtable))
//		TODO: poner define para memtable vacia
		return 1;
	list_iterate(memtable, (void*) &paraDumpearTabla);
	list_clean(memtable);
	cantidadDeDumpeos++;
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
	string_append_with_format(&strsize, "%d", (size + textsize - 1));
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
			log_debug(logger, "voy a crear string de bloques dumpeo");
			char* bloqueDumpeoNuevo = string_new();
			string_append(&bloqueDumpeoNuevo, configLFS->dirMontaje);
			string_append(&bloqueDumpeoNuevo, "Bloques/");
			string_append_with_format(&bloqueDumpeoNuevo, "%d", bit_index);
			string_append(&bloqueDumpeoNuevo, ".bin");
			log_debug(logger, "cree string de bloques dumpeo");
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
	msync(map, size, MS_SYNC);
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

int compactacion(char* nombre_tabla) {
//TODO: RENOMBRAR TODOS//SEMAFOROS!!!!!!!!!
	char* temporales = string_new();
	char* binarios = string_new();
	t_list* lista_bin = list_create();
	t_list* lista_Temp = list_create();

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
		registro* encontrado = list_find(lista_bin, (void*) &esta_registro);

		if (encontrado != NULL) { //replace
			if (encontrado->timestamp < reg->timestamp)
				encontrado = &reg;
		} else
			log_debug(logger, "Llego un registro: %s", reg->value);
		list_add(lista_bin, reg);
	}
//  TODO: no se porque las dos veces compacta el segundo elemento de la lista, crearListaRegistros se arma bien.
//	Probado con dos INSERTS
	list_iterate(lista_Temp, (void*) &compactar);
	char* tabla = string_new();
	string_append_with_format(&tabla, "%sTablas/%s/metadata",
			configLFS->dirMontaje, nombre_tabla);
	t_config* t = config_create(tabla);
	int part = config_get_int_value(t, "PARTITIONS");

	log_debug(logger, "%d", lista_bin->elements_count);
	guardar_en_disco(lista_bin, part, nombre_tabla);

	return 0;
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
		char** bloques = config_get_array_value(tmp, "BLOCKS");
		int s = 0;
		while (bloques[s] != NULL)
			s++;
		char* bloque = string_new();
		string_append_with_format(&bloque, "%sBloques/%s.bin",
				configLFS->dirMontaje, bloques[s - 1]);
		log_debug(logger, bloque);
		int fd2 = open(bloque, O_RDWR, (mode_t) 0600);
		log_debug(logger, "bajo a memoria");
		bajarAMemoria(&fd2, registroParaEscribir, tmp);
		close(fd2);
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

		if (size > 0) {
			char** bloquesABuscar = config_get_array_value(particion, "BLOCKS");
			int i = 0;

			//		Unifico la informacion de todos los bloques en los que esta dividido el archivo .tmp
			while (bloquesABuscar[i] != NULL) {

				char* bloque = string_new();
				string_append_with_format(&bloque, "%sBloques/%s.bin",
						configLFS->dirMontaje, bloquesABuscar[i]);
				int fd = open(bloque, O_RDONLY, S_IRUSR | S_IWUSR);

				char* f = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);
				string_append(bloquesUnificados, f);

				t_bitarray* bitmap = levantarBitmap();
				bitarray_clean_bit(bitmap, atoi(bloquesABuscar[i]));
				char* bloque_a_limpiar = string_new();
				string_append_with_format(&bloque_a_limpiar,
						"rm %sBloques/%s.bin", configLFS->dirMontaje,
						bloquesABuscar[i]);

				system(bloque_a_limpiar);
				free(bloque_a_limpiar);
				close(fd);
				free(bloque);
				i++;
			}
			free(bloquesABuscar);
		} else

			free(rutaBinario);
		config_destroy(particion);
	}
	log_debug(logger, "en tmp encontre %s", *bloquesUnificados);
	return todoJoya;
}

int obtener_temporales(char* nombre_tabla, char** bloquesUnificados) {

	for (int aux = 1; aux <= cantidadDeDumpeos; aux++) {
		char* rutaTemporal = string_new();
		string_append_with_format(&rutaTemporal, "%sTablas/%s/%d.tmp",
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

				t_bitarray* bitmap = levantarBitmap();
				bitarray_clean_bit(bitmap, atoi(bloquesABuscar[i]));
				char* bloque_a_limpiar = string_new();
				string_append_with_format(&bloque_a_limpiar,
						"rm %sBloques/%s.bin", configLFS->dirMontaje,
						bloquesABuscar[i]);

				system(bloque_a_limpiar);
				free(bloque_a_limpiar);
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
	t_bitarray* structBitarray = bitarray_create_with_mode(bitarray, size, MSB_FIRST);
	for (int i = 0; i < configMetadata->blocks; i++) {
		if (i == 10)
			bitarray_set_bit(structBitarray, i);
		else
			bitarray_clean_bit(structBitarray, i);
	}
	char* path = string_from_format("%s/Metadata/Bitmap.bin", configLFS->dirMontaje);
	FILE* file = fopen(path, "wb+");
	fwrite(structBitarray->bitarray, sizeof(char), configMetadata->blocks / 8, file);
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
	log_debug(logger,"entre al hilo");

	while (1) {
		char buffer[BUF_LEN];


			int file_descriptor = inotify_init();
			if (file_descriptor < 0) {
				perror("inotify_init");
			}


			int watch_descriptor = inotify_add_watch(file_descriptor, "../", IN_MODIFY | IN_CREATE | IN_DELETE);





			int length = read(file_descriptor, buffer, BUF_LEN);
			if (length < 0) {
				perror("read");
			}

			int offset = 0;



			while (offset < length) {




				struct inotify_event *event = (struct inotify_event *) &buffer[offset];


				if (event->len) {


					if (event->mask & IN_CREATE) {
						if (event->mask & IN_ISDIR) {
							printf("The directory %s was created.\n", event->name);
						} else {
							printf("The file %s was created.\n", event->name);
						}
					} else if (event->mask & IN_DELETE) {
						if (event->mask & IN_ISDIR) {
							printf("The directory %s was deleted.\n", event->name);
						} else {
							printf("The file %s was deleted.\n", event->name);
						}
					} else if (event->mask & IN_MODIFY) {
						if (event->mask & IN_ISDIR) {
							printf("The directory %s was modified.\n", event->name);
						} else {
							printf("The file %s was modified.\n", event->name);
							levantarConfigLFS();
						}
					}
				}
				offset += sizeof (struct inotify_event) + event->len;
			}

			inotify_rm_watch(file_descriptor, watch_descriptor);
			close(file_descriptor);
	}
}

