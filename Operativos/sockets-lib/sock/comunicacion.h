/*
 * comunicacion.h
 *
 *  Created on: 1 may. 2019
 *      Author: utnso
 */

#ifndef SOCK_COMUNICACION_H_
#define SOCK_COMUNICACION_H_
#define MAX_BUFFER 1024
#include "sockets-lib.h"

typedef enum {
INSERT,
SELECT,
CREATE,
DESCRIBE,
DROP,
JOURNAL,
ADD,
RUN,
REGISTRO,
NIL,
GOSSIPING,
RESPGOSS,
GOSSIPKERNEL}type;

typedef struct {

	char* header;
	char* query;
// NOTA: Es calculable. Aca lo tenemos por fines didacticos!
} t_Package_Request;

typedef struct {

	char* message;
	uint32_t message_long;
	uint32_t total_size; // NOTA: Es calculable. Aca lo tenemos por fines didacticos!
} t_Package_Response;

typedef struct {
	int8_t type;
	int16_t length;
	char payload[MAX_BUFFER];
} tPaquete;

typedef struct {
	uint16_t key;
	char* value;
	int timestamp;
} registro;

typedef struct {
	type tipo;
	uint16_t key;
	int value_long;
	char* value;
	int timestamp;
	int length;
} tRegistroRespuesta;



typedef struct {
	type type;
	uint32_t nombre_tabla_long;
	char* nombre_tabla;
	uint16_t key; //INSERT
	uint32_t value_long;
	char* value;
	uint32_t length;
	int timestamp;
} tInsert;

typedef struct {
	type type;
	uint32_t nombre_tabla_long;
	char* nombre_tabla;							//SELECT
	uint16_t key;
	uint32_t length;
} tSelect;

typedef struct {//CREATE
	type type;
	uint32_t nombre_tabla_long;
	char* nombre_tabla;
	uint32_t consistencia_long;
	char* consistencia;
	int particiones;
	int compaction_time;
	uint32_t length;
} tCreate;

typedef struct {//DESCRIBE
	type type;
	char* nombre_tabla;
	int nombre_tabla_long;
	int length;
} tDescribe;


typedef struct {
	char nombre_tabla[12];
	uint8_t particiones;
	char* consistencia;
	//TODO: ver uso de la consistencia
	uint8_t tiempo_compactacion;
} t_metadata;

typedef struct {
	uint16_t cant_tablas;
	t_metadata* tablas;
} t_describe;

typedef struct {//DROP
	type type;
	char* nombre_tabla;
	int nombre_tabla_long;
	int length;
} tDrop;

typedef struct {
	type type;
	uint32_t length;						//JOURNAL
} tJournal;

typedef struct {
	int memory;
	int numero;									//ADD
	int criterio;
} tAdd;

typedef struct {
	char puerto[5];
	char ip[12];
	int numeroMemoria;
}tMemoria;
typedef struct {
	type header;
	int cant_memorias;
	tMemoria* memorias;
}tGossip;
type leerHeader(int socket);

void cargarPaqueteSelect(tSelect *pack, char* cons);
void cargarPaqueteInsert(tInsert *pack, char* cons);
void cargarPaqueteCreate(tCreate *pack, char* cons);
void cargarPaqueteDescribe(tDescribe *pack, char* cons);
void cargarPaqueteInsertLFS(tInsert *pack, char* cons);
void cargarPaqueteDrop(tDrop *pack, char* cons);
void cargarPaqueteJournal(tJournal* pack, char* cons);
void cargarPackGossip(tGossip* packGossip, t_list* tablaGossip, type header);

char* serializarGossip(tGossip* packGossip);
int desSerializarGossip(tGossip* packGossip, int socket);

int desSerializarRegistro(tRegistroRespuesta* reg, int socket);
char* serializarRegistro(tRegistroRespuesta* reg);

int desSerializarInsert(tInsert* packageInsert, int socket);
char* serializarInsert(tInsert* packageInsert);

char* serializarSelect(tSelect* packageSelect);
int desSerializarSelect(tSelect* packageSelect, int socket);

char* serializarCreate(tCreate* packageCreate);
int desSerializarCreate(tCreate* packageCreate, int socket);

char* serializarDescribe(tDescribe* packageDescribe);
int desSerializarDescribe(tDescribe* paqueteSerializado, int socket);

char* serializarDescribe_Response(t_describe *package);
int desserializarDescribe_Response(t_describe* package, int socket);
char* serializarDrop(tDrop* packageDrop);
int desSerializarDrop(tDrop* packageDrop, int socket);

char* serializarJournal(tJournal* packageJournal);
int desSerializarJournal(tJournal* paqueteSerializado, int socket);

int serializarAdd(tAdd packageAdd, tPaquete* paqueteSerializado);
int desSerializarAdd(tPaquete* paqueteSerializado, tAdd* packageAdd);

int enviarPaquete(int clienteSocket, char* payload, uint32_t size);
int recibirPaquete(int socketReceptor);

#endif /* SOCK_COMUNICACION_H_ */
