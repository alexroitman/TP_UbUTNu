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
NIL}type;
typedef struct {

char* header;
char* query;
 // NOTA: Es calculable. Aca lo tenemos por fines didacticos!
} t_Package_Request;

typedef struct {

char* message;
uint32_t message_long;
uint32_t total_size;// NOTA: Es calculable. Aca lo tenemos por fines didacticos!
} t_Package_Response;

typedef struct {
int8_t type;
int16_t length;
char payload[MAX_BUFFER];
} tPaquete;

typedef struct {
type type;
uint32_t nombre_tabla_long;
char* nombre_tabla;
int key;//INSERT
uint32_t value_long;
char* value;
uint32_t length;
} tInsert;

typedef struct {
type type;
uint32_t nombre_tabla_long;
char* nombre_tabla;							//SELECT
int key;
uint32_t length;
} tSelect;

typedef struct {
char* nombre_tabla;
int consistencia;							//CREATE
int particiones;
int compaction_time;
} tCreate;

typedef struct {
char* nombre_tabla;							//DESCRIBE
} tDescribe;

typedef struct {
char* nombre_tabla;							//DROP
} tDrop;

typedef struct {
							//JOURNAL   (QUE CARAJO PONGO ACA)
} tJournal;

typedef struct {
int memory;
int numero;									//ADD
int criterio;
} tAdd;

int serializarRequest(t_Package_Request packageRequest,
	tPaquete* paqueteSerializado);
int desSerializarRequest(tPaquete* paqueteSerializado,
	t_Package_Request* packageRequest);

int desSerializarInsert(tInsert* packageInsert, int socket);
char* serializarInsert(tInsert* packageInsert);

int serializarDrop(tDrop packageDrop, tPaquete* paqueteSerializado);
int desSerializarDrop(tPaquete* paqueteSerializado, tDrop* packageDrop);

char* serializarSelect(tSelect* packageSelect);
int desSerializarSelect(tSelect* packageSelect,int socket);

int serializarCreate(tCreate packageCreate, tPaquete* paqueteSerializado);
int desSerializarCreate(tPaquete* paqueteSerializado, tCreate* packageCreate);

int serializarDescribe(tDescribe packageDescribe, tPaquete* paqueteSerializado);
int desSerializarDescribe(tPaquete* paqueteSerializado,
	tDescribe* packageDescribe);

int serializarJournal(tJournal packageJournal, tPaquete* paqueteSerializado);
int desSerializarJournal(tPaquete* paqueteSerializado, tJournal* packageJournal);

int serializarAdd(tAdd packageAdd, tPaquete* paqueteSerializado);
int desSerializarAdd(tPaquete* paqueteSerializado, tAdd* packageAdd);

int enviarPaquete(int clienteSocket, char* payload, uint32_t size);
int recibirPaquete(int socketReceptor);

#endif /* SOCK_COMUNICACION_H_ */
