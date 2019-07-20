#include <stdio.h>
#include <ctype.h>
#include <sys/inotify.h>
#include <sys/mman.h>
#include <signal.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <pthread.h>
#include <sock/sockets-lib.h>
#include <sock/comunicacion.h>
#include <commons/log.h>
#include <commons/collections/queue.h>
#include <commons/collections/list.h>
#include <commons/config.h>
#include <commons/collections/dictionary.h>

#ifndef KERNEL_KERNEL_H_
#define KERNEL_KERNEL_H_
#define MAX_MESSAGE_SIZE 300
#define EVENT_SIZE  ( sizeof (struct inotify_event) + 24 )

#define BUF_LEN     ( 1024 * EVENT_SIZE )
typedef enum{
	new,
	ready,
	exec,
	exit_
}estados;

typedef enum{
	sc,
	shc,
	ec,
	nada
}consistencias;


typedef struct{
    char* path;
    int pos;
    estados estado;
    int id;
}script;

typedef struct{
	int contInsert;
	int contSelect;
	unsigned long long acumtInsert;
	unsigned long long acumtSelect;
}t_contMetrics;
typedef struct{
	int id;
	int socket;
}t_infoMem;

typedef struct{
	char* ip_mem;
	char* puerto_mem;
	int quantum;
	int MULT_PROC;
	int metadata_refresh;
	int sleep;
	int t_gossip;
}configKernel;

typedef struct{
	tMemoria* mem;
	int cont;
}tMemLoad;
void ejecutarMetrics();
void describe ();
type leerConsola(); //Lee la consulta y devuelve el string
type validarSegunHeader(char* header);
bool analizarConsulta();
void run();
void innotificar();
int despacharQuery(char* consulta, t_list* socket_memoria);
void add();
void cargarPaqueteSelect();
void cargarPaqueteInsert();
void CPU();
void planificador();
int leerLinea(char* path, int linea, char* leido);
void cargarPaqueteCreate(tCreate *pack, char* cons);
int validarSelect(char* consulta);
int validarCreate(char* consulta);
int validarInsert(char* consulta);
int validarAdd(char* consulta);
tMemoria* generarMem(char* consulta);
int levantarCpus(pthread_t* cpus);
void cargarConfig(t_config* config);
consistencias obtCons(char* criterio);
void inicializarTodo();
void finalizarEjecucion();
consistencias consTabla (char* nombre);
t_infoMem* devolverSocket(consistencias cons, t_list* sockets, int key);
void ejecutarDescribe(t_describe *response);
void actualizarTablaGossip(tGossip* packGossip);
void ejecutarAdd(char* consulta);
void gossip();
void borrarSocket(void* lista);
void metrics();
#endif /* KERNEL_KERNEL_H_ */

