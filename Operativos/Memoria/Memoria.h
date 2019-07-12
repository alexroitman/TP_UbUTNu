#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <commons/collections/list.h>
#include <pthread.h>
#include <sock/sockets-lib.h>
#include <sock/comunicacion.h>
#include <commons/config.h>
#include <commons/log.h>
#include <signal.h>

#ifndef MEMORIA_MEMORIA_H_
#define MEMORIA_MEMORIA_H_
#define IP "127.0.0.1"
#define PUERTO "6666"
#define BACKLOG 5			// Define cuantas conexiones vamos a mantener pendientes al mismo tiempo
#define PACKAGESIZE 1024	// Define cual va a ser el size maximo del paquete a enviar
t_log *logger;
t_config* config;

bool leyoConsola;
bool recibioSocket;

tSelect* packSelect;
tInsert* packInsert;
tCreate* packCreate;
tDescribe* packDescribe;
t_describe* packDescResp;
tDrop* packDrop;
tJournal* packJournal;
type* header;
t_list* tablaSegmentos;
t_list* tablaGossip;
int tamanioMaxValue;
int socket_lfs;
int socket_kernel;
int socket_sv;
int socket_gossip;
int encontroSeg;
int indexPag;
void* memoria;
int cantPagsMax;
pthread_t hiloConsola;
pthread_t hiloSocket;
pthread_t hiloJournal;
pthread_t hiloGossip;

sem_t mutexJournal;

typedef struct{
	type* header;
	char consulta[256];
}tHiloConsola;
tHiloConsola* paramsConsola;


typedef struct {
	int offsetMemoria;
	bool modificado;
	int index;
	int ultimoTime;
}elem_tabla_pag;

typedef struct {
	char* path;
	t_list* tablaPaginas;
}tSegmento;

typedef struct {
	int timestamp;
	u_int16_t key;
	char* value;
} tPagina;

typedef struct {
	int numeroMemoria;
	int puerto_kernel;
	int puerto_fs;
	char* ip_fs;
	char** ip_seeds;
	char** puerto_seeds;
	int tam_mem ;
	int retardoMemoria;
	int retardoJournal;
	int retardoGossiping;
	char* miPuerto;
	char* mi_IP;
} t_miConfig;

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

tGossip* packGossip;
t_miConfig* miConfig;
fd_set active_fd_set, read_fd_set;
char package[PACKAGESIZE];
struct addrinfo hints;
struct addrinfo *serverInfo;

void crearHilosRecepcion(type* header, pthread_t hiloSocket,
		pthread_t hiloConsola,tHiloConsola* paramsConsola);
int ejecutarLRU();
int listMinTimestamp(t_list* listaPaginas,elem_tabla_pag* pagina);
void ejecutarJournal();
void actualizarIndexLista(t_list* lista);
int buscarPaginaEnMemoria(int key, tSegmento* miseg,elem_tabla_pag* pagTabla,tPagina* pagina);
int agregarPaginaAMemoria(tSegmento* seg,tPagina* pagina, bool modificado);
void cargarSegmentoEnTabla(char* path,t_list* listaSeg);
tSegmento* obtenerSegmentoDeTabla(t_list* tablaSeg,int index);
int buscarSegmentoEnTabla(char* nombreTabla, tSegmento* segmento, t_list* listaSegmentos);
void actualizarPaginaEnMemoria(tSegmento* segmento,int index, char* newValue);
tSegmento* obtenerUltimoSegmentoDeTabla(t_list* tablaSeg);
void recibirMensajeDeKernel();
char* separarNombrePath(char* path);
void* recibirHeader(void* arg);
int handshakeLFS(int socket_lfs);
void eliminarDeMemoria(void* elemento);
void liberarPaginasDelSegmento(tSegmento* miSegmento, t_list* tablaSegmentos);
void mandarInsertDePaginasModificadas(t_list* paginasModificadas,char* nombreTabla, int socket_lfs);
void borrarPaginasModificadas(t_list* paginasModificadas, t_list* tablaPaginasMiSegmento);
void chequearMemoriaFull(bool leyoConsola, int error,int socket, tSegmento* miSegmento,
		tPagina* pagina, bool modificado);
void finalizarEjecucion();
void enviarMensajeAKernel();
t_miConfig* cargarConfig();
void journalAsincronico();
void inicializarTablaGossip();
void realizarGossiping();
void actualizarTablaGossip(tGossip* packGossip);
void devolverTablaGossip(tGossip* packGossip,int socket);
void cargarPackGossip(tGossip* packGossip, t_list* tablaGossip, type header);
char* serializarGossip(tGossip* packGossip);
int desSerializarGossip(tGossip* packGossip, int socket);
int levantarCliente();

int levantarServidor();
void cerrarConexiones();


#endif /* MEMORIA_MEMORIA_H_ */
