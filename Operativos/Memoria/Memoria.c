/*
 * Kernel.c
 *
 *  Created on: 11 abr. 2019
 *      Author: utnso
 */

#include "Memoria.h"

#define IP "127.0.0.1"
#define PUERTOKERNEL "7878"
#define BACKLOG 5			// Define cuantas conexiones vamos a mantener pendientes al mismo tiempo
#define PACKAGESIZE 1024	// Define cual va a ser el size maximo del paquete a enviar
char package[PACKAGESIZE];
#define CANTIDADMAXPAGINAS 10
#define CANTIDADPAGINASMEMORIA 50
struct addrinfo hints;
struct addrinfo *serverInfo;
#define PUERTOLFS "7879"
// Define cual va a ser el size maximo del paquete a enviar

int main() {
	int socket_sv = levantarServidor(PUERTOKERNEL);
	int socket_cli = aceptarCliente(socket_sv);
	int socket_lfs = levantarCliente(PUERTOLFS, IP);

	type header;
	while (1) {
		header = leerHeader(socket_cli);
		tSelect* packSelect=malloc(sizeof(tSelect));
		tInsert* packInsert=malloc(sizeof(tInsert));

		switch (header) {
		case SELECT:
			desSerializarSelect(packSelect,socket_cli);
			packSelect->type=header;
			char* selectAEnviar=serializarSelect(packSelect);
			enviarPaquete(socket_lfs,selectAEnviar,packSelect->length);

			break;
		case INSERT:
			desSerializarInsert(packInsert,socket_cli);
			packInsert->type = header;
			char* insertAEnviar = malloc(packInsert->length);
			insertAEnviar = serializarInsert(packInsert);
			enviarPaquete(socket_lfs,insertAEnviar,packInsert->length);
			free(insertAEnviar);
			//printf("recibi un una consulta INSERT de la tabla %s con le key %d y el value %s \n",
			//		packInsert->nombre_tabla, packInsert->key, packInsert->value);
			break;
		}
		free(packSelect);
		free(packInsert);
	}
	close(socket_lfs);
	close(socket_cli);
	close(socket_sv);
}

tSegmento* cargarSegmentoEnTabla(tNuevoSegmento nuevoSeg,t_list* listaSeg){
	elem_tabla_pag* vecPaginas = calloc(CANTIDADMAXPAGINAS,sizeof(elem_tabla_pag));
	tSegmento* seg;
	seg->path = nuevoSeg.path;
	seg->tablaPaginas = vecPaginas;
	list_add(listaSeg,seg);
	return seg;
}

/*

int cargarPaginaEnMemoria(tNuevoSegmento nuevoSeg,tSegmento* seg){
	elem_tabla_pag* vecPaginas = seg->tablaPaginas;
	int i;
	for(i=0;i<CANTIDADMAXPAGINAS-1;i++){
		//pregunto por null y por 0 porque no estoy seguro con cual de las dos se inicializa
		if(vecPaginas[i]->punteroMemoria != NULL && vecPaginas[i]->punteroMemoria != 0){
			//cargo la pagina en memoria
		}
	}
}
*/

int buscarSegmentoEnTabla(char* nombreTabla, tSegmento* segmento, t_list* listaSegmentos){
	bool mismoNombre (void* elemento){
		tSegmento* seg = (tSegmento*) elemento;
		char* path = seg->path;
		return !strcmp(nombreTabla,separarNombrePath(path));
	}
	segmento = list_find(listaSegmentos, mismoNombre);
	if(segmento != NULL){
		return 1;
	}
	return -1;
}


char* separarNombrePath(char* path){
	char** separado = string_split(path,"/");
	int i =0;
	char* nombre;
	while(separado[i+1] != NULL){
		i++;
	}
	nombre = separado[i];
	int j = 0;
	while(separado[j] != NULL && j != i){
		free(separado[j]);
	}
	free(separado);
	return nombre;
}


int buscarPaginaEnTabla(tSegmento* segmento, tPagina* pagina, int key){
	int i;
	elem_tabla_pag* vectorPaginas = (segmento->tablaPaginas);
	for(i=0;i<CANTIDADMAXPAGINAS;i++){
		tPagina* paginaAux = (tPagina*)vectorPaginas[i].punteroMemoria;
		if(paginaAux != NULL){
			if(paginaAux->key ==  key){
				pagina = paginaAux;
				return 1;
			}
		}
	}
	return -1;
}

/*
 void parsearMensaje(t_Package *paquete){

 char** vector = NULL;
 split(paquete -> message, vector);

 switch(vector[0] ){
 case "SELECT":
 t_select miSelect;
 miSelect -> key = atoi(vector[2]);
 miSelect -> table = vector[1];
 hacerSelect(miSelect);
 break;
 case "INSERT":
 t_Insert miInsert;
 miInsert -> table = vector[1];
 miInsert -> value = vector[2];
 miInsert -> timestap = atoi(vector[3]);
 hacerInsert(miInsert);
 break;
 case "CREATE":
 t_Create miCreate;
 miCreate -> table = vector[1];
 miCreate -> tipoConsistencia = vector[2];
 miCreate -> particiones = atoi(vector[3]);
 miCreate -> tiempoCompactacion = atoi(vector[4]);
 hacerCreate(miCreate);
 break;
 case "DROP":
 t_Drop miDrop;
 miDrop -> table = vector[1];
 hacerDrop(miDrop);
 break;
 case "DESCRIBE":
 t_Describe miDescribe;
 miDescribe -> table = vector[1];
 hacerDescribe(miDescribe);
 break;
 default:
 //DEVOLVER ERROR
 // ALGUN DIA
 break;
 }
 }

 */
