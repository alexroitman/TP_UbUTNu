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
#define TAMANIOMAXMEMORIA 50
struct addrinfo hints;
struct addrinfo *serverInfo;
#define PUERTOLFS "7879"
// Define cual va a ser el size maximo del paquete a enviar

int main() {
	void* memoria = malloc(TAMANIOMAXMEMORIA);
	t_list* tablaSegmentos = list_create();
	cargarSegmentoEnTabla("workspace/tabla1",tablaSegmentos);
	tSegmento* seg = obtenerSegmentoDeTabla(tablaSegmentos,0);
	cargarSegmentoEnTabla("workspace/tabla2",tablaSegmentos);
	tSegmento* seg2 = obtenerSegmentoDeTabla(tablaSegmentos,1);
	//creo pagina para segmento 1
	tPagina pagina;
	pagina.key = 4;
	pagina.timestamp = 100;
	pagina.value = "hola\0";
	int cod = agregarPaginaAMemoria(seg,pagina,memoria);
	//creo pagina para segmento 1
	tPagina pagina2;
	pagina2.key = 5;
	pagina2.timestamp = 200;
	pagina2.value = "chau\0";
	cod = agregarPaginaAMemoria(seg,pagina2,memoria);
	//creo pagina para segmento 2
	tPagina pagina3;
	pagina3.key = 6;
	pagina3.timestamp = 300;
	pagina3.value = "olakase\0";
	agregarPaginaAMemoria(seg2,pagina3,memoria);
	//creo pagina para segmento 2
	tPagina pagina4;
	pagina4.key = 7;
	pagina4.timestamp = 400;
	pagina4.value = "jaja\0";
	agregarPaginaAMemoria(seg2,pagina4,memoria);

	tPagina* a;
	a = memoria + (seg->tablaPaginas)[0].offsetMemoria;
	printf("PRIMER SEGMENTO, PRIMERA PAG\n", a->key);
	printf("key1:  %d \n", a->key);
	printf("time1:  %d \n", a->timestamp);
	printf("value1:  %s \n\n", a->value);
	a = memoria + (seg->tablaPaginas)[1].offsetMemoria;
	printf("PRIMER SEGMENTO, SEGUNDA PAG\n", a->key);
	printf("key2:  %d \n", a->key);
	printf("time2:  %d \n", a->timestamp);
	printf("value2:  %s \n\n", a->value);
	a = memoria + (seg2->tablaPaginas)[0].offsetMemoria;
	printf("SEGUNDO SEGMENTO, PRIMERA PAG\n", a->key);
	printf("key3:  %d \n", a->key);
	printf("time3:  %d \n", a->timestamp);
	printf("value3:  %s \n\n", a->value);
	a = memoria + (seg2->tablaPaginas)[1].offsetMemoria;
	printf("SEGUNDO SEGMENTO, SEGUNDA PAG\n", a->key);
	printf("key3:  %d \n", a->key);
	printf("time3:  %d \n", a->timestamp);
	printf("value3:  %s \n\n", a->value);

	tSegmento* segmento = malloc(sizeof(tSegmento));
	int encontro = buscarSegmentoEnTabla("tabla2", segmento, tablaSegmentos);

	if(encontro==1){
		printf("Encontre la tabla %s \n", segmento->path);
	}
	tPagina* pagBuscada = malloc(sizeof(tPagina));
	encontro = buscarPaginaEnMemoria(7,segmento,memoria,pagBuscada);
	if(encontro==1){
		printf("Encontre la pagina con value %s \n", pagBuscada->value);
	}
	free(segmento);
	/*
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
	*/
}

int agregarPaginaAMemoria(tSegmento* seg, tPagina pag, void* memoria){
	int cantPags = 0;
	int cantPagsMax = TAMANIOMAXMEMORIA / sizeof(tPagina);
	tPagina temp = *(tPagina*)memoria;
	while(temp.timestamp != 0){
		cantPags++;
		if(cantPags<=cantPagsMax){
			temp = *(tPagina*)(memoria + cantPags * sizeof(tPagina));
		}
		else{
			return -1;
			//no hay mas lugar en memoria
		}
	}
	int offset = (cantPags * sizeof(tPagina));
	*(tPagina*)(memoria + offset) = pag;

	int cantPagSegmento = 0;
	while((seg->tablaPaginas)[cantPagSegmento].offsetMemoria != -1){
		cantPagSegmento++;
		if(cantPagSegmento>CANTIDADMAXPAGINAS){
			return -2;
			//no hay mas lugar en la tabla de paginas
		}
	}

	elem_tabla_pag pagina;
	pagina.modificado = false;
	pagina.offsetMemoria = offset;

	(seg->tablaPaginas)[cantPagSegmento] = pagina;
	return 1;

}

void cargarSegmentoEnTabla(char* path,t_list* listaSeg){
	elem_tabla_pag* vecPaginas = calloc(CANTIDADMAXPAGINAS,sizeof(elem_tabla_pag));
	tSegmento* seg = malloc(sizeof(tSegmento));
	seg->path = path;
	seg->tablaPaginas = vecPaginas;

	int i;
	for(i = 0;i<CANTIDADMAXPAGINAS;i++){
			seg->tablaPaginas[i].offsetMemoria = -1;
		}
	list_add(listaSeg,(tSegmento*) seg);
}


tSegmento* obtenerSegmentoDeTabla(t_list* tablaSeg,int index){
	tSegmento* seg = malloc(sizeof(tSegmento*));
	*seg = *(tSegmento*) list_get(tablaSeg,index);
	return seg;
}




int buscarSegmentoEnTabla(char* nombreTabla, tSegmento* miseg, t_list* listaSegmentos){
	bool mismoNombre (void* elemento){
		tSegmento* seg = malloc(sizeof(tSegmento));
		seg = (tSegmento*) elemento;
		char* path = seg->path;
		return !strcmp(nombreTabla,separarNombrePath(path));
	}
	*miseg = *(tSegmento*)list_find(listaSegmentos, mismoNombre);
	if(miseg != NULL){
		return 1;
	}
	return -1;
}

int buscarPaginaEnMemoria(int key, tSegmento* miseg, void* memoria, tPagina* pagina){
	elem_tabla_pag* vecPaginas;
	vecPaginas = miseg->tablaPaginas;
	int index=0;
	while(vecPaginas[index].offsetMemoria != -1){
		//RECORRO LA TABLA DE PAGINAS YENDO A BUSCAR A MEMORIA SEGUN CADA OFFSET
		*pagina = *(tPagina*)(memoria + vecPaginas[index].offsetMemoria);
		if(pagina->key == key){
			//ENCONTRE MI PAGINA EN MEMORIA
			return 1;
		}
		index++;
	}
	pagina = NULL;
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
	/*int j = 0;
	while(separado[j] != NULL && j != i){
		free(separado[j]);
	}
	free(separado);*/
	return nombre;
}
