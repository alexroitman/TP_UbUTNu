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
#define TAMANIOMAXMEMORIA 2048
struct addrinfo hints;
struct addrinfo *serverInfo;
#define PUERTOLFS "7879"
// Define cual va a ser el size maximo del paquete a enviar

int main() {


	t_miConfig* miConfig = malloc(sizeof(t_miConfig));
	miConfig = cargarConfig();

	printf("/n %d", miConfig->tam_mem);


	int socket_sv = levantarServidor(miConfig->puerto_kernel);
	int socket_cli = aceptarCliente(socket_sv);
	//int socket_lfs = levantarCliente(miConfig->puerto_lfs, miConfig->ip);
	void* memoria = malloc(miConfig->tam_mem);
	t_list* tablaSegmentos = list_create();
	type header;
	int encontroSeg = -1;
	int indexPag = -1;
	while (1) {
		header = leerHeader(socket_cli);

		tSelect* packSelect = malloc(sizeof(tSelect));
		tInsert* packInsert = malloc(sizeof(tInsert));
		tPagina* pagina = malloc(sizeof(int)*2 + packInsert->value_long);
		tSegmento* miSegmento = malloc(sizeof(tSegmento));
		encontroSeg = -1;
		indexPag = -1;
		switch (header) {
		case SELECT:
			desSerializarSelect(packSelect, socket_cli);
			packSelect->type = header;

			encontroSeg = buscarSegmentoEnTabla(packSelect->nombre_tabla,
								miSegmento, tablaSegmentos);
			if(encontroSeg == 1){
				indexPag = buscarPaginaEnMemoria(packSelect->key,
										miSegmento, memoria, pagina);
				if(indexPag >= 0){
					printf("Value: %s \n", pagina->value);
				}else{
					printf("\nLe pido a LFS la pagina del segmento %s \n", miSegmento->path);
				}
			}else{
				printf("\nLe pido a LFS el segmento y la pagina correspondiente.");
			}

			//char* selectAEnviar = serializarSelect(packSelect);
			//enviarPaquete(socket_lfs, selectAEnviar, packSelect->length);

			break;
		case INSERT:
			desSerializarInsert(packInsert, socket_cli);
			packInsert->type = header;


			tPagina pagAux;
			pagAux.key = packInsert->key;
			pagAux.timestamp = 123456; //HAY QUE OBTENER EL TIMESTAMP REAL
			pagAux.value = packInsert->value;


			encontroSeg = buscarSegmentoEnTabla(packInsert->nombre_tabla,
					miSegmento, tablaSegmentos);
			if (encontroSeg == 1) {

				indexPag = buscarPaginaEnMemoria(packInsert->key,
						miSegmento, memoria, pagina);
				if (indexPag >= 0) {
					actualizarPaginaEnMemoria(packInsert,miSegmento,memoria,indexPag);
					printf("\nPagina encontrada y actualizada. \n");
					//FALTA HACER LA FUNCION MODIFICAR VALUE DE PAGINA
				} else {
					//Encontro el segmento en tabla pero no tiene la pagina en memoria
					agregarPaginaAMemoria(miSegmento, pagAux, memoria);
					printf("\nPagina cargada en memoria.\n");
				}

			} else {
				//No encontro el segmento en tabla de segmentos
				cargarSegmentoEnTabla(packInsert->nombre_tabla, tablaSegmentos);
				printf("\nSegmento cargado en tabla.\n");
				int cantSegmentos = list_size(tablaSegmentos);
				tSegmento* newSeg = obtenerSegmentoDeTabla(tablaSegmentos,
						cantSegmentos - 1);
				agregarPaginaAMemoria(newSeg, pagAux, memoria);
				 tPagina* a;
				 a = memoria + (newSeg->tablaPaginas)[0].offsetMemoria;
				 printf("Pagina cargada en memoria.\n");
			}
			break;
		}
		free(miSegmento);
		free(pagina);
		free(packSelect);
		free(packInsert);
	}
	//close(socket_lfs);
	close(socket_cli);
	close(socket_sv);

}

void actualizarPaginaEnMemoria(tInsert* packInsert,tSegmento* segmento, void* memoria,int index){
	tPagina* pag;
	pag = (tPagina*)(memoria + (segmento->tablaPaginas[index]).offsetMemoria);
	pag->value = packInsert->value;
}


int agregarPaginaAMemoria(tSegmento* seg, tPagina pag, void* memoria) {
	int cantPags = 0;
	int cantPagsMax = TAMANIOMAXMEMORIA / sizeof(tPagina);
	tPagina temp = *(tPagina*) memoria;
	while (temp.timestamp != 0) {
		cantPags++;
		if (cantPags <= cantPagsMax) {
			temp = *(tPagina*) (memoria + cantPags * sizeof(tPagina));
		} else {
			return -1;
			//no hay mas lugar en memoria
		}
	}
	int offset = (cantPags * sizeof(tPagina));
	*(tPagina*) (memoria + offset) = pag;

	int cantPagSegmento = 0;
	while ((seg->tablaPaginas)[cantPagSegmento].offsetMemoria != -1) {
		cantPagSegmento++;
		if (cantPagSegmento > CANTIDADMAXPAGINAS) {
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

void cargarSegmentoEnTabla(char* path, t_list* listaSeg) {
	elem_tabla_pag* vecPaginas = calloc(CANTIDADMAXPAGINAS,
			sizeof(elem_tabla_pag));
	tSegmento* seg = malloc(sizeof(tSegmento));
	seg->path = path;
	seg->tablaPaginas = vecPaginas;

	int i;
	for (i = 0; i < CANTIDADMAXPAGINAS; i++) {
		seg->tablaPaginas[i].offsetMemoria = -1;
	}
	list_add(listaSeg, (tSegmento*) seg);
}

tSegmento* obtenerSegmentoDeTabla(t_list* tablaSeg, int index) {
	tSegmento* seg = malloc(sizeof(tSegmento*));
	*seg = *(tSegmento*) list_get(tablaSeg, index);
	return seg;
}

int buscarSegmentoEnTabla(char* nombreTabla, tSegmento* miseg,
		t_list* listaSegmentos) {
	bool mismoNombre(void* elemento) {
		tSegmento* seg = malloc(sizeof(tSegmento));
		seg = (tSegmento*) elemento;
		char* path = seg->path;
		return !strcmp(nombreTabla, separarNombrePath(path));
	}
	if (!list_is_empty(listaSegmentos)) {
		if(list_find(listaSegmentos, mismoNombre) != NULL){
			*miseg = *(tSegmento*) list_find(listaSegmentos, mismoNombre);
			return 1;
		}
	}
	return -1;
}
char* separarNombrePath(char* path) {
	char** separado = string_split(path, "/");
	int i = 0;
	char* nombre;
	while (separado[i + 1] != NULL) {
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
int buscarPaginaEnMemoria(int key, tSegmento* miseg, void* memoria,
		tPagina* pagina) {
	elem_tabla_pag* vecPaginas;
	vecPaginas = miseg->tablaPaginas;
	int index = 0;
	while (vecPaginas[index].offsetMemoria != -1) {
		//RECORRO LA TABLA DE PAGINAS YENDO A BUSCAR A MEMORIA SEGUN CADA OFFSET
		*pagina = *(tPagina*) (memoria + vecPaginas[index].offsetMemoria);
		if (pagina->key == key) {
			//ENCONTRE MI PAGINA EN MEMORIA
			return index;
			//RETORNA LA POSICION DE LA PAGINA EN LA TABLA DE PAGINAS
		}
		index++;
	}
	pagina = NULL;
	return -1;
}

t_miConfig* cargarConfig(){
	t_miConfig* miConfig = malloc(sizeof(t_miConfig));
	t_config* config;
	config = config_create("/home/utnso/workspace/tp-2019-1c-UbUTNu/Operativos/Memoria/Memoria.config");
	miConfig->puerto_kernel = config_get_string_value(config, "PUERTO_KERNEL");
	miConfig->puerto_fs =  config_get_string_value(config, "PUERTO_FS");
	miConfig->ip_fs = config_get_string_value(config, "IP");
	miConfig->tam_mem = config_get_int_value(config, "TAM_MEM");
	return miConfig;
}
