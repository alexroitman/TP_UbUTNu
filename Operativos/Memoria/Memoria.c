/*
 * Kernel.c
 *
 *  Created on: 11 abr. 2019
 *      Author: utnso
 */

#include "Memoria.h"
#include "Querys.h"
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
t_log *logger;
// Define cual va a ser el size maximo del paquete a enviar

int main() {

	logger = log_create("Memoria.log", "Memoria.c", 1, LOG_LEVEL_DEBUG);
	t_miConfig* miConfig = malloc(sizeof(t_miConfig));
	miConfig = cargarConfig();
	log_debug(logger, "Levanta archivo de config");

	int socket_sv = levantarServidor((char*) miConfig->puerto_kernel);
	int socket_kernel = aceptarCliente(socket_sv);
	int socket_lfs = levantarCliente((char*) miConfig->puerto_fs,miConfig->ip_fs);
	log_debug(logger, "Levanta conexion con kernel");
	log_debug(logger, "Levanta conexion con LFS");

	void* memoria = malloc(miConfig->tam_mem);
	t_list* tablaSegmentos = list_create();
	type header;
	int encontroSeg = -1;
	int indexPag = -1;
	tSelect* packSelect;
	tInsert* packInsert;
	tCreate* packCreate;
	while (1) {
		header = leerHeader(socket_kernel);



		tPagina* pagina = malloc(sizeof(tPagina));
		tSegmento* miSegmento = malloc(sizeof(tSegmento));
		encontroSeg = -1;
		indexPag = -1;
		switch (header) {
		case SELECT:
			packSelect = malloc(sizeof(tSelect));
			desSerializarSelect(packSelect, socket_kernel);
			packSelect->type = header;
			encontroSeg = buscarSegmentoEnTabla(packSelect->nombre_tabla,
					miSegmento, tablaSegmentos);
			if (encontroSeg == 1) {
				log_debug(logger, "Encontre segmento: %s ",
						packSelect->nombre_tabla);
				indexPag = buscarPaginaEnMemoria(packSelect->key, miSegmento,
						memoria, pagina);
			}
			if (indexPag >= 0) {
				log_debug(logger, "Encontre pagina buscada");
				enviarPagina(socket_kernel, pagina);
				printf("Value: %s \n", pagina->value);
			} else {
				tRegistroRespuesta* reg = malloc(sizeof(tRegistroRespuesta));
				pedirRegistroALFS(socket_lfs, packSelect, reg);
				if (reg->key != -1) {
					tPagina pagAux;
					pagAux.key = reg->key;
					pagAux.timestamp = reg->timestamp;
					pagAux.value = reg->value;
					if (encontroSeg != 1) {
						cargarSegmentoEnTabla(packSelect->nombre_tabla,
								tablaSegmentos);
						log_debug(logger, "Segmento cargado en tabla");
						miSegmento = obtenerUltimoSegmentoDeTabla(
								tablaSegmentos);
					}
					agregarPaginaAMemoria(miSegmento, pagAux, memoria,
							miConfig->tam_mem);
					log_debug(logger, "Pagina cargada en memoria.");
					log_debug(logger, "El value es: %s", pagAux.value);
				}
				//SI NO LO ENCONTRO IGUALMENTE SE LO MANDO A KERNEL PARA QUE TAMBIEN MANEJE EL ERROR
				enviarRegistroAKernel(reg, socket_kernel);
				log_debug(logger, "Value enviado a Kernel");
				free(reg);
			}
			free(packSelect);
			break;
		case INSERT:
			log_debug(logger,"entro al insert");
			packInsert = malloc(sizeof(tInsert));
			desSerializarInsert(packInsert, socket_kernel);
			packInsert->type = header;


			tPagina pagAux;
			pagAux.key = packInsert->key;
			pagAux.timestamp = (int) time(NULL); //HAY QUE OBTENER EL TIMESTAMP REAL
			pagAux.value = packInsert->value;

			encontroSeg = buscarSegmentoEnTabla(packInsert->nombre_tabla,
					miSegmento, tablaSegmentos);
			if (encontroSeg == 1) {
				log_debug(logger, "Encontre segmento %s ",
						packInsert->nombre_tabla);

				indexPag = buscarPaginaEnMemoria(packInsert->key, miSegmento,
						memoria, pagina);
				if (indexPag >= 0) {
					log_debug(logger,
							"Encontre la pagina buscada en el segmento %s ",
							packInsert->nombre_tabla);
					actualizarPaginaEnMemoria(packInsert, miSegmento, memoria,
							indexPag);
					log_debug(logger, "Pagina encontrada y actualizada.");
				} else {
					//Encontro el segmento en tabla pero no tiene la pagina en memoria

					log_debug(logger,
							"Encontro el segmento en tabla pero no tiene la pagina en memoria");
					agregarPaginaAMemoria(miSegmento, pagAux, memoria,
							miConfig->tam_mem);
					log_debug(logger, "Pagina cargada en memoria.");
				}

			} else {
				//No encontro el segmento en tabla de segmentos
				log_debug(logger,
						"No encontro el segmento en tabla de segmentos");
				cargarSegmentoEnTabla(packInsert->nombre_tabla, tablaSegmentos);
				log_debug(logger, "Segmento cargado en tabla");
				tSegmento* newSeg = obtenerUltimoSegmentoDeTabla(tablaSegmentos);
				agregarPaginaAMemoria(newSeg, pagAux, memoria,
						miConfig->tam_mem);
				log_debug(logger, "Pagina cargada en memoria");
			}
			free(packInsert);
			break;
		case CREATE:
			packCreate = malloc(sizeof(tCreate));
			desSerializarCreate(packCreate, socket_kernel);
			packCreate->type = header;
			char* createAEnviar = serializarCreate(packCreate);
			enviarPaquete(socket_lfs, createAEnviar, packCreate->length);
			log_debug(logger, "Mando la consulta a LFS");
			free(packCreate);
			break;

		}
		free(miSegmento);
		free(pagina);


	}
	close(socket_lfs);
	close(socket_kernel);
	close(socket_sv);

}

void actualizarPaginaEnMemoria(tInsert* packInsert, tSegmento* segmento,
		void* memoria, int index) {
	tPagina* pag;
	pag = (tPagina*) (memoria + (segmento->tablaPaginas[index]).offsetMemoria);
	pag->value = packInsert->value;
	pag->timestamp = (int) time(NULL);
	segmento->tablaPaginas[index].modificado = true;
}

tSegmento* obtenerUltimoSegmentoDeTabla(t_list* tablaSeg) {
	int cantSegmentos = list_size(tablaSeg);
	tSegmento* seg = malloc(sizeof(tSegmento*));
	*seg = *(tSegmento*) list_get(tablaSeg, cantSegmentos-1);
	return seg;
}

int agregarPaginaAMemoria(tSegmento* seg, tPagina pag, void* memoria,
		int tam_max) {
	int cantPags = 0;
	int cantPagsMax = tam_max / sizeof(tPagina);
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
		if (list_find(listaSegmentos, mismoNombre) != NULL) {
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

t_miConfig* cargarConfig() {
	t_miConfig* miConfig = malloc(sizeof(t_miConfig));
	t_config* config;
	config = config_create("/home/utnso/workspace/tp-2019-1c-UbUTNu/Operativos/Memoria/Memoria.config");
	miConfig->puerto_kernel = config_get_string_value(config, "PUERTO_KERNEL");
	miConfig->puerto_fs = config_get_string_value(config, "PUERTO_FS");
	miConfig->ip_fs = config_get_string_value(config, "IP");
	miConfig->tam_mem = config_get_int_value(config, "TAM_MEM");
	return miConfig;
}
