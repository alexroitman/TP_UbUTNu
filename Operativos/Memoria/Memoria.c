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
struct addrinfo hints;
struct addrinfo *serverInfo;

// Define cual va a ser el size maximo del paquete a enviar

int main() {
	int socket_sv = levantarServidor(PUERTOKERNEL);
	int socket_cli = aceptarCliente(socket_sv);
	type header;
	while (1) {
		header = leerHeader(socket_cli);
		tSelect packSelect;
		tInsert packInsert;
		packSelect.type = header;
		switch (header) {
		case SELECT:
			desSerializarSelect(&packSelect,socket_cli);
			printf("recibi un una consulta SELECT de la tabla %s con le key %d \n",
					packSelect.nombre_tabla, packSelect.key);
			break;
		case INSERT:
			desSerializarInsert(&packInsert,socket_cli);
			printf("recibi un una consulta INSERT de la tabla %s con le key %d y el value %s \n",
					packInsert.nombre_tabla, packInsert.key, packInsert.value);
			break;
		}
	}

	close(socket_cli);
	close(socket_sv);
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
