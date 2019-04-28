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
	//char* consulta = leerConsola();
	int socket_kernel = levantarServidor(PUERTOKERNEL);
	pthread_t hiloescucha;
	pthread_t hiloenvio;
	socket_kernel = pthread_create(&hiloescucha, NULL, (void*) aceptarCliente, (int*) socket_kernel);
	//pthread_create(&hiloenvio, NULL, (void*) enviarMensaje, (int*) socket_kernel);
	pthread_join(hiloescucha,NULL);
	//pthread_join(hiloenvio,NULL);
	close(socket_kernel);
	return 0;
}
/*
void recibirMensajeDeKernel(int serverSocket) {

	struct sockaddr_in addr;			// Esta estructura contendra los datos de la conexion del cliente. IP, puerto, etc.
	socklen_t addrlen = sizeof(addr);

	t_Package miPaquete;

	int socketCliente = accept(serverSocket, (struct sockaddr *) &addr, &addrlen);
	char package[PACKAGESIZE];
	int status = 1;		// Estructura que manjea el status de los recieve.

	printf("Cliente conectado. Esperando mensajes:\n");

	while (status != 0) {
		status = recieve_and_deserialize(miPaquete, socketCliente);

		if (status != 0)
			printf("%s", package);
	}
}

void enviarMensajeAKernel(int clienteSocket) {
	int enviar = 1;
	char message[PACKAGESIZE];

	printf("Conectado al servidor. Bienvenido al sistema, ya puede enviar mensajes. Escriba 'exit' para salir\n");

	while (enviar) {
		fgets(message, PACKAGESIZE, stdin);	// Lee una linea en el stdin (lo que escribimos en la consola) hasta encontrar un \n (y lo incluye) o llegar a PACKAGESIZE.
		if (!strcmp(message, "exit\n"))
			enviar = 0;			// Chequeo que el usuario no quiera salir
		if (enviar)
			send(clienteSocket, message, strlen(message) + 1, 0); // Solo envio si el usuario no quiere salir.

	}
}

int levantarCliente() {

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC; // Permite que la maquina se encargue de verificar si usamos IPv4 o IPv6
	hints.ai_socktype = SOCK_STREAM; // Indica que usaremos el protocolo TCP

	getaddrinfo(IP, PUERTOESCRIBE, &hints, &serverInfo); // Carga en serverInfo los datos de la conexion

	int serverSocket;
	serverSocket = socket(serverInfo->ai_family, serverInfo->ai_socktype,
			serverInfo->ai_protocol);

	while(connect(serverSocket, serverInfo->ai_addr, serverInfo->ai_addrlen) != 0){
			connect(serverSocket, serverInfo->ai_addr, serverInfo->ai_addrlen);
		}
	freeaddrinfo(serverInfo);	// No lo necesitamos mas
	return serverSocket;
}

int levantarServidor() {

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;		// No importa si uso IPv4 o IPv6
	hints.ai_flags = AI_PASSIVE;// Asigna el address del localhost: 127.0.0.1
	hints.ai_socktype = SOCK_STREAM;	// Indica que usaremos el protocolo TCP

	getaddrinfo(NULL, PUERTOESCUCHA, &hints, &serverInfo); // Notar que le pasamos NULL como IP, ya que le indicamos que use localhost en AI_PASSIVE
	int listenningSocket;
	listenningSocket = socket(serverInfo->ai_family, serverInfo->ai_socktype,
			serverInfo->ai_protocol);

	bind(listenningSocket, serverInfo->ai_addr, serverInfo->ai_addrlen);
	freeaddrinfo(serverInfo); // Ya no lo vamos a necesitar


	listen(listenningSocket, BACKLOG);
	return listenningSocket;
}



void cerrarConexiones(int socket_cliente, int socket_servidor) {
	close(socket_cliente);
	close(socket_servidor);
}





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


int recieve_and_deserialize(t_Package *package, int socketCliente){

	int status;
	int buffer_size;
	char *buffer = malloc(buffer_size = sizeof(uint32_t));

	uint32_t username_long;
	status = recv(socketCliente, buffer, sizeof(package->username_long), 0);
	memcpy(&(username_long), buffer, buffer_size);
	if (!status) return 0;

	status = recv(socketCliente, package->username, username_long, 0);
	if (!status) return 0;

	uint32_t message_long;
	status = recv(socketCliente, buffer, sizeof(package->message_long), 0);
	memcpy(&(message_long), buffer, buffer_size);
	if (!status) return 0;

	status = recv(socketCliente, package->message, message_long, 0);
	if (!status) return 0;


	free(buffer);

	return status;
}


int split (const char *str, char c, char ***arr)
{
    int count = 1;
    int token_len = 1;
    int i = 0;
    char *p;
    char *t;

    p = str;
    while (*p != '\0')
    {
        if (*p == c)
            count++;
        p++;
    }

    *arr = (char**) malloc(sizeof(char*) * count);
    if (*arr == NULL)
        exit(1);

    p = str;
    while (*p != '\0')
    {
        if (*p == c)
        {
            (*arr)[i] = (char*) malloc( sizeof(char) * token_len );
            if ((*arr)[i] == NULL)
                exit(1);

            token_len = 0;
            i++;
        }
        p++;
        token_len++;
    }
    (*arr)[i] = (char*) malloc( sizeof(char) * token_len );
    if ((*arr)[i] == NULL)
        exit(1);

    i = 0;
    p = str;
    t = ((*arr)[i]);
    while (*p != '\0')
    {
        if (*p != c && *p != '\0')
        {
            *t = *p;
            t++;
        }
        else
        {
            *t = '\0';
            i++;
            t = ((*arr)[i]);
        }
        p++;
    }

    return count;
}*/
