#include <stdio.h>
#include <arpa/inet.h>
#include <sys/socket.h>

int main(void){


	//------------CREAR SERVIDOR---------------------------------------

	struct sockaddr_in direccionServidor;
	direccionServidor.sin_family = AF_INET;
	direccionServidor.sin_addr.s_addr = INADDR_ANY;
	direccionServidor.sin_port = htons(1027);

	int servidor = socket(AF_INET, SOCK_STREAM, 0);

	int activado = 1;
	setsockopt(servidor, SOL_SOCKET, SO_REUSEADDR, &activado, sizeof(activado));

	if(bind(servidor, (void*) &direccionServidor, sizeof(direccionServidor)) != 0){
		perror("Fallo el bind");
		return 1;
	}

	printf("Estoy escuchando\n");
	listen(servidor, 100);

	//------------ACEPTAR CONEXION Y ENVIAR MENSAJE---------------------------------------

	struct sockaddr_in direccionCliente;
	unsigned int tamanioDireccion;
	int cliente = accept(servidor, (void*) &direccionCliente, &tamanioDireccion);

	printf("Recibi una conexion en %d!!\n", cliente);
	send(cliente, "Hola Mundo!!", 13, 0);

	for(;;);
	return 0;
}
