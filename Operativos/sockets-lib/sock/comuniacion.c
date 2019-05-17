#include "comunicacion.h"

int serializarRequest(t_Package_Request packageRequest,
		tPaquete* paqueteSerializado) {
	paqueteSerializado = malloc(sizeof(package));

	int offset = 0;
	int size_to_send;

	size_to_send = strlen(packageRequest.header) + 1;
	memcpy(paqueteSerializado->payload + offset, packageRequest.header,
			size_to_send);
	offset += size_to_send;

	size_to_send = strlen(packageRequest.query) + 1;
	memcpy(paqueteSerializado->payload + offset, packageRequest.query,
			size_to_send);
	paqueteSerializado->length = offset + size_to_send;

	return paqueteSerializado;
}


int enviarPaquete(int clienteSocket, char* payload, uint32_t size) {

	return send(clienteSocket, payload, size, 0);
}

int recibirPaquete(int socketReceptor) {

	type header;
	int algo = recv(socketReceptor, &header, sizeof(type), MSG_WAITALL);

	tCreate createARecibir;
	tInsert insertARecibir;
	tSelect selectARecibir;
	tDescribe describeARecibir;
	tDrop dropARecibir;
	tJournal journalARecibir;
	tAdd addARecibir;
	switch (header) {

	case INSERT:

		//desSerializarInsert(paquete_a_recibir,insertARecibir);
		break;
	case SELECT:
		selectARecibir.type=header;

		desSerializarSelect(&selectARecibir, socketReceptor);
		printf("recibi un SELECT %s %d \n",selectARecibir.nombre_tabla,selectARecibir.key);
		break;
	case CREATE:

		//desSserializarcreate(paquete_a_recibir,createARecibir);
		break;
	case DESCRIBE:

		//desSerializarDescribe(paquete_a_recibir,describeARecibir);
		break;
	case DROP:

		//desSserializarDescribe(paquete_a_recibir,dropARecibir);
		break;
	case JOURNAL:

		//	desSerializarJournal( paquete_a_recibir,journalARecibir);

		break;
	case ADD:

		//desSerializarAdd( paquete_a_recibir,addARecibir);
		break;

	}
	return algo;

}

char* serializarSelect(tSelect* packageSelect) {

	char *serializedPackage = malloc(packageSelect->length);
	int offset = 0;
	int size_to_send;

	size_to_send=sizeof(packageSelect->type);
	memcpy(serializedPackage + offset,&(packageSelect->type),size_to_send);//sizeof(int8_t)
	offset += size_to_send;


	size_to_send = sizeof(packageSelect->nombre_tabla_long);
	memcpy(serializedPackage+ offset,&(packageSelect->nombre_tabla_long),size_to_send);
	offset+=size_to_send;


	size_to_send = packageSelect->nombre_tabla_long;

	memcpy(serializedPackage + offset,(packageSelect->nombre_tabla),size_to_send);
	offset+=size_to_send;


	size_to_send = sizeof(int);
	memcpy(serializedPackage + offset ,&packageSelect->key,size_to_send);

	packageSelect->length=sizeof(packageSelect->type) +sizeof(packageSelect->nombre_tabla_long)+packageSelect->nombre_tabla_long+sizeof(packageSelect->key);

	return serializedPackage;
}

int desSerializarSelect(tSelect* packageSelect, int socket) {// LO RECIBE PERO NO LO PUEDO METER EN EL STRUCT

	int status;
	int buffer_size;
	char *buffer = malloc(buffer_size = sizeof(uint32_t));

	uint32_t nombrelong;
	status = recv(socket, buffer, sizeof((packageSelect->nombre_tabla_long)), 0);//recibo la longitud
	memcpy(&(nombrelong), buffer, buffer_size);
	if (!status)
		return 0;
	status = recv(socket, packageSelect->nombre_tabla, nombrelong, 0);//recibo el nombre de la tabla

	if (!status)
		return 0;

	status = recv(socket, &packageSelect->key , sizeof(packageSelect->key), 0);//recibo el nombre de la key
	if (!status)
		return 0;

	free(buffer);

	return status;

}
