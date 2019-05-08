#include "comunicacion.h"

int serializarRequest(t_Package_Request packageRequest, tPaquete* paqueteSerializado) {
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


int enviarPaquete(int clienteSocket,char* payload,uint32_t size) {

	return send(clienteSocket, payload,size, 0);
}

int recibirPaquete(int socketReceptor) {



	type header;
	int algo = recv(socketReceptor,&header, sizeof(type), MSG_WAITALL);
	if(algo!=-1)
	{
		printf("recibi algo:%s",&header);
	}
return algo;
	tCreate* createARecibir;
	tInsert* insertARecibir;
	tSelect* selectARecibir;
	tDescribe* describeARecibir;
	tDrop* dropARecibir;
	tJournal* journalARecibir;
	tAdd* addARecibir;
}
	/*switch (paquete_a_recibir->type) {

	case INSERT:

		//desSerializarInsert(paquete_a_recibir,insertARecibir);
		break;
	case SELECT:

		desSerializarSelect( paquete_a_recibir, selectARecibir);
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
	return algo;*/


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
	memcpy(serializedPackage + offset,&(packageSelect->nombre_tabla),size_to_send);
	offset+=size_to_send;


	size_to_send = sizeof(int);
	memcpy(serializedPackage + offset ,&packageSelect->key,size_to_send);


	return serializedPackage;
}


int desSerializarHeader(char* payload,int socket){

		int status;
		int buffer_size;
		char *buffer = malloc(buffer_size = sizeof(SELECT));



		status = recv(socket, buffer, SELECT, 0);
		int recibido;
		memcpy(recibido, buffer, sizeof(int));
		if (!status) return 0;


		free(buffer);

		return status;
	/*
int desSerializarSelect(tSelect* packageSelect,int socket){

		int status;
		int buffer_size;
		char *buffer = malloc(buffer_size = sizeof(uint32_t));


		uint32_t username_long;
		status = recv(socket, buffer, sizeof(package->username_long), 0);
		memcpy(&(username_long), buffer, buffer_size);
		if (!status) return 0;

		status = recv(socket, package->username, username_long, 0);
		if (!status) return 0;

		uint32_t message_long;
		status = recv(socket, buffer, sizeof(package->message_long), 0);
		memcpy(&(message_long), buffer, buffer_size);
		if (!status) return 0;

		status = recv(socket, package->message, message_long, 0);
		if (!status) return 0;


		free(buffer);

		return status;



	*/


	/*
	packageSelect = malloc(sizeof(paqueteSerializado));
		int offset = 0;
		int size_to_send;
		for (size_to_send = 1;(paqueteSerializado->payload)[size_to_send - 1] != '\0';size_to_send++);

		memcpy(&(packageSelect->nombre_tabla), paqueteSerializado + offset, size_to_send);

		offset += size_to_send;
	    size_to_send = sizeof(int);

		memcpy(&(packageSelect->key), paqueteSerializado + offset, size_to_send);

		return 0;*/

}
