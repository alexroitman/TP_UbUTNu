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

type leerHeader(int socket) {
	type header;
	int algo = recv(socket, &header, sizeof(type), MSG_WAITALL);
	return header;
}



char* serializarSelect(tSelect* packageSelect) {

	char *serializedPackage = malloc(packageSelect->length);
	int offset = 0;
	int size_to_send;

	size_to_send = sizeof(packageSelect->type);
	memcpy(serializedPackage + offset, &(packageSelect->type), size_to_send); //sizeof(int8_t)
	offset += size_to_send;

	size_to_send = sizeof(packageSelect->nombre_tabla_long);
	memcpy(serializedPackage + offset, &(packageSelect->nombre_tabla_long),
			size_to_send);
	offset += size_to_send;

	size_to_send = packageSelect->nombre_tabla_long;

	memcpy(serializedPackage + offset, (packageSelect->nombre_tabla),
			size_to_send);
	offset += size_to_send;

	size_to_send = sizeof(int);
	memcpy(serializedPackage + offset, &packageSelect->key, size_to_send);

	packageSelect->length = sizeof(packageSelect->type)
			+ sizeof(packageSelect->nombre_tabla_long)
			+ packageSelect->nombre_tabla_long + sizeof(packageSelect->key);

	return serializedPackage;
}

int desSerializarSelect(tSelect* packageSelect, int socket) {

	int status;
	int buffer_size;
	char *buffer = malloc(buffer_size = sizeof(uint32_t));

	uint32_t nombrelong;
	status = recv(socket, buffer, sizeof((packageSelect->nombre_tabla_long)),
			0); //recibo la longitud
	memcpy(&(packageSelect->nombre_tabla_long), buffer, buffer_size);

	if (!status)
		return 0;
	packageSelect->nombre_tabla = malloc(packageSelect->nombre_tabla_long);

	status = recv(socket, packageSelect->nombre_tabla, packageSelect->nombre_tabla_long, 0); //recibo el nombre de la tabla

	if (!status)
		return 0;
	packageSelect->key = malloc(sizeof(int));
	status = recv(socket, &packageSelect->key, sizeof(packageSelect->key), 0); //recibo el nombre de la key
	if (!status)
		return 0;


	free(buffer);

	return status;

}
char* serializarInsert(tInsert* packageInsert) {

	char *serializedPackage = malloc(packageInsert->length);
	int offset = 0;
	int size_to_send;

	size_to_send = sizeof(packageInsert->type);
	memcpy(serializedPackage + offset, &(packageInsert->type), size_to_send); //sizeof(int8_t)
	offset += size_to_send;

	size_to_send = sizeof(packageInsert->nombre_tabla_long);
	memcpy(serializedPackage + offset, &(packageInsert->nombre_tabla_long),
			size_to_send);
	offset += size_to_send;

	size_to_send = packageInsert->nombre_tabla_long;

	memcpy(serializedPackage + offset, (packageInsert->nombre_tabla),
			size_to_send);
	offset += size_to_send;

	size_to_send = sizeof(int);
	memcpy(serializedPackage + offset, &packageInsert->key, size_to_send);

	offset += size_to_send;
	size_to_send = sizeof(uint32_t);
	memcpy(serializedPackage + offset, &packageInsert->value_long, size_to_send);
	offset += size_to_send;

	size_to_send = packageInsert->value_long;
	memcpy(serializedPackage + offset, (packageInsert->value),
				size_to_send);
	offset += size_to_send;
	return serializedPackage;
}

int desSerializarInsert(tInsert* packageInsert, int socket) {

	int status;
	int buffer_size;
	char *buffer = malloc(buffer_size = sizeof(uint32_t));

	uint32_t nombrelong;
	status = recv(socket, buffer, sizeof((packageInsert->nombre_tabla_long)),
			0); //recibo la longitud
	memcpy(&(nombrelong), buffer, buffer_size);
	if (!status)
		return 0;
	packageInsert->nombre_tabla = malloc(nombrelong);

	status = recv(socket, packageInsert->nombre_tabla, nombrelong, 0); //recibo el nombre de la tabla

	if (!status)
		return 0;
	packageInsert->key = malloc(sizeof(int));
	status = recv(socket, &packageInsert->key, sizeof(packageInsert->key), 0); //recibo el key
	if (!status)
		return 0;
	uint32_t valuelong;
		status = recv(socket, buffer, sizeof((packageInsert->value_long)),
				0); //recibo la longitud
		memcpy(&(valuelong), buffer, buffer_size);
		if (!status)
			return 0;
		packageInsert->value = malloc(valuelong);

		status = recv(socket, packageInsert->value, valuelong, 0); //recibo el value

		if (!status)
			return 0;


	free(buffer);

	return status;

}

