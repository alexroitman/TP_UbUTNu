/*
 * errores.c
 *
 *  Created on: 23 jun. 2019
 *      Author: utnso
 */

#include "logger.h"

// ---------------LOGGER-----------------

t_log* iniciar_logger() {
	return log_create("LFS.log", "LFS", 1, LOG_LEVEL_DEBUG);
}

void imprimir_registro(registro* unreg) {
	printf("Encontre un %s", unreg->value);
}

void logeoDeErroresLFS(int errorHandler, t_log* logger) {
	//Optimizacion: ver si hay una forma mejor de manejar los errores
	switch (errorHandler) {
	case tablaExistente:
		log_info(logger, "Se trato de crear una tabla ya existente");
		break;

	case carpetaNoCreada:
		log_info(logger, "No se pudo crear la carpeta de la tabla");
		break;

	case metadataNoCreada:
		log_info(logger, "No se pudo crear el archivo metadata");
		break;

	case BINNoCreado:
		log_info(logger, "No se pudo crear un archivo .bin");
		break;

	case errorAlBorrarDirectorio:
		log_info(logger, "No se pudo borrar el directorio");
		break;

	case tablaNoEliminada:
			log_info(logger, "La tabla solicitada no pudo ser eliminada");
			break;

	case noAbreMetadata:
		log_info(logger, "No se pudo abrir el archivo metadata");
		break;

	case noExisteParametro:
		log_info(logger, "El parametro solicitado no existe");
		break;

	case noExisteTabla:
		log_info(logger, "No existe la tabla solicitada");
		break;

	case noSeAgregoTabla:
		log_info(logger, "La tabla no pudo ser agregada");
		break;

	case noLevantoServidor:
		log_info(logger, "No se pudo levantar el servidor, verifique que el puerto configurado sea correcto");
		break;

	case errorTamanioValue:
		log_info(logger, "Ha ocurrido un error al enviar el TAMANIO_VALUE al cliente");
		break;

	case errorDeMalloc:
		log_info(logger, "No se pudo alocar la memoria solicitada");
		break;

	case noExisteKey:
		log_info(logger, "La Key solicitada no pudo ser seleccionada, es inexistente");
		break;

	case particionesInvalidas:
		log_info(logger, "Las particiones en el archivo de configuracion debe tener un valor mayor a 0");
		break;

	case noAbreBIN:
		log_info(logger, "El archivo .bin no pudo ser accedido");
		break;

	case comandoMalIngresado:
		log_info(logger, "El comando ingresado fue erroneo o se encuentra incompleto");
		break;

	default:
		break;
	}
}
