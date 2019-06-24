/*
 * errores.h
 *
 *  Created on: 23 jun. 2019
 *      Author: utnso
 */

#ifndef SOCK_LOGGER_H_
#define SOCK_LOGGER_H_

// TIPOS DE ERRORES PARA LOGGER
#define todoJoya 0
#define tablaExistente -1
#define carpetaNoCreada -2
#define metadataNoCreada -3
#define BINNoCreado -4
#define errorAlBorrarDirectorio -5
#define tablaNoEliminada -6
#define noAbreMetadata -7
#define noExisteParametro -8
#define noExisteTabla -9
#define noSeAgregoTabla -10


#include <commons/log.h>
#include "comunicacion.h"


t_log* iniciar_logger(void);
void imprimir_registro(registro* unreg);
void logeoDeErrores(int errorHandler, t_log* logger);


#endif /* SOCK_LOGGER_H_ */
