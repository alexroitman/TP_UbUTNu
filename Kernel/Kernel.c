#include "Servidor/src/ServerKM.h"

char* newpack="";
int main()

{

	while(1)
	{

		if(!strcmp(newpack,package))
		{
			leerMensaje(newpack);
			newpack=package;
		}


	}

	return 0;
}

void leerMensaje(char* pack){
	printf("Recibí esto %s", pack);
}
