#!/bin/bash
cd ../Pruebas/ConfigPruebaMemoria/
cp -a 1 ../../Operativos/Memoria/
cp -a Kernel.config ../../Operativos/
cp -a LFS.config ../../Operativos/
cd /home/utnso
mkdir -p lfs-prueba-memoria
cd lfs-prueba-memoria
mkdir -p Bloques
mkdir -p Tablas
mkdir -p Metadata
cd /home/utnso/tp-2019-1c-UbUTNu
cd Pruebas/ConfigPruebaMemoria/
cp -a Metadata.bin /home/utnso/lfs-prueba-memoria/Metadata
exit
