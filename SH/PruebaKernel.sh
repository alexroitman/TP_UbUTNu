#!/bin/bash
cd ../Pruebas/ConfigPruebaKernel/
cp -a 1 ../../Operativos/Memoria/
cp -a 2 ../../Operativos/Memoria/
cp -a 3 ../../Operativos/Memoria/
cp -a 4 ../../Operativos/Memoria/
cp -a Kernel.config ../../Operativos/
cp -a LFS.config ../../Operativos/
cd /home/utnso
mkdir -p lfs-prueba-kernel
cd lfs-prueba-kernel
mkdir -p Bloques
mkdir -p Tablas
mkdir -p Metadata
cd /home/utnso/tp-2019-1c-UbUTNu
cd Pruebas/ConfigPruebaKernel/
cp -a Metadata.bin /home/utnso/lfs-prueba-kernel/Metadata
exit
