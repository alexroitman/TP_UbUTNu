#!/bin/bash
cd ../Pruebas/ConfigPruebaLFS/
cp -a 1 ../../Operativos/Memoria/
cp -a 2 ../../Operativos/Memoria/
cp -a 3 ../../Operativos/Memoria/
cp -a Kernel.config ../../Operativos/
cp -a LFS.config ../../Operativos/
cd /home/utnso
mkdir -p lfs-compactacion
cd lfs-compactacion
mkdir -p Bloques
mkdir -p Tablas
mkdir -p Metadata
cd /home/utno/tp-2019-1c-UbUTNu
cd Pruebas/ConfigPruebaLFS/
cp -a Metadata.bin ../../lfs-compactacion/Metadata
exit
