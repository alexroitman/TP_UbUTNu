#!/bin/bash
cd ../Pruebas/ConfigPruebaBase/
cp -a 1 ../../Operativos/Memoria/
cp -a 2 ../../Operativos/Memoria/
cp -a Kernel.config ../../Operativos/
cp -a LFS.config ../../Operativos/
cd /home/utnso
mkdir -p lfs-base
cd lfs-base
mkdir -p Bloques
mkdir -p Tablas
mkdir -p Metadata
cd /home/utno/tp-2019-1c-UbUTNu
cd Pruebas/ConfigPruebaBase/
cp -a Metadata.bin ../../lfs-base/Metadata
exit
