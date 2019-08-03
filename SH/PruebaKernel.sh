#!/bin/bash
cd ../Pruebas/ConfigPruebaKernel/
cp -a 1 ../../Operativos/Memoria/
cp -a 2 ../../Operativos/Memoria/
cp -a 3 ../../Operativos/Memoria/
cp -a 4 ../../Operativos/Memoria/
cp -a Kernel.config ../../Operativos/
cp -a LFS.config ../../Operativos/
cd ../../
mkdir -p lfs-prueba-kernel
cd lfs-base
mkdir -p Bloques
mkdir -p Tablas
mkdir -p Metadata
cd ..
cd Pruebas/ConfigPruebaKernel/
cp -a Metadata.bin ../../lfs-prueba-kernel/Metadata
exit
