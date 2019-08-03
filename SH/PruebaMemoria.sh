#!/bin/bash
cd ../Pruebas/ConfigPruebaMemoria/
cp -a 1 ../../Operativos/Memoria/
cp -a Kernel.config ../../Operativos/
cp -a LFS.config ../../Operativos/
cd ../../
mkdir -p lfs-prueba-memoria
cd lfs-base
mkdir -p Bloques
mkdir -p Tablas
mkdir -p Metadata
cd ..
cd Pruebas/ConfigPruebaMemoria/
cp -a Metadata.bin ../../lfs-prueba-memoriae/Metadata
exit
