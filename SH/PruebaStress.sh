#!/bin/bash
cd ../Pruebas/ConfigPruebaStress/
cp -a 1 ../../Operativos/Memoria/
cp -a 2 ../../Operativos/Memoria/
cp -a 3 ../../Operativos/Memoria/
cp -a 4 ../../Operativos/Memoria/
cp -a 5 ../../Operativos/Memoria/
cp -a Kernel.config ../../Operativos/
cp -a LFS.config ../../Operativos/
cd ../../
mkdir -p lfs-stress
cd lfs-stress
mkdir -p Bloques
mkdir -p Tablas
mkdir -p Metadata
cd ..
cd Pruebas/ConfigPruebaStress/
cp -a Metadata.bin ../../lfs-stress/Metadata
exit
