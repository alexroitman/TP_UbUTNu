#!/bin/bash
cd ../Pruebas/ConfigPruebaStress/
cp -a 1 ../../Operativos/Memoria/Debug/
cp -a 2 ../../Operativos/Memoria/Debug/
cp -a 3 ../../Operativos/Memoria/Debug/
cp -a 4 ../../Operativos/Memoria/Debug/
cp -a 5 ../../Operativos/Memoria/Debug/
cp -a Kernel.config ../../Operativos/Kernel/
cp -a LFS.config ../../Operativos/LFS/
cp -a Metadata.bin ../../FS_LISSANDRA/Metadata
exit