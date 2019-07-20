#!/bin/bash
cd ../Pruebas/ConfigPruebaKernel/
cp -a 1 ../../Operativos/Memoria/
cp -a 2 ../../Operativos/Memoria/
cp -a 3 ../../Operativos/Memoria/
cp -a 4 ../../Operativos/Memoria/
cp -a Kernel.config ../../Operativos/
cp -a LFS.config ../../Operativos/
cp -a Metadata.bin ../../FS_LISSANDRA/Metadata
exit