#!/bin/bash
cd ../Pruebas/ConfigPruebaBase/
cp -a 1 ../../Operativos/Memoria/
cp -a 2 ../../Operativos/Memoria/
cp -a Kernel.config ../../Operativos/
cp -a LFS.config ../../Operativos/
cp -a Metadata.bin ../../FS_LISSANDRA/Metadata
exit