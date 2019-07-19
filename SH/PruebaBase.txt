#!/bin/bash
cd ../Pruebas/ConfigPruebaBase/
cp -a 1 ../../Operativos/Memoria/Debug/
cp -a 2 ../../Operativos/Memoria/Debug/
cp -a Kernel.config ../../Operativos/Kernel/
cp -a LFS.config ../../Operativos/LFS/
cp -a Metadata.bin ../../FS_LISSANDRA/Metadata
exit