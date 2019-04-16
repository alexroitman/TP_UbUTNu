################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../LFS/Servidor/src/Server.c 

OBJS += \
./LFS/Servidor/src/Server.o 

C_DEPS += \
./LFS/Servidor/src/Server.d 


# Each subdirectory must supply rules for building sources it contributes
LFS/Servidor/src/%.o: ../LFS/Servidor/src/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -I"/home/utnso/workspace/tp-2019-1c-UbUTNu/tp-libs" -O2 -g -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


