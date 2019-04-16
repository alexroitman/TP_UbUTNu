################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../LFS/Cliente/src/Cliente.c 

OBJS += \
./LFS/Cliente/src/Cliente.o 

C_DEPS += \
./LFS/Cliente/src/Cliente.d 


# Each subdirectory must supply rules for building sources it contributes
LFS/Cliente/src/%.o: ../LFS/Cliente/src/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -I"/home/utnso/workspace/tp-2019-1c-UbUTNu/tp-libs" -O2 -g -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


