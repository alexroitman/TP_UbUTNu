################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../sock/comuniacion.c \
../sock/logger.c \
../sock/sockets-lib.c 

OBJS += \
./sock/comuniacion.o \
./sock/logger.o \
./sock/sockets-lib.o 

C_DEPS += \
./sock/comuniacion.d \
./sock/logger.d \
./sock/sockets-lib.d 


# Each subdirectory must supply rules for building sources it contributes
sock/%.o: ../sock/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -fPIC -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


