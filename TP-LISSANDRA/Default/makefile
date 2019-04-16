################################################################################
# Automatically-generated file. Do not edit!
################################################################################

-include ../makefile.init

RM := rm

# All of the sources participating in the build are defined here
-include sources.mk
-include Memoria/subdir.mk
-include LFS/Servidor/src/subdir.mk
-include LFS/Cliente/src/subdir.mk
-include Kernel/subdir.mk
-include subdir.mk
-include objects.mk

ifneq ($(MAKECMDGOALS),clean)
ifneq ($(strip $(C_DEPS)),)
-include $(C_DEPS)
endif
endif

-include ../makefile.defs

# Add inputs and outputs from these tool invocations to the build variables 

# All Target
all: TP-LISSANDRA

dependents:
	-cd /home/utnso/workspace/tp-2019-1c-UbUTNu/tp-libs/Debug && $(MAKE) all

# Tool invocations
TP-LISSANDRA: $(OBJS) $(USER_OBJS) /home/utnso/workspace/tp-2019-1c-UbUTNu/tp-libs/Debug/libtp-libs.so
	@echo $(OBJS)
	@echo $(USER_OBJS) 
	@echo $(LIBS)
	@echo 'Building target: $@'
	@echo 'Invoking: GCC C Linker'
	gcc -pthread -L"/home/utnso/workspace/tp-2019-1c-UbUTNu/tp-libs/Debug" -o "Kernel/Kernel" "../Kernel/Kernel.c"  $(USER_OBJS) $(LIBS)
	gcc -pthread -L"/home/utnso/workspace/tp-2019-1c-UbUTNu/tp-libs/Debug" -o "Memoria/Memoria" "../Memoria/Memoria.c"  $(USER_OBJS) $(LIBS)
	@echo 'Finished building target: $@'
	@echo ' '

# Other Targets
clean:
	-$(RM) $(EXECUTABLES)$(OBJS)$(C_DEPS) TP-LISSANDRA
	-@echo ' '

.PHONY: all clean dependents
/home/utnso/workspace/tp-2019-1c-UbUTNu/tp-libs/Debug/libtp-libs.so:

-include ../makefile.targets
