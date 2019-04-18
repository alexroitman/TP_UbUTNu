################################################################################
# Automatically-generated file. Do not edit!
################################################################################

-include ../makefile.init

RM := rm -rf

# All of the sources participating in the build are defined here
-include sources.mk
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
all: Kernel

dependents:
	-cd /home/utnso/Operativos/sockets-lib/Debug && $(MAKE) clean all

# Tool invocations
Kernel: $(OBJS) $(USER_OBJS) /home/utnso/Operativos/sockets-lib/Debug/libsockets-lib.so
	@echo 'Building target: $@'
	@echo 'Invoking: GCC C Linker'
	gcc -L"/home/utnso/Operativos/sockets-lib/Debug" -o "Kernel" $(OBJS) $(USER_OBJS) $(LIBS)
	@echo 'Finished building target: $@'
	@echo ' '

# Other Targets
clean:
	-$(RM) $(EXECUTABLES)$(OBJS)$(C_DEPS) Kernel
	-@echo ' '

.PHONY: all clean dependents
/home/utnso/Operativos/sockets-lib/Debug/libsockets-lib.so:

-include ../makefile.targets
