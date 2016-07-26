################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/DataRead.c \
../src/Query.c \
../src/RawBitmapReader.c \
../src/WAHCompressor.c \
../src/Writer.c \
../src/main.c \
../src/main_networking.c \
../src/main_old.c 

OBJS += \
./src/DataRead.o \
./src/Query.o \
./src/RawBitmapReader.o \
./src/WAHCompressor.o \
./src/Writer.o \
./src/main.o \
./src/main_networking.o \
./src/main_old.o 

C_DEPS += \
./src/DataRead.d \
./src/Query.d \
./src/RawBitmapReader.d \
./src/WAHCompressor.d \
./src/Writer.d \
./src/main.d \
./src/main_networking.d \
./src/main_old.d 


# Each subdirectory must supply rules for building sources it contributes
src/%.o: ../src/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -pthread -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


