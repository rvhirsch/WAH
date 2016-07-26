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
../src/fopen.c \
../src/main.c \
../src/main_mine.c 

OBJS += \
./src/DataRead.o \
./src/Query.o \
./src/RawBitmapReader.o \
./src/WAHCompressor.o \
./src/Writer.o \
./src/fopen.o \
./src/main.o \
./src/main_mine.o 

C_DEPS += \
./src/DataRead.d \
./src/Query.d \
./src/RawBitmapReader.d \
./src/WAHCompressor.d \
./src/Writer.d \
./src/fopen.d \
./src/main.d \
./src/main_mine.d 


# Each subdirectory must supply rules for building sources it contributes
src/%.o: ../src/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


