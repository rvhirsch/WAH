#include <math.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "Control.h"
#include "Query.h"
#include "RawBitmapReader.h"
#include "Writer.h"

void printfile(char *filename);
void* discretize_abalone_line(void *line);
void* switch_file_and_compress(void *file);
void compress_file();
