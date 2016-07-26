#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

#include "Control.h"
//#include "Query.h"
//#include "RawBitmapReader.h"
//#include "Writer.h"

struct abaloneLine {
	char* sex;
	double length;
	double diameter;
	double height;
	double wholeWeight;
	double shuckedWeight;
	double visceraWeight;
	double shellWeight;
	int rings;
};

int processResponse(int sockfd);
void* disc_by_line(void *file);
void* discretize_line(void *buf);
void* discretize_abalone_data(void *buf);
void* fill_abalone_buffer(void *buf);
