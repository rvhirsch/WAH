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

// method headers //
int main_old();
int main_new();
void* discretize_line_abalone(void *line);
void* compress_data(void* bufNum);
void* compress_buffer(void* buf);
int switch_backup_buf(int compNum);
int switch_buffer(int bufNum);
char* clear_buffer(char* buf);
void compression_overhead();


int INPUTLEVEL = 40;	// number of lines to read into buffer before compression
int COMPRESSIONLEVEL = 100;	// amount of compressed lines in query buffer

char* INPUTFILE = "Files/abalone_data.txt";
FILE * fr;	// file to read from - INPUTFILE

// init threads
pthread_t discretize, compress;

// init buffers to read data from txt file to
char* inputBuffer;
char* backupBuffer;
char* buffer;	// pointer to buffer currently being written to

// init compressed buffers
char** compBuffer;
char** compBackupBuffer;
char** cBuffer;

// init other variables
int bufName;	// inputBuffer = 0, backupBuffer = 1
int compName;	// compressBuffer = 0, compressBackupBuffer = 1
int colsAmt;	// number of columns in each line - add to this number later

// various global variables
int lineNumber = 0;		// number of total lines read in
int iter = 0;			// spot in txt data file

int lineNumCount = 0;	// number of lines read in [0..INPUTLEVEL]

// init locks
pthread_mutex_t *writelock, *compresslock, *countlock;

int main(int argc, char *argv[]) {
	if (strcmp(argv[1],"new") == 0) {
		main_new();
	}
	else if (strcmp(argv[1],"old") == 0) {
		main_old();
	}
	else {
		printf("NEED MAIN ARGUMENT?????");
	}

	return 0;
}

int main_new() {
	// init buffers
	inputBuffer = (char*) malloc(sizeof(char) * INPUTLEVEL * 50);	// buffer written to first
	backupBuffer = (char*) malloc(sizeof(char) * INPUTLEVEL * 50);

	compBuffer = (char**) malloc(sizeof(char*) * COMPRESSIONLEVEL * 50);	// buffer compressed to first
	compBackupBuffer = (char**) malloc(sizeof(char*) * COMPRESSIONLEVEL * 50);

	buffer = (char*) malloc(sizeof(char) * INPUTLEVEL * 50);
	cBuffer = (char**) malloc(sizeof(char*) * INPUTLEVEL * 50);

	int i=0;
	while(compBuffer[i] != NULL) {
		compBuffer[i] = (char*) malloc(sizeof(char) * INPUTLEVEL * 50);
		compBackupBuffer[i] = (char*) malloc(sizeof(char) * INPUTLEVEL * 50);
		cBuffer[i] = (char*) malloc(sizeof(char) * INPUTLEVEL * 50);
		i++;
	}

	// init locks
	writelock = (pthread_mutex_t*) malloc(sizeof(pthread_mutex_t));
	countlock = (pthread_mutex_t*) malloc(sizeof(pthread_mutex_t));
	compresslock = (pthread_mutex_t*) malloc(sizeof(pthread_mutex_t));

	pthread_mutex_init(writelock, NULL);
	pthread_mutex_init(countlock, NULL);
	pthread_mutex_init(compresslock, NULL);

	buffer = inputBuffer;	// to start
	bufName = 0;
	compName = 0;

	// open file to read from
	char * line = NULL;
	size_t len = 0;
	ssize_t read;

	fr = fopen(INPUTFILE, "r");
	if (fr == NULL)
		exit(EXIT_FAILURE);

	// read file line by line
	while ((read = getline(&line, &len, fr)) != -1) {
		pthread_create(&discretize, NULL, discretize_line_abalone, line);
		pthread_join(discretize, NULL);

//		printf("discretized line");

		pthread_create(&compress, NULL, compress_data, &bufName);
		pthread_join(compress, NULL);

//		printf("compressed buffer??");
	}

	printf("\nBUFFER: \n%s\n", buffer);		// TODO - remove later
	printf("\nBACKUP BUFFER: \n%s\n", backupBuffer);	// TODO - remove later

	// destroy locks
	pthread_mutex_destroy(writelock);
	pthread_mutex_destroy(countlock);
	pthread_mutex_destroy(compresslock);

	return 0;
}

void* discretize_line_abalone(void *line) {
	char buf[10];

	pthread_mutex_lock(writelock);

	char *q = strtok(line, ",");
	while (q != NULL) {
		// check if start of line
		if (strcmp(q, "M") == 0) {		// sex
			pthread_mutex_lock(countlock);
			lineNumCount++;
			pthread_mutex_unlock(countlock);

			// set up char buffer, add to line
			sprintf(buf, "%d", lineNumber);	// change int to string

			strcat(buffer, buf);
			strcat(buffer, ",100");

			iter = 0;
			lineNumber++;
		}
		else if (strcmp(q, "F") == 0) {
			pthread_mutex_lock(countlock);
			lineNumCount++;
			pthread_mutex_unlock(countlock);

			// set up char buffer, add to line
			sprintf(buf, "%d", lineNumber);	// change int to string

			strcat(buffer, buf);
			strcat(buffer, ",010");

			iter = 0;
			lineNumber++;
		}
		else if (strcmp(q, "I") == 0) {
			pthread_mutex_lock(countlock);
			lineNumCount++;
			pthread_mutex_unlock(countlock);

			// set up char buffer, add to line
			sprintf(buf, "%d", lineNumber);	// change int to string

			strcat(buffer, buf);
			strcat(buffer, ",001");

			iter = 0;
			lineNumber++;
		}
		// else check other data
		else {
			iter++;
			double num = atof(q);	// for comparisons
			if (iter == 1) {		// length
				if (num < 0.2) {
					strcat(buffer, "10000");
				}
				else if (num < 0.4) {
					strcat(buffer, "01000");
				}
				else if (num < 0.6) {
					strcat(buffer, "00100");
				}
				else if (num < 0.6) {
					strcat(buffer, "00010");
				}
				else {
					strcat(buffer, "00001");
				}
			}
			else if (iter == 2) {	// diameter
				if (num < 0.1) {
					strcat(buffer, "1000");
				}
				else if (num < 0.3) {
					strcat(buffer, "0100");
				}
				else if (num < 0.5) {
					strcat(buffer, "0010");
				}
				else {
					strcat(buffer, "0001");
				}
			}
			else if (iter == 3) {	// height
				if (num < 0.05) {
					strcat(buffer, "100");
				}
				else if (num < 0.1) {
					strcat(buffer, "010");
				}
				else {
					strcat(buffer, "001");
				}
			}
			else if (iter == 4) {	// whole weight
				if (num < 0.3) {
					strcat(buffer, "100000");
				}
				else if (num < 0.6) {
					strcat(buffer, "010000");
				}
				else if (num < 0.9) {
					strcat(buffer, "001000");
				}
				else if (num < 1.2) {
					strcat(buffer, "000100");
				}
				else if (num < 1.5) {
					strcat(buffer, "000010");
				}
				else {
					strcat(buffer, "000001");
				}
			}
			else if (iter == 5) {	// shucked weight
				if (num < 0.2) {
					strcat(buffer, "100000");
				}
				else if (num < 0.4) {
					strcat(buffer, "010000");
				}
				else if (num < 0.6) {
					strcat(buffer, "001000");
				}
				else if (num < 0.8) {
					strcat(buffer, "000100");
				}
				else if (num < 1.0) {
					strcat(buffer, "000010");
				}
				else {
					strcat(buffer, "000001");
				}
			}
			else if (iter == 6) {	// viscera weight
				if (num < 0.1) {
					strcat(buffer, "10000");
				}
				else if (num < 0.3) {
					strcat(buffer, "01000");
				}
				else if (num < 0.5) {
					strcat(buffer, "00100");
				}
				else if (num < 0.7) {
					strcat(buffer, "00010");
				}
				else {
					strcat(buffer, "00001");
				}
			}
			else if (iter == 7) {	// shell weight
				if (num < 0.1) {
					strcat(buffer, "10000");
				}
				else if (num < 0.3) {
					strcat(buffer, "01000");
				}
				else if (num < 0.5) {
					strcat(buffer, "00100");
				}
				else if (num < 0.7) {
					strcat(buffer, "00010");
				}
				else {
					strcat(buffer, "00001");
				}
			}
			else if (iter == 8) {	// rings
				if (num < 5) {
					strcat(buffer, "100000\n");
				}
				else if (num < 10) {
					strcat(buffer, "010000\n");
				}
				else if (num < 15) {
					strcat(buffer, "001000\n");
				}
				else if (num < 20) {
					strcat(buffer, "000100\n");
				}
				else if (num < 25) {
					strcat(buffer, "000010\n");
				}
				else {
					strcat(buffer, "000001\n");
				}
			}
			else {
				printf("ERROR\n");
			}
		}
		q = strtok(NULL, ",");
	}

	pthread_mutex_unlock(writelock);

	return NULL;
}

void compression_overhead() {
	// get number of columns to compress
	char *buf = inputBuffer;
	if (bufName == 1) {
		buf = backupBuffer;
	}
	char *token = strtok(buf, "\n");		// full line
	char * newtoken = strtok(token, ",");	// line num
	newtoken = strtok(NULL, ",");			// discretized data
	colsAmt = strlen(newtoken);
}

void* compress_data(void* bufNum) {
	pthread_mutex_lock(countlock);
	if (lineNumCount != INPUTLEVEL) {
		pthread_mutex_unlock(countlock);
		return NULL;
	}

	// if lineNumCount == INPUTLEVEL
	pthread_mutex_unlock(countlock);

	// compress buffer
	pthread_mutex_lock(compresslock);

	pthread_mutex_lock(writelock);

	compression_overhead();		// get number of columns in buffer

	bufName = switch_buffer((int)bufNum);	// swap buffers

	pthread_mutex_lock(countlock);
	lineNumCount = 0;	// reset count
	pthread_mutex_unlock(countlock);

	pthread_mutex_unlock(writelock);

	if (bufName == 0) {	// buffer to compress == 1
//		compress_buffer(backupBuffer);		// todo: do later

		backupBuffer = clear_buffer(backupBuffer);
	}
	else {	// bufName == 1 -> buffer to compress == 0
//		compress_buffer(inputBuffer);		// todo: do later

		inputBuffer = clear_buffer(inputBuffer);
	}
	pthread_mutex_unlock(compresslock);

	return NULL;
}

void* compress_buffer(void* buf) {
//	char* compressed;
	int count = 0;

//	runOverhead();		// todo: maybe necessary later??

	printf("Start Compression\n");
	double start = clock();	// start timer

//	compressed = readFile(buf);

	// compress buffer
//	if(FORMAT_TYPE == UNSTRIPED)	// todo: finish later
//		compressUnstriped();//start compression
//	else if(FORMAT_TYPE == STRIPED)
//		compressStriped();

	// store in new buffer

	// if new buffer full, switch with old buffer
	if (count == COMPRESSIONLEVEL) {
		compName = switch_backup_buf(compName);
	}

	// move old buffer to disc

	double end = clock();	// end timer
	double time = end-start;
	printf("End Compression: time = %f\n\n", time);

	return NULL;
}

int switch_backup_buf(int compNum) {
	// compressBuffer = 0, compressBackupBuffer = 1
	if (compNum == 0) {
//		compressBuffer = compressBackupBuffer;
		compNum = 1;
	}
	else {	// compNum == 1
//		compressBuffer = inputBuffer;
		compNum = 0;
	}

		return compNum;
}

int switch_buffer(int bufNum) {
	// inputBuffer = 0, backupBuffer = 1
	if (bufName == 0) {
		buffer = backupBuffer;
		bufNum = 1;
	}
	else {	// bufName == 1
		buffer = inputBuffer;
		bufNum = 0;
	}

	return bufNum;
}

char* clear_buffer(char* buf) {
	int i;
	for (i=0; i<strlen(buf); i++) {
		buf[i] = '\0';		// null out spot in buffer
	}

	return buf;
}
