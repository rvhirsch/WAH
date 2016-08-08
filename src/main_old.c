
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "RawBitmapReader.h"
#include "Control.h"
#include "Query.h"
#include "Writer.h"




/*
 * Runs formatter/compressor/query engines as set in Control.h
 */
int main_old() {
	setbuf(stdout,NULL);

	if(RUN_FORMAT){
		char *folder;
		if(FORMAT_TYPE == UNSTRIPED)
			folder = toUnstriped();
		else if(FORMAT_TYPE == STRIPED)
			folder = toStriped();
		printf("Reformatted file extension\t%s\n",folder);
		return 1;
	}

	char newFile[BUFF_SIZE];//will hold where the reformatted files are
	char buff[BUFF_SIZE];
	if(FORMAT_TYPE==UNSTRIPED){
		snprintf(buff,BUFF_SIZE,"UNSTRIPED_");
	}
	else if(FORMAT_TYPE==STRIPED){
		snprintf(buff,BUFF_SIZE,"STRIPED_%dkB_%d_",BLOCK_SIZE,NUM_THREADS);
	}
	snprintf(newFile,BUFF_SIZE,"%s",getDir(BITMAP_FILE,buff));

	time_t start,end;//start and end clocking times
	unsigned long total;//total amount of time compression ran
	char *folder;

	if(RUN_COMPRESSION){
		char *format;
		if(FORMAT_TYPE==UNSTRIPED) format = "UNSTRIPED";
		else if(FORMAT_TYPE==STRIPED) format = "STRIPED";
		printf("Start compression of\t\t\t%s\t %dkB\t%d threads\t%s\n",BITMAP_FILE,BLOCK_SIZE,NUM_THREADS,format);
		start = clock();
		folder = readFile(newFile);
		end = clock();
		total = end-start;
		printf("End compression\n");
		printf("Total compression time:\t%lu\n",(unsigned long)total);
		return 1;
	}
	else{
		folder = readFile(newFile);
	}

	if(RUN_QUERY){
		printf("Start query\n");
		start = clock();
		runQueries(folder);
		end = clock();
		total = end-start;
		printf("End querying\n");
		printf("Total query time: %lu\n",(unsigned long)total);
	}

	return 0;
}
