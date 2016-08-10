
//this file is used to define the parameters
#define WAH_32 1
#define WAH_64 0
#define UNSTRIPED 1
#define STRIPED 0

//DEFINE HERE***************************************
#define FORMAT_TYPE UNSTRIPED						//STRIPED or UNSTRIPED
#define RUN_FORMAT 1								//do you want to run the reformatter (Writer.c)
#define BLOCK_SIZE 16								//kB blocks (recommended: 32)
#define NUM_THREADS 1 								//define number of threads to use for compression (1,2,3,4)
#define BITMAP_FILE "Files/abalone_data_bitmap.txt"	//the bitmap file to be compressed

#define RUN_COMPRESSION 1							//do you want to run compression (WAHCompressor.c)
#define COMPRESSION WAH_32 							//define compression type (WAH_32 or WAH_64)

#define RUN_QUERY 0									//do you want to run query (Query.c)
#define QUERY_FILE "Queries/query1.txt"				//the query file to be parsed
//DEFINE HERE***************************************


#if COMPRESSION == WAH_32	//WAH 32
	#define WORD_LENGTH 32
	typedef unsigned int word_32;
#else	//WAH 64
	#define WORD_LENGTH 64
	typedef unsigned long long word_32;
#endif


#define BUFF_SIZE 300

