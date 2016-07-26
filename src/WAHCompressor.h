#include "RawBitmapReader.h"
#include "Control.h"

#define LITERAL -1//a literal word (0...0111)
#define ZERO_LIT 0//a literal of all zeros (000000...00)
#define ONE_LIT 1//a literal of all ones(01111111..11)
#define ZERO_RUN 2//a fill of zeros (100000...10)
#define ONE_RUN 3//a fill of ones (1100000...10)

//different blockSeg statuses:
#define NOT_VALID -1//this block is empty (no information, cannot be compressed)
#define EMPTY_FIRST 0//no information in segment but marks the beginning of a column
#define READ_FIRST 1//valid block with data (needs to be compressed)
#define VALID 2//block in the middle of a column
#define LAST_BLOCK 3//block is the last block of a column (needs to be written)
#define FIRST_LAST 4//block is both the first and last block of the column

//struct that holds uncompressed data to send to compressor
struct blockSeg{
	word_32 *toCompress;//the current block seg of words
	int size;//number of words in toCompress
	int colNum;//column num compressed
	struct blockSeg *next;//next one to be compressed
	FILE *colFile;//where all the compressed words are going
	word_32 curr;//the latest compressed
	int status;//first/last block of column? or not valid block (empty)?
};




void compressBlock(struct blockSeg *);
word_32 getZeroFill();
word_32 getOneFill();
word_32 getMaxZeroFill();
word_32 getMaxOneFill();
int getType(word_32);
