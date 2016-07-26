#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include "WAHCompressor.h"
#include "Control.h"



/*
 * Compresses one segment of a column
 */
void compressBlock(struct blockSeg *param){

	int numWords = param->size;//number of words in this block
	int j;//the word we're scanning
	int prev;//this tracks what the word before it was (the type)

	if(param->status==READ_FIRST || param->status==FIRST_LAST){//if this block is the first of the column
		param->curr = param->toCompress[0];
		j=1;//we're starting to read the 2nd word (first one is automatically the current word)
		word_32 len = WORD_LENGTH-1;
		fwrite(&len, sizeof(word_32),1,param->colFile);//write the word length to file
	}
	else{
		j=0;
	}
	prev = getType(param->curr);//find the type of the previous compressed word (to see if we can concatenate)

	for(;j<numWords;j++){//go through every uncompressed word
		int next = getType(param->toCompress[j]);//the type of this word

		//the next word is a run of 0s
		if(next==ZERO_LIT){//this one and the one before it were both literal runs of zeros so put it in a new fill word of 0s
			if(prev==ZERO_LIT){
				param->curr = getZeroFill(WORD_LENGTH);
				prev = ZERO_RUN;
			}
			else if(prev==ZERO_RUN){//this one is a run of zeros and the one before it was already a fill of 0s
				//but the previous one is full (can't increment anymore) so we still need to make this a literal (keep as is)
				if(param->curr==getMaxZeroFill(WORD_LENGTH)){
					fwrite(&(param->curr), sizeof(word_32),1,param->colFile);
					param->curr = param->toCompress[j];
					prev = next;
				}
				else{//this means that we can increment the zero run in the previous spot
					param->curr += 1;
				}
			}
			else{//the current one is a zero literal but the word before has both 0s and 1s (we can't do anything), so just save as is and keep going
				fwrite(&(param->curr), sizeof(word_32),1,param->colFile);
				param->curr = param->toCompress[j];
				prev = next;
			}
		}
		else if(next==ONE_LIT){
			if(prev==ONE_LIT){//if the last one was a literal of ones and this one was too, put them together
				param->curr = getOneFill(WORD_LENGTH);
				prev = ONE_RUN;
			}
			else if(prev==ONE_RUN){//we want to increment the last one but we can't because it's full so
									//so we still have to keep the literal
				if(param->curr==getMaxOneFill(WORD_LENGTH)){
					fwrite(&(param->curr), sizeof(word_32),1,param->colFile);
					param->curr = param->toCompress[j];
					prev = next;
				}
				else{//the one before this was a fill of ones, so just increment the last fill
					param->curr += 1;
				}
			}
			else{//when the current one is a literal of all ones
				//if the previous one is not a run or literal of ones
				 //(either literal or run of zeros), just save as the literal that it is
				fwrite(&(param->curr), sizeof(word_32),1,param->colFile);
				param->curr = param->toCompress[j];
				prev = next;
			}
		}
		else{//the word is neither a run of 0s or a run of 1s so it's a literal --> just save and continue
			fwrite(&(param->curr), sizeof(word_32),1,param->colFile);
			param->curr = param->toCompress[j];
			prev = next;
		}
	}
	if(param->status==LAST_BLOCK || param->status==FIRST_LAST){//if this is the last block of the column
		fwrite(&(param->curr), sizeof(word_32),1,param->colFile);//write the last word
	}
}

/*
 * Returns minimum run of 0s (2) : 100000....10
 */
word_32 getZeroFill(){
	word_32 one = 1;
	word_32 ret = (0 | (one<<(WORD_LENGTH-one)))+2;
	return ret;
}

/**
 * Returns a maxed out run of 0s (101111...11)
 */
word_32 getMaxZeroFill(){
	word_32 ret = 0;
	int i;
	word_32 one = 1;
	ret |= (one<<(WORD_LENGTH-one));

	for(i=WORD_LENGTH-3;i>=0;i--){
		ret |= (one<<i);
	}

	return ret;
}

/**
 * Returns a maxed out run of 1s (111111...11)
 */
word_32 getMaxOneFill(){
	word_32 ret = 0;
	ret--;
	return ret;
}

/*
 * Returns type of word (see int constants in WAHCompressor.h)
 */
int getType(word_32 word){
	if(word == 0x0) return ZERO_LIT;
	word_32 oneLit = 0;
	int i;
	word_32 one = 1;
	for(i=0;i<WORD_LENGTH-1;i++){
		oneLit |= (one<<i);
	}
	if(word == oneLit) return ONE_LIT;
	word_32 runMin = 0;
	runMin |= (1<<(WORD_LENGTH-one));

	if(word>>(WORD_LENGTH-1) & one){
		if(word>>(WORD_LENGTH-2) & one){
			return ONE_RUN;
		}
		else{
			return ZERO_RUN;
		}
	}
	return LITERAL;
}

/*
 * Returns minimum run of 1s (2) : 110000....10
 */
word_32 getOneFill(){
	word_32 one = 1;
	word_32 ret =  getZeroFill() | (one<<(WORD_LENGTH-2));

	return ret;
}
