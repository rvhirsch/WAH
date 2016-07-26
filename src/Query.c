#include <stdio.h>
#include <stdlib.h>
#include "Query.h"
#include "RawBitmapReader.h"
#include <sys/stat.h>
#include <unistd.h>
#include "WAHCompressor.h"
#include <string.h>

word_32 **cols;//the compressed columns loaded into memory
int *sz;//array that saves the size of each column (in words, -1 if hasn't been loaded)
word_32 *result;//result of the first range
word_32 *result2;//result of the second range
word_32 *resultCopy;//used for range queries to OR result with another col and save into result


/*
 * Loads a column into col d of the cols array in memory from the designated file pointer
 */
void loadCol(FILE *fp,int d){
	cols[d] = (word_32 *) malloc(sizeof(word_32) *sz[d]);
	fread(&(cols[d][0]),sizeof(word_32),sz[d],fp);
	fclose(fp);
}


/*
 * ANDs two columns together (col0 AND col1) and saves into ret arg
 * sz0 and sz1 are sizes of the columns we're ANDing
 */
int AND(word_32 *ret, word_32 * col0, int sz0, word_32 *col1, int sz1){
	int c0;//the word number we're scanning from col0
	int c1;//the word number we're scanning from col1
	int d;//the spot we're saving into the result
	c0 = c1 = 1;//track which word we're looking at
	d=0;//start saving into the first spot

	word_32 w0 = col0[c0++];//get the first word from first col
	word_32 w1 = col1[c1++];//get the first word from second col

	//get each of their word type Compression.h type definitions
	int t0 = getType(w0);
	int t1 = getType(w1);


	while(c0<=sz0 && c1<=sz1){
		word_32 toAdd;//this is the result word we're creating from w0 and w1
		if(t0 < 2 && t1 < 2){//two literals
			toAdd = litANDlit(w0,w1);

			//update both
			w0 = col0[c0++];
			t0 = getType(w0);
			w1 = col1[c1++];
			t1 = getType(w1);

		}
		else if(t0 < 2 || t1 < 2){//one literal, one fill
			if(t0 < 2){//w0 is the literal
				toAdd = fillANDlit(&w1,&t1,w0);
				w0 = col0[c0++];//update the literal
				t0 = getType(w0);
			}
			else{//w1 is the literal
				toAdd = fillANDlit(&w0,&t0,w1);
				w1 = col1[c1++];//update the literal
				t1 = getType(w1);
			}
		}
		else{//two fills
			 if ((w0 << 2) < (w1 << 2)){//w0 is smaller
				toAdd = fillANDfill(w0,t0,&w1,&t1);
				w0 = col0[c0++];//update the smaller fill
				t0 = getType(w0);
			 }
			 else if ((w0 << 2) > (w1 << 2)){//w1 is smaller
				toAdd = fillANDfill(w1,t1,&w0,&t0);
				w1 = col1[c1++];//update the smaller fill
				t1 = getType(w1);
			 }
			 else{//special case, equal fills (can be treated as literals)
				 toAdd = litANDlit(w0,w1);
				 //update both
				 w0 = col0[c0++];
				 t0 = getType(w0);
				 w1 = col1[c1++];
				 t1 = getType(w1);
			 }
		}

		if(d>=1){//if this isn't the first word, append it to the end of the resulting column we're building
			append(ret,toAdd,&d);

		}
		else{//special case --> first word (can't append because first word is wordLength)
			ret[++d] = toAdd;//just add it
		}
	}

	return d+1;//the number of words we just wrote
}


/*
 * ORs two columns together (col0 AND col1) and saves into ret arg
 * sz0 and sz1 are sizes of the columns we're ORing
 */
int OR(word_32 *ret,word_32 *col0,int sz0, word_32 *col1, int sz1){
		int c0=1;//track which word in col0 we're looking at
		int c1=1;//track which word in col1 we're looking at
		int d=0;//track which spot in the resulting array we're saving into

		//get the first word from each column
		word_32 w0 = col0[c0++];
		word_32 w1 = col1[c1++];

		//and figure out their types (see type definition in Compression.h)
		int t0 = getType(w0);
		int t1 = getType(w1);

		while(c0<=sz0 && c1<=sz1){
			word_32 toAdd;//this is the resulting word from ORing w0 and w1
			if(t0 < 2 && t1 < 2){//two literals
				toAdd = litORlit(w0,w1);
				//update both
				w0 = col0[c0++];
				t0 = getType(w0);
				w1 = col1[c1++];
				t1 = getType(w1);
			}
			else if(t0 < 2 || t1 < 2){//one literal, one fill
				if(t0 < 2){//w0 is the literal
					toAdd = fillORlit(&w1,&t1,w0);
					w0 = col0[c0++];//update the literal
					t0 = getType(w0);
				}
				else{//w1 is the literal
					toAdd = fillORlit(&w0,&t0,w1);
					w1 = col1[c1++];//update the literal
					t1 = getType(w1);
				}
			}
			else{//two fills
				 if ((w0 << 2) < (w1 << 2)){//w0 is smaller
					toAdd = fillORfill(w0,t0,&w1,&t1);
					w0 = col0[c0++];//update the smaller fill
					t0 = getType(w0);
				 }
				 else if ((w0 << 2) > (w1 << 2)){//w1 is smaller
					toAdd = fillORfill(w1,t1,&w0,&t0);
					w1 = col1[c1++];//update the smaller
					t1 = getType(w1);
				 }
				 else{//special case, equal fills (can be treated as literals)
					 toAdd = litORlit(w0,w1);
					 //update both
					 w0 = col0[c0++];
					 t0 = getType(w0);
					 w1 = col1[c1++];
					 t1 = getType(w1);
				 }
			}

			if(d>=1){//if we're in the middle of the columns, just append
				append(ret,toAdd,&d);
			}
			else{//if we're on the first word, can't append (first word is wordLength)
				ret[++d] = toAdd;
			}
		}

		return d+1;//the total number of words result now has
}


/*
 * Adds the wordToAdd to the end (d=last added position) of the addTo sequence
 * wordToAdd will be consolidated into position if possible and if not (or the leftover) will go into d+1
 */
void append(word_32 *addTo, word_32 wordToAdd, int *d){

	int prevT = getType(addTo[*d]);//type of the last added

	if(prevT==LITERAL){//there's no way to consolidate
		addTo[++(*d)] = wordToAdd;
		return;
	}
	int addT = getType(wordToAdd);//type of the one we're adding

	if(prevT==ZERO_RUN){
		if(addT == ZERO_RUN){//both zero runs so we might be able to consolidate if there's enough room
			word_32 minCheck = getZeroFill()-2;//helps to check the stopping condition (as long as there are still fills left)
			word_32 maxCheck = getMaxZeroFill();
			while(wordToAdd>minCheck && addTo[(*d)]<maxCheck){
				addTo[(*d)]++;
				wordToAdd--;
			}
			if(wordToAdd==minCheck){
				return;//successfully added everything to previous
			}
			else{//stopped because ran out of space
				if(wordToAdd==minCheck+1){//there was exactly one left so switch to literal before adding
					wordToAdd = 0;
				}
				addTo[++(*d)] = wordToAdd;
				return;
			}
		}
		else if(addT == ZERO_LIT){//we can probably just add this one lit to the previous run
			if(addTo[(*d)]<getMaxZeroFill()){//not maxed out yet, so just add it
				addTo[(*d)]++;
			}
			else{//maxed out so can't consolidate
				addTo[++(*d)] = wordToAdd;//save into the next spot
				return;
			}
		}
		else{//can't consolidate
			addTo[++(*d)] = wordToAdd;
			return;
		}
	}
	else if(prevT==ZERO_LIT){
		if(addT==ZERO_LIT){//consolidate two literals into one fill
			addTo[(*d)] = getZeroFill();
			return;
		}
		else if(addT==ZERO_RUN){
			if(wordToAdd<getMaxZeroFill()){//not maxed out yet, so add it
				addTo[(*d)] = wordToAdd+1;
				return;
			}
			else{//maxed out
				addTo[(*d)+1] = addTo[(*d)];
				addTo[(*d)++] = wordToAdd;
				return;
			}
		}
		else{//can't consolidate
			addTo[++(*d)] = wordToAdd;
			return;
		}
	}
	else if(prevT==ONE_RUN){
		if(addT == ONE_RUN){//both run of ones so might be able to consolidate
			word_32 minCheck = getOneFill()-2;//helps to check the stopping condition (as long as there are still fills left)
			word_32 maxCheck = getMaxOneFill();
			while(wordToAdd>minCheck && addTo[(*d)]<maxCheck){
				addTo[(*d)]++;
				wordToAdd--;
			}
			if(wordToAdd==minCheck){
				return;//successfully added everything to previous
			}
			else{//stopped because ran out of space
				if(wordToAdd==minCheck+1){//there was exactly one left so switch to literal before adding
					wordToAdd = getZeroFill()-3;
				}
				addTo[++(*d)] = wordToAdd;
				return;
			}
		}
		else if(addT == ONE_LIT){
			if(addTo[(*d)]<getMaxOneFill()){//previous isn't maxed out so just add it
				addTo[(*d)]++;
				return;
			}
			else{//maxed out so can't consolidate
				addTo[++(*d)] = wordToAdd;
				return;
			}
		}
		else{
			addTo[++(*d)] = wordToAdd;
			return;
		}
	}
	else{//prev is lit of ones
		if(addT==ONE_LIT){
			addTo[(*d)] = getOneFill();
			return;
		}
		else if(addT==ONE_RUN){
			if(wordToAdd<getMaxOneFill()){//not maxed out
				addTo[(*d)] = wordToAdd+1;
			}
			else{//maxed out so can't consolidate
				addTo[(*d)+1] = addTo[(*d)];
				addTo[(*d)++] = wordToAdd;
				return;
			}
		}
		else{//can't consolidate
			addTo[++(*d)] = wordToAdd;
			return;
		}
	}
}



/* Performs an OR on 2 fills
 * Updates the larger fill for the future
 * Returns the resulting word
 */
word_32 fillORfill(word_32 smallFill, int smallT, word_32 *bigFill, int *bigT){
	word_32 ret;
	word_32 sub = ((smallFill<<2)>>2);//what we are going to subtract from the larger

	if(smallT == ZERO_RUN && *bigT == ZERO_RUN){//if both 0 runs, must return small run of 0s
		ret = smallFill;
	}
	else{
		ret = getOneFill()-2+sub;//build run of 1s of length sub (number of runs in smallFill)
	}
	*bigFill -= sub;//subtract that from the larger

	//check to see if we subtracted too much
	if(((*bigFill<<2)>>2)==1){//need to change to literal
		if(*bigT==ZERO_RUN){
			*bigFill = 0;//literal run of zeros
			*bigT = ZERO_LIT;
		}
		else{//update the larger fill for the future
			*bigFill = getZeroFill() -3;//literal run of ones
			*bigT = ONE_LIT;
		}
	}

	return ret;
}

/*
 * Performs an AND on 2 fills
 * Updates the larger of the fills for the future
 * Returns the resulting word
 */
word_32 fillANDfill(word_32 smallFill, int smallT, word_32 *bigFill, int *bigT){
	word_32 ret;
	word_32 sub = ((smallFill<<2)>>2);//what we are going to subtract from the larger (number of runs the smallFill represents)

	if(smallT == ONE_RUN && *bigT == ONE_RUN){//the only way to return run of 1s is if both are 1s
		ret = smallFill;
	}
	else{//otherwise we're returning a run of zeros of length smallFill was
		ret = getZeroFill()-2+sub;//build run of 0s of length sub (number of runs in smallFill)
	}

	*bigFill -= sub;

	if(((*bigFill<<2)>>2)==1){
		if(*bigT==ZERO_RUN){
			*bigFill = 0;//literal run of zeros
			*bigT = ZERO_LIT;
		}
		else{
			*bigFill = getZeroFill() -3;//literal run of ones
			*bigT = ONE_LIT;
		}
	}

	return ret;
}


/*
 * Performs an OR on a fill and a literal word
 * Updates the fill for the future
 * Returns the resulting word
 */
word_32 fillORlit(word_32 *fill, int *fillT, word_32 lit){
	word_32 ret;//the word to be returned

	if(*fillT == ONE_RUN){//whatever we or with a run of 1s will be 1 so just return lit of ones (later)
		ret = getZeroFill() -3;//literal run of 1s
	}
	else{//otherwise we have a run of 1s so the result will be whatever the literal is
		ret = lit;
	}

	if(((*fill<<2)>>2)==2){//we need to turn it into a literal
		if(*fillT == ZERO_RUN){
			*fill = 0;//literal run of 0s
			*fillT = ZERO_LIT;
		}
		else{
			*fill = getZeroFill() -3;//literal run of ones
			*fillT = ONE_LIT;
		}
	}
	else{//otherwise we can decrement without a problem
		*fill -= 1;
	}
	return ret;
}

/*
 * Performs an AND on a fill and a literal
 * Updates the fill for the future
 * Returns the resulting word
 */
word_32 fillANDlit(word_32 *fill, int *fillT, word_32 lit){
	word_32 ret;
	if(*fillT == ZERO_RUN){//whatever we and with a run of 0s will be 0 so just return 0 (later)
		ret = 0;
	}
	else{//otherwise we have a run of 1s so the result will be whatever the literal is
		ret = lit;
	}

	if(((*fill<<2)>>2)==2){//we need to turn it into a literal
		if(*fillT == ZERO_RUN){
			*fill = 0;//literal run of 0s
			*fillT = ZERO_LIT;
		}
		else{
			*fill = getZeroFill() -3;//literal run of ones
			*fillT = ONE_LIT;
		}
	}
	else{//otherwise we can decrement without a problem
		*fill -= 1;
	}
	return ret;
}

/*
 * Performs an OR between 2 literals and returns the resulting word
 */
word_32 litORlit(word_32 lit1, word_32 lit2){
	return lit1 | lit2;//just or them together
}

/*
 * Performs an AND between 2 literals and returns the resulting word
 */
word_32 litANDlit(word_32 lit1, word_32 lit2){
	return lit1 & lit2;//just and them together
}


/*
 * Runs the designated file of queries on the bitmap file folder
 */
void runQueries(char bitmapFile[]){

	int n=0;
	while(1){
		char name[BUFF_SIZE];
		snprintf(name,sizeof(name),"%s%d%s",bitmapFile,n,".dat");//build the file name based on colNum

		//counting the number of columns there are in that folder
		if(access(name,F_OK) != -1){
			n++;
			continue;
		}
		else{
			break;
		}
	}
	//TODO: figure out a better size estimate
	result = (word_32 *) malloc(sizeof(word_32)* 100000000);//result of the first range
	result2 = (word_32 *) malloc(sizeof(word_32)* 100000000);//result of the second range
	resultCopy = (word_32 *) malloc(sizeof(word_32)* 100000000);//used for range queries to OR result with another col and save into result

	cols = (word_32 **) malloc(sizeof(word_32 *) * n);//the actual column
	sz = (int *) malloc(sizeof(int)*n);//how many words are in each column (empty --> -1)
	int m;
	for(m=0;m<n;sz[m++]=-1);//no columns have been loaded

	FILE *fp = fopen(QUERY_FILE, "r");//open the query file
	if(fp == NULL){
		fprintf(stderr,"Can't open query file\n");
	}
	else{
		//building the folder for query results
		//the results will be saved in a folder where the query file was
		//Ex. if we ran "Queries/query1.txt"
		//the results will be "Queries/Query_results_query1.txt/qID_0.dat" etc

		char folder[] = "QueryResults_";
		char *dir = getDir(QUERY_FILE,folder);
		mkdir(dir,S_IRWXU);
		char qF[BUFF_SIZE];
		int i=0;
		for(;i<BUFF_SIZE;qF[i++]='\0');
		strcpy(qF,dir);
		strcat(qF,"/qID_");

		int qid = 0;//which query we're on
		char c = '\n';//new line char
		while(c!=EOF){//keep scanning each query
			fscanf(fp, "%d", &qid);//query id
			getc(fp);//comma
			word_32 *range1 = parseRange(fp);//first range
			getc(fp);//comma
			word_32 *range2 = parseRange(fp);//second range
			executeQuery(bitmapFile,qF,qid,range1,range2);//actual query execution
			free(range1);
			free(range2);
			c=getc(fp);//get new line
		}
	}
}

/*
 * Executes the query on the specified 2 ranges using the compressed bitmap in bitmapFolder
 * Writes the result into dir folder
 */
void executeQuery(char *bitmapFolder, char *dir, word_32 id, word_32 range1[] , word_32 range2[]){
	char buff[BUFF_SIZE];
	snprintf(buff,sizeof(buff),"%s%d%s",dir,id,".dat");//build the file name based on colNum
	FILE *dest = fopen(buff,"wb");//this is the destination file for the query result
	int i;

	//go through the first range and make sure every column that we're going to need is loaded
	for(i=1;i<=range1[0];i++){
		if(sz[range1[i]]==-1){//column hasn't been loaded
			char file[BUFF_SIZE];
			snprintf(file,sizeof(file),"%s%d%s",bitmapFolder,range1[i],".dat");//build the file name based on colNum
			FILE *fp = fopen(file,"rb");

			if(fp==NULL){//out of range query probably
				printf("Can't open file");
				return;
			}

			//just trying to find the size of the file (to know how many words are in it)
			struct stat st;
			stat(file, &st);
			int sz1 = st.st_size;
			sz1 /= sizeof(word_32);
			sz[range1[i]] = sz1;
			loadCol(fp,range1[i]);//load each file into memory first
		}
	}
	//go through the second range and make sure every column that we're going to need is loaded
	for(i=1;i<=range2[0];i++){
		if(sz[range2[i]]==-1){//column hasn't been loaded yet
			char file[BUFF_SIZE];
			snprintf(file,sizeof(file),"%s%d%s",bitmapFolder,range2[i],".dat");//build the file name based on colNum
			FILE *fp = fopen(file,"rb");

			if(fp==NULL){//out of range query probably
				printf("Can't open bitmap file");
				return;
			}

			struct stat st;
			stat(file, &st);
			int sz1 = st.st_size;
			sz1 /= sizeof(word_32);
			sz[range2[i]] = sz1;
			loadCol(fp,range2[i]);//load each file into memory first
		}
	}

	int c = 1;//column tracker (to go through range)

	//start by assigning first column in range1 to results

	int k;
	for(k=0;k<sz[range1[c]];k++){
		result[k]=cols[range1[c]][k];
	}

	int resultWords = sz[range1[c]];//the number of words in that column
	//and by assigning first column in range 2 to results2

	for(k=0;k<sz[range2[c]];k++){
			result2[k]=cols[range2[c]][k];
		}
	int resultWords2 = sz[range2[c]];//the number of words in that column


	//go through every column in the range and OR them together
	while(c<range1[0]){
		if(c==1){//first or (just first two columns)
			resultWords = OR(result,cols[range1[1]],sz[range1[1]],cols[range1[2]],sz[range1[2]]);
		}
		else{//if we're not on the first OR, we're ORing the previous result (a copy of it) and saving it into the result
			for(k=0;k<resultWords;k++){
				resultCopy[k]=result[k];
			}
			OR(result,resultCopy,resultWords,cols[range1[c+1]],sz[range1[c+1]]);
		}
		c++;
	}

	//start over with second range
	c = 1;
	while(c<range2[0]){
		if(c==1){//first or (just first two columns)
			resultWords2 = OR(result2,cols[range2[1]],sz[range2[1]],cols[range2[2]],sz[range2[2]]);
			c++;
		}
		else{//if we're not on the first OR, we're ORing the previous result (a copy of it) and saving it into the result
			for(k=0;k<resultWords2;k++){
				resultCopy[k]=result2[k];
			}

			OR(result2,resultCopy,resultWords2,cols[range2[c]],sz[range2[c]]);
		}
		c++;
	}

	//and now just AND the two results together
	for(k=0;k<resultWords;k++){
		resultCopy[k]=result[k];
	}

	int d =	AND(result,resultCopy,resultWords,result2,resultWords2);

	//write to file
	fwrite(result,sizeof(word_32),d,dest);
}

/*
 * Parses a range from a given file and returns array of the number of ints followed by the specified ints
 * Example: if query file says "9,11"
 * Returns [3,9,10,11] --> [numCols, col1, col2, col3]
 */
word_32 *parseRange(FILE *qFile){
	int i =0;
	fscanf(qFile, "%d", &i);//first column of the range
	getc(qFile);//comma
	int j = 0;
	fscanf(qFile, "%d", &j);//second column of the range
	int sz = j-i+2;//size of the range array (number of columns + 1 for size)
	word_32 *ret = (word_32 *) malloc(sizeof(word_32) * (sz));
	int k;
	ret[0] = sz-1;//save number of columns
	for(k=1;k<sz;ret[k++] = i++);//fill array with the right range

	return ret;
}
