
#include "Control.h"

void runQueries(char[]);
void executeQuery(char *,char *,word_32,word_32[],word_32[]);

word_32 *parseRange(FILE *);
void loadCol(FILE *,int);

int AND(word_32 *, word_32 *,int, word_32 *,int);
word_32 fillANDfill(word_32, int, word_32 *, int *);
word_32 litANDlit(word_32,word_32);
word_32 fillANDlit(word_32 *, int *, word_32);

int OR(word_32 *,word_32 *,int,word_32 * ,int);
word_32 fillORfill(word_32, int, word_32 *, int *);
word_32 litORlit(word_32,word_32);
word_32 fillORlit(word_32 *, int *, word_32);

void append(word_32 *,word_32,int *);
