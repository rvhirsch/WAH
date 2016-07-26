/*
 * @author Brad Richards - socket stuff
 */

#include "DataRead.h"

int counter = 0;
int lineNum = 1;

// Some constants
const int READBUFSIZE = 60;
const int BACKUPBUFSIZE = 30;
const int MAXDATASIZE = 1000;      // max number of bytes we can get at once
const char* SEPARATOR = "\r\n\r\n"; // Packet header separator

struct abaloneLine readBuffer[READBUFSIZE];
struct abaloneLine backupBuffer[BACKUPBUFSIZE];

/*
 * The writeFile function is passed a socket and a file descriptor.  It
 * receives data from the socket and writes it to the file until the
 * data runs out.  The first receive should contain the header of the
 * HTTP response packet, as well as some data.
 */
int processResponse(int sockfd)
{
//    int numbytes;
    char buf[MAXDATASIZE+1];
    int bytesRecvd;

    buf[MAXDATASIZE] = '\0';	// null-terminate buffer

    // Read the first batch of data.  Assume that we at least get the header.
    if ((bytesRecvd=recv(sockfd, buf, MAXDATASIZE, 0)) == -1) {
        perror("recv");
        exit(1);
    }

    /*
     * Find the blank line separating the header from the data, and dump
     * the data that follows it to the file.
     */
    char *data = strstr(buf, SEPARATOR);
    if( data == NULL )
        return 0;       // didn't find the blank line

    printf("\n\nFIRST BUFFER: %s :END BUFFER", buf);

    data = data+strlen(SEPARATOR); // move past the <CRLF>'s

    printf("\n\nSECOND BUFFER: %s :END BUFFER", buf);

    int totalBytes = bytesRecvd - (data-buf);
    fwrite(data, 1, totalBytes, stdout);

//    printf("Got %d bytes of data after header.\n", totalBytes);

    do {
        if ((bytesRecvd=recv(sockfd, buf, MAXDATASIZE, 0)) == -1) {
            perror("recv");
            exit(1);
        }
//        if (0 == bytesRecvd) {
//            continue;
//        }

        printf("\n\nTHIRD BUFFER: %s :END BUFFER", buf);

        buf[bytesRecvd] = '\0';

        if (bytesRecvd > 0) {
            totalBytes += bytesRecvd;
    	    fwrite(buf, 1, bytesRecvd, stdout);
        }

        printf("\n\nFINAL BUFFER: %s :END BUFFER", buf);

        printf("\n\nTO DO: DISC BUFFER\n");
        pthread_t readbuf;
//        pthread_t discLine;
        pthread_create(&readbuf, NULL, fill_abalone_buffer, &buf);
//        pthread_create(&discLine, NULL, discretize_abalone_data, &readBuffer);
        pthread_join(readbuf, NULL);
//        pthread_join(discLine, NULL);
        printf("DISC BUFFER DONE\n\n");

    } while (bytesRecvd > 0);

    return totalBytes;
}

void* fill_abalone_buffer(void *buf) {
	printf("IN FAB THREAD\n");
	int i = 0;
	int j = 0;
	char *q = strtok(buf, ", \n\r");
	printf("q = %s\n", q);
	while (q != NULL) {
		printf("IN WHILE LOOP\n");
		if (strcmp(q, "M") == 0 || strcmp(q, "F") == 0 || strcmp(q, "I") == 0) {
			printf("q = %s\n", q);
			if (i < READBUFSIZE) {
				// set up struct
				readBuffer[i].sex = q;
				readBuffer[i].length = atof(strtok(NULL, ", "));
				readBuffer[i].diameter = atof(strtok(NULL, ", "));
				readBuffer[i].height = atof(strtok(NULL, ", "));
				readBuffer[i].wholeWeight = atof(strtok(NULL, ", "));
				readBuffer[i].shuckedWeight = atof(strtok(NULL, ", "));
				readBuffer[i].visceraWeight = atof(strtok(NULL, ", "));
				readBuffer[i].shellWeight = atof(strtok(NULL, ", "));
				readBuffer[i].rings = atoi(strtok(NULL, ", "));

				printf("%d: %s, %f, %f, %f, %f, %f, %f, %f, %d\n",
						i, readBuffer[i].sex, readBuffer[i].length, readBuffer[i].diameter,
						readBuffer[i].height, readBuffer[i].wholeWeight, readBuffer[i].shuckedWeight,
						readBuffer[i].visceraWeight, readBuffer[i].shellWeight, readBuffer[i].rings);

				i++;
			}
			else if (j < BACKUPBUFSIZE) {
				// set up struct
				backupBuffer[j].sex = q;
				backupBuffer[j].length = atof(strtok(NULL, ", "));
				backupBuffer[j].diameter = atof(strtok(NULL, ", "));
				backupBuffer[j].height = atof(strtok(NULL, ", "));
				backupBuffer[j].wholeWeight = atof(strtok(NULL, ", "));
				backupBuffer[j].shuckedWeight = atof(strtok(NULL, ", "));
				backupBuffer[j].visceraWeight = atof(strtok(NULL, ", "));
				backupBuffer[j].shellWeight = atof(strtok(NULL, ", "));
				backupBuffer[j].rings = atoi(strtok(NULL, ", "));

				printf("%d: %s, %f, %f, %f, %f, %f, %f, %f, %d\n",
						i, backupBuffer[i].sex, backupBuffer[i].length, backupBuffer[i].diameter,
						backupBuffer[i].height, backupBuffer[i].wholeWeight, backupBuffer[i].shuckedWeight,
						backupBuffer[i].visceraWeight, backupBuffer[i].shellWeight, backupBuffer[i].rings);

				j++;
			}
		}
		q = strtok(NULL, ", \n\r");
	}
	return NULL;
}

void* discretize_abalone_data(void *buf) {
	char *q = strtok(buf, ",\n\r");
	while (q != NULL) {
		// check if start of line
		if (!strcmp(q, "M")) {
			printf("%d, M: 100 ", lineNum);
			counter = 0;
			lineNum++;
		}
		else if (!strcmp(q, "F")) {
			printf("%d, F: 010 ", lineNum);
			counter = 0;
			lineNum++;
		}
		else if (!strcmp(q, "I")) {
			printf("%d, I: 001 ", lineNum);
			counter = 0;
			lineNum++;
		}
		// else check other data
		else {
			counter++;
			double num = atof(q);	// for comparisons
			if (counter == 1) {
				if (num < 0.3) {
					printf("100 ");
				}
				else if (num < 0.6) {
					printf("010 ");
				}
				else {
					printf("001 ");
				}
			}
			else if (counter == 2) {
				if (num < 0.2) {
					printf("1000 ");
				}
				else if (num < 0.4) {
					printf("0100 ");
				}
				else if (num < 0.6) {
					printf("0010 ");
				}
				else {
					printf("0001 ");
				}
			}
			else if (counter == 3) {
				if (num < 0.1) {
					printf("100 ");
				}
				else if (num < 0.2) {
					printf("010 ");
				}
				else {
					printf("001 ");
				}
			}
			else if (counter == 4) {
				if (num < 0.3) {
					printf("1000 ");
				}
				else if (num < 0.6) {
					printf("0100 ");
				}
				else if (num < 0.9) {
					printf("0010 ");
				}
				else {
					printf("0001 ");
				}
			}
			else if (counter == 5) {
				if (num < 0.1) {
					printf("1000 ");
				}
				else if (num < 0.3) {
					printf("0100 ");
				}
				else if (num < 0.5) {
					printf("0010 ");
				}
				else {
					printf("0001 ");
				}
			}
			else if (counter == 6) {
				if (num < 0.1) {
					printf("1000 ");
				}
				else if (num < 0.2) {
					printf("0100 ");
				}
				else if (num < 0.3) {
					printf("0010 ");
				}
				else {
					printf("0001 ");
				}
			}
			else if (counter == 7) {
				if (num < 0.2) {
					printf("100 ");
				}
				else if (num < 0.4) {
					printf("010 ");
				}
				else {
					printf("001 ");
				}
			}
			else if (counter == 8) {
				if (num < 5) {
					printf("1000 ");
				}
				else if (num < 10) {
					printf("0100 ");
				}
				else if (num < 15) {
					printf("0010 ");
				}
				else {
					printf("0001 ");
				}
			}
			else {
				printf("??? ");
			}
		}

		if (counter == 8)
			printf("\n");

		q = strtok(NULL, ",\n\r");
	}
	printf("\n");

	return NULL;
}

void* disc_by_line(void *file) {
	printf("IN DISC BY LINE\n");
	char line[256];
	int i=0;
	while (fgets(line, sizeof(line), file)) {
		printf("i=%d: %s", i, line);
		i++;
	}
	return NULL;
}

void* discretize_line(void *buf) {
	printf("???????????????????????????????????????????\n");

	int i=0;
	char *p = strtok(buf, "\n\r");
	while (p != NULL) {
		printf("i = %d: %s\n", i, p);

		discretize_abalone_data(p);

		p = strtok(NULL, "\n\r");
		i++;
	}
	return NULL;
}
