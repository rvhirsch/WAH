#include "DataRead.h"
#include "Control.h"
//#include "RawBitmapReader.c"

const char* HOST = "archive.ics.uci.edu";
const char* PATH = "/ml/machine-learning-databases/abalone/abalone.data";

/*
 * Main verifies that we got the right number of command-line inputs,
 * then establishes a connection with the specified HTTP server.  It
 * constructs the appropriate GET request packet, sends it to the server,
 * then transfers control to the writeFile function.
 *
 * @author Brad Richards - just the socket connection stuff, though
 */
int main_networking(int argc, char *argv[])
{
//    char *data;          // A pointer to some bytes of data
    int sockfd;             // Our socket
    struct addrinfo hints;  // Helps tell getaddrinfo what we want
    struct addrinfo *them;  // will point to the results

    // Set up the hints addrinfo structure to describe the characteristics
    // of the socket and address we're after.
    memset(&hints, 0, sizeof hints); // make sure the struct is empty
    hints.ai_family = AF_UNSPEC;     // don't care IPv4 or IPv6
    hints.ai_socktype = SOCK_STREAM; // TCP stream sockets, please
    hints.ai_flags = AI_PASSIVE;     // Fill in IP for me

    // Ask getaddrinfo to work its magic.  If all goes well, it sets up
    // an address structure for the destination machine (server).
    int status = getaddrinfo(HOST, "http", &hints, &them);
    if (status != 0) {
        fprintf(stderr,"Error in getaddrinfo: %s\n", gai_strerror(status));
        exit(1);
    }

    // Now it's time to create the socket.  It should have the same
    // characteristics as the machine on the far side, in terms of
    // address family and protocol, so we can borrow that info from
    // the "them" structure.
    sockfd = socket(them->ai_family, them->ai_socktype, them->ai_protocol);

    // See if it worked
    if (sockfd == -1) {
        perror("Attempt to create socked failed");
        exit(1);
    }

    // Ask connect to pair our socket with the one on the far side.
    if((connect(sockfd, them->ai_addr, them->ai_addrlen)) == -1){
        perror("Connection attempt failed");
    }

    // ---------------------------------------------------------------
    // At this point we're connected to the other side.  We can build
    // our packet (as a string), and use send() to transmit it to the
    // server.
    // ---------------------------------------------------------------
    printf("Sending GET to %s\n", HOST);
    char request[200];
    sprintf(request, "GET %s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", PATH, HOST);

    int bytesSent = send(sockfd, request, strlen(request), 0);
    if ( bytesSent == -1) {
        perror("sending GET");
        exit(1);
    }

    // Even if the send doesn't fail, it's possible it didn't send ALL
    // of the data.  We'll check, but give up if only some went out.
    if ( bytesSent != strlen(request) ) {
	fprintf(stderr, "Didn't get entire GET sent.  Giving up.\n");
        exit(1);
    }

    // ---------------------------------------------------------------
    // GET was successfully sent if we get this far.  Watch for the
    // response, being careful to trim off the HTML header.
    // ---------------------------------------------------------------
    processResponse(sockfd);

    // TODO: see comments below
    // read in data until buffer X% full
    // compress data in buffer, while continuing to read in data to other (100-X)% of buffer
    // put compressed data in recent data buffer
    // when recent data buffer full, move to old data buffer
    // when old data buffer full, move to disk
    // query from recent data buffer first

    return 0;
}
