#define _GNU_SOURCE
#include <stdio.h>
#include <dlfcn.h>
#include <errno.h>

// Client side implementation of UDP client-server model
//#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>

// for fprintf
#include <stdarg.h>
//#include <libioP.h>

#define PORT     8080
#define MAXLINE 1024

static FILE *(*real_fopen)(const char *filename, const char *mode) = NULL;
static int (*real_fclose)(FILE *fp) = NULL;
static int (*real_fprintf)(FILE *stream, const char *format, ...) = NULL;
static size_t (*real_fwrite)(const void *ptr, size_t size, size_t nmemb, FILE *stream) = NULL;
static ssize_t (*real_write)(int fd,const void *buf,size_t len) = NULL;
static int (*real_remove)(const char *filename) = NULL;
static int (*real_close)(int fd) = NULL;
static ssize_t (*real_read)(int fildes, void *buf, size_t nbyte) = NULL;

__attribute__((constructor))
void
my_lib_init(void)
{

    real_fopen = dlsym(RTLD_NEXT,"fopen");
    real_fclose = dlsym(RTLD_NEXT, "fclose");
    real_fprintf = dlsym(RTLD_NEXT, "fprintf");
    real_fwrite = dlsym(RTLD_NEXT, "fwrite");
    real_remove = dlsym(RTLD_NEXT,"remove");
}

FILE *fopen(const char *filename, const char *mode)
{
    FILE *fp;

    // do whatever special stuff ...

    fp = real_fopen(filename, mode);
    printf("fopen worked!\n");
    char message[200];
    char fidString[10];
    sprintf(fidString, "fopen %d ", fp);
    strcat(message, fidString);
    strcat(message, filename);
    sendMessage(message);
    // do whatever special stuff ...

    return fp;
}

int
fclose(FILE *fp)
{
    int i;
    char message[200];

    i = real_fclose(fp);
    printf("fclose worked!\n");
    sprintf(message, "fclose %d", fp);
    sendMessage(message);

    return i;
}

size_t
fwrite(const void *ptr, size_t size, size_t nmemb, FILE *stream)
{
    char message[200];
    size_t num;

    num = real_fwrite(ptr, size, nmemb, stream);
    printf("fwrite worked!\n");
    sprintf(message, "fwrite %d", stream);
    sendMessage(message);

    return num;
}

int
fprintf(FILE *stream, const char *format, ...)
{
    va_list arg;
    int done;

    va_start (arg, format);
    done = vfprintf(stream, format, arg);
    va_end (arg);

    char message[200];
    printf("fprintf worked!\n");
    sprintf(message, "fprintf %d", stream);
    sendMessage(message);

    return done;
}

int
remove(const char *filename)
{
    int ret;
    char message[200];

    // do whatever special stuff ...
    printf("remove worked!\n");
    sprintf(message, "remove %s", filename);
    sendMessage(message);
    ret = real_remove(filename);

    // do whatever special stuff ...

    return ret;
}

int
sendMessage(char *message)
{
    int sockfd;
    struct sockaddr_in     servaddr;

    // Creating socket file descriptor
    if ( (sockfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0 ) {
        perror("socket creation failed");
        exit(EXIT_FAILURE);
    }

    memset(&servaddr, 0, sizeof(servaddr));

    // Filling server information
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(PORT);
    servaddr.sin_addr.s_addr = INADDR_ANY;

    int n, len;

    sendto(sockfd, (const char *)message, strlen(message),
        MSG_CONFIRM, (const struct sockaddr *) &servaddr,
            sizeof(servaddr));
    printf("message sent\n");
    close(sockfd);
    return 0;
}
