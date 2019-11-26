#define _GNU_SOURCE
#include <stdio.h>
#include <dlfcn.h>
#include <errno.h>

#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdarg.h>

#include "hashmap.h"
#include <assert.h>

#define PORT     8080
#define MAXLINE 1024
#define KEY_MAX_LENGTH (256)


static int (*real_open)(const char *file,int flags,int mode) = NULL;
static int (*real_creat)(const char *file, mode_t mode) = NULL;
static ssize_t (*real_write)(int fd,const void *buf,size_t len) = NULL;
static int (*real_remove)(const char *filename) = NULL;
static int (*real_close)(int fd) = NULL;
static ssize_t (*real_read)(int fildes, void *buf, size_t nbyte);
static int socketid = -1;
struct hashmap map;

__attribute__((constructor))
void
my_lib_init(void)
{

    real_open = dlsym(RTLD_NEXT,"open");
    real_open = dlsym(RTLD_NEXT,"creat");
    real_write = dlsym(RTLD_NEXT,"write");
    real_remove = dlsym(RTLD_NEXT,"remove");
    real_close = dlsym(RTLD_NEXT,"close");
    real_read = dlsym(RTLD_NEXT, "read");
    hashmap_init(&map, hashmap_hash_string, hashmap_compare_string, 100);


}

int
creat(const char *file, mode_t mode)
{
    int fd;
    int error;
    /* do whatever special stuff ...*/


    fd = real_creat(file, mode);



    char* fidString = (char*)malloc(KEY_MAX_LENGTH * sizeof(char));
    /*Use fopen-fd as key*/
    sprintf(fidString, "fopen%d", fd);

    printf("creat worked\n");

    char* value = (char *)malloc(200 * sizeof(char));
    char* filepath = file;
    printf("Current size of hashmap is %d\n", hashmap_size(&map));
    hashmap_put(&map, fidString, filepath);
    printf("hashmap put %s, %s\n", fidString, filepath);
    value = hashmap_get(&map, fidString);
    printf("Get value from hashmap with key %s, value is %s\n", fidString, value);

    return fd;
}

int
open(const char *file,int flags,int mode)
{
    int fd;
    int error;
    /* do whatever special stuff ...*/


    fd = real_open(file,flags,mode);



    char* fidString = (char*)malloc(KEY_MAX_LENGTH * sizeof(char));
    /*Use fopen-fd as key*/
    sprintf(fidString, "fopen%d", fd);

    printf("open worked\n");

    char* value = (char *)malloc(200 * sizeof(char));
    char* filepath = file;
    printf("Current size of hashmap is %d\n", hashmap_size(&map));
    hashmap_put(&map, fidString, filepath);
    printf("hashmap put %s, %s\n", fidString, filepath);
    value = hashmap_get(&map, fidString);
    printf("Get value from hashmap with key %s, value is %s\n", fidString, value);

    return fd;
}

ssize_t
read(int fildes, void *buf, size_t nbyte)
{
    ssize_t ret;
    printf("read worked!\n");
    ret = real_read(fildes, buf, nbyte);
    return ret;
}

ssize_t
write(int fd,const void *buf,size_t len)
{
    static int __thread in_self = 0;
    int sverr;
    ssize_t ret;
    int hasherror;

    ++in_self;

    if (in_self == 1)
        printf("mywrite: fd=%d buf=%p len=%ld\n",fd,buf,len);
    printf("write worked!");
    ret = real_write(fd,buf,len);

    /* preserve errno value for actual syscall -- otherwise, errno may
    // be set by the following printf and _caller_ will get the _wrong_
    // errno value*/
    sverr = errno;

    if (in_self == 1)
        printf("mywrite: fd=%d buf=%p ret=%ld\n",fd,buf,ret);

    --in_self;

    /* restore correct errno value for write syscall*/
    errno = sverr;

    /*Update fileList*/   
    char* fidString = (char*)malloc(KEY_MAX_LENGTH * sizeof(char));
    sprintf(fidString, "fopen%d", fd);
    char* newkey = (char*)malloc(KEY_MAX_LENGTH * sizeof(char));
    char* value = (char *)malloc(200 * sizeof(char));
    sprintf(newkey, "fwrite%d", fd);

    printf("Current size of hashmap is %d\n", hashmap_size(&map));
    printf("Try to get value from key %s\n", fidString);   
    value = hashmap_get(&map, fidString);
    printf("Get value from hashmap with key %s, value is %s\n", fidString, value);
    hashmap_put(&map, newkey, value);
    printf("Put a write file to the hashmap, %s, %s\n", newkey, value);
    
    return ret;
}
int
remove(const char *filename)
{   
    int ret;
    /*
    if (real_remove == NULL)
        real_remove = dlsym(RTLD_NEXT,"remove");
    */

    // do whatever special stuff ...
    printf("remove worked!\n");
    ret = real_remove(filename);

    // do whatever special stuff ...
    char *message = (char *)malloc(200 * sizeof(char));
    sprintf(message, "delete:%s", filename);
    sendMessage(message);

    return ret;
}

int
close(int fd)
{
    int ret;
    /*
    if (real_close == NULL)
        real_close = dlsym(RTLD_NEXT,"close");
    */


    ret = real_close(fd);
    /* do whatever special stuff ...*/
    printf("close worked on %d\n", fd);
    char *message = (char *)malloc(210 * sizeof(char));
    /*Avoid close the socket recursively*/
    
    if (fd != socketid) {
      
      char* fidString = (char*)malloc(KEY_MAX_LENGTH * sizeof(char));
      sprintf(fidString, "fwrite%d", fd);
      char *value = (char *)malloc(200 * sizeof(char));
      value = hashmap_get(&map, fidString);
      sprintf(message, "create:%s", value);
      printf("Current size of hashmap is %d\n", hashmap_size(&map));
      printf("get key once, key is %s, value is %s \n", fidString, value);
      
      if (value != NULL)
        sendMessage(message);
    }
    free(message);
    return ret;
}

int
sendMessage(char *message)
{
    int sockfd;
    struct sockaddr_in     servaddr;

    /* Creating socket file descriptor*/
    if ( (sockfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0 ) {
        perror("socket creation failed");
        exit(EXIT_FAILURE);
    }
    socketid = sockfd;
    printf("socketid is %d\n", socketid);
    memset(&servaddr, 0, sizeof(servaddr));

    /* Filling server information*/
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
