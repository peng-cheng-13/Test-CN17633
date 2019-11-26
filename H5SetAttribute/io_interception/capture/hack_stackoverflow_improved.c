#define _GNU_SOURCE
#include <stdio.h>
#include <dlfcn.h>
#include <errno.h>
/*
#include <map>
#include <set>
#include <vector>
*/
static int (*real_open)(const char *file,int flags,int mode) = NULL;
static ssize_t (*real_write)(int fd,const void *buf,size_t len) = NULL;
static int (*real_remove)(const char *filename) = NULL;
static int (*real_close)(int fd) = NULL;
static ssize_t (*real_read)(int fildes, void *buf, size_t nbyte);
static int num = 0;
/*
static std::map<int, std::string> tmpmap;
static std::set<int> writeList;
static std::vector<std::string> updateList;
static std::vector<std::string> removeList;
*/
__attribute__((constructor))
void
my_lib_init(void)
{

    real_open = dlsym(RTLD_NEXT,"open");
    real_write = dlsym(RTLD_NEXT,"write");
    real_remove = dlsym(RTLD_NEXT,"remove");
    real_close = dlsym(RTLD_NEXT,"close");
    real_read = dlsym(RTLD_NEXT, "read");
    num = 10;
}

int
open(const char *file,int flags,int mode)
{
    int fd;
    num++;
    // do whatever special stuff ...

    printf("open worked! %d, file path is %s\n", num, file);
    fd = real_open(file,flags,mode);
    
    // do whatever special stuff ...
    //std::string tmpfile = file;
    //tmpmap.insert(std::pair<int, std::string>(fd, tmpfile));
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

    ++in_self;

    if (in_self == 1)
        printf("mywrite: fd=%d buf=%p len=%ld\n",fd,buf,len);
    printf("write worked!");
    ret = real_write(fd,buf,len);

    // preserve errno value for actual syscall -- otherwise, errno may
    // be set by the following printf and _caller_ will get the _wrong_
    // errno value
    sverr = errno;

    if (in_self == 1)
        printf("mywrite: fd=%d buf=%p ret=%ld\n",fd,buf,ret);

    --in_self;

    // restore correct errno value for write syscall
    errno = sverr;
    //writeList.insert(fd);
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
    //std::string tmpfile = filename;
    //removeList.push(tmpfile);
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
    // do whatever special stuff ...
    printf("close worked!\n");
    ret = real_close(fd);
    // do whatever special stuff ...
    return ret;
}
/*
int 
Update () {
    std::set<int>::iterator it;
    for (it = writeList.begin(); it != writeList.end(); it++) {
      if (tmpmap.find(it) != tmpmap.end()) {
        int index = tmpmap.find(it);
        std::string tmpfile = tmpmap[index];
        updateList.push(tmpfile);
      }
    }

}
*/
