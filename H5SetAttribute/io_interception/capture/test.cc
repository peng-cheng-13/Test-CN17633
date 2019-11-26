#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>

int main()
{
    char filename[20];
    sprintf(filename, "test.txt");
    int fd = open(filename, O_RDWR | O_CREAT | O_TRUNC, 0666);
    if (fd == -1)
    {
        exit(1);
    }

    write(fd, "test", 4);
    lseek(fd, 0, SEEK_SET);
    char buf[5] = {0};
    read(fd, buf, 4);
    close(fd);

    printf("%s\n", buf);
    return 0;
}





