#include "comm.h"
#include "stdio.h"
#include "stdlib.h"
#include "atomicops.h"
#include "readerwriterqueue.h"
 
int main()
{
	//int shmid = CreateShm(4096);
 	int shmid = GetShm(4096);
        DestroyShm(shmid); 
	//char *addr = shmat(shmid,NULL,0);
        moodycamel::ReaderWriterQueue<int> q(100, shmid);
        printf("create queue\n");	
	int i = 0;
        int number;
	while(i++ < 26)
	{
                bool succeeded = q.try_dequeue(number);
                if (succeeded)
                  printf("recv number# %s\n",number);
		
	}
	//shmdt(addr);
        

	DestroyShm(shmid);
	return 0;
}
