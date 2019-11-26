#include"comm.h"
#include <sys/time.h>
#include "stdio.h"
#include "stdlib.h"
#include "atomicops.h"
#include "readerwriterqueue.h"

int main()
{
	struct timeval t1, t2, t3, t4;
        gettimeofday(&t1, NULL);
	int shmid = GetShm(4096);
	//char *addr = shmat(shmid,NULL,0);
	moodycamel::ReaderWriterQueue<int> q(100, shmid);
        gettimeofday(&t2, NULL);

	int i = 0;
        
	while(i < 26)
	{
	  q.enqueue(i);
	}
        /*
        gettimeofday(&t3, NULL);
	shmdt(addr);
        gettimeofday(&t4, NULL);
	sleep(2);
        */
        float init_time = (t2.tv_sec - t1.tv_sec)*1000 + (t2.tv_usec - t1.tv_usec)/1000;
        float release_time = (t4.tv_sec - t3.tv_sec)*1000 + (t4.tv_usec - t3.tv_usec)/1000;
        printf("init_time : %f ms\n", init_time);
        printf("release_time : %f ms\n", release_time);
	return 0;
}





