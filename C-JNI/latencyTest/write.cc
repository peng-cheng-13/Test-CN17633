#include "Alluxio.h"
#include "Util.h"
#include <stdlib.h>
#include <string.h>
#include <fstream>
#include <iostream>
#include <ctime>
#include <iomanip>
#include <chrono>
#include <functional>
#include <thread>
#include "JNIHelper.h"
using namespace tdms;

void setarg(int num,...){
  va_list ap;
  va_start(ap,num);
  char* tmp1;
  char* tmp2;
  for(int i=0; i<num; i++){
    tmp1 = va_arg(ap, char*);
    tmp2 = va_arg(ap, char*);
    printf("varname is %s, type is %s \n",tmp1, tmp2);
  }
}



int main(int argc, char *argv[]){

	char* testfile = "/lustre/testfile-c-jni";
	//char* testfile = "/testfile-c-jni";
	TDMSClientContext acc;
	printf("Context successed \n");
	TDMSFileSystem stackFS(acc);
        jTDMSFileSystem client = &stackFS;
        printf("Init jTDMSFileSystem  successed \n");
  	// Create file output stream
	TDMSCreateFileOptions* options = TDMSCreateFileOptions::getCreateFileOptions();

        int iter = atoi(argv[1]);
	
        const int wsize = 16384;
	static double mydata[wsize];
	int i,j,k=0;
        for(i=0;i<wsize;i++)
                mydata[i]=i;

	if(client->exists(testfile))
                client->deletePath(testfile, false);

	std::chrono::duration<double> duration = std::chrono::duration<double>::zero();
        std::chrono::time_point<std::chrono::system_clock> startTime, stopTime;
	//startTime = std::chrono::system_clock::now();

	jFileOutStream fileOutStream = client->createFile(testfile, options);


        //Write file
        //int iter = 0;
	//while (iter < 10) {
	int numWrites = (1 << iter) * 8;
	startTime = std::chrono::system_clock::now();
	for(j=0;j<numWrites;j++){      
  	    fileOutStream->write(mydata, wsize*sizeof(double));
	}
        //printf("size of mydata is : %d \n",sizeof(mydata[1]));     
	stopTime =  std::chrono::system_clock::now();
  	duration = stopTime - startTime;
	int Writesize = wsize * sizeof(double) * numWrites / 1024 / 1024;
  	std::cout << "Write " << Writesize << " MB to TDMS in " << duration.count() << " seconds" << std::endl;

	//iter++;
	//}
	fileOutStream->close();
	delete fileOutStream;


	return 0;

}

