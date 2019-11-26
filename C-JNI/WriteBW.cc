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

struct sdouble{
  double v1;
  double v2;
};

int main(int argc, char *argv[]){

        const int wsize = 8192;
	//char* testfile = "/lustre/testfile-c-jni";
	char* testfile = "/testfile-c-jni5";
	TDMSClientContext acc;
	printf("Context successed \n");
	TDMSFileSystem stackFS(acc);
        jTDMSFileSystem client = &stackFS;
        printf("Init jTDMSFileSystem  successed \n");
  	// Create file output stream
	TDMSCreateFileOptions* options = TDMSCreateFileOptions::getCreateFileOptions();
        //options->setFileInfo(1,"test","DOUBLE");
        //options->setFileInfo(2,"test","DOUBLE","id","DOUBLE");
        options->setFileInfo(3,"test","DOUBLE","id","DOUBLE","geneid","DOUBLE");
	//options->setDataAccessPattern("SCATTER");
	if(client->exists(testfile))
                client->deletePath(testfile, false);
	jFileOutStream fileOutStream = client->createFile(testfile, options);
	//Write 4GB file 
	//static double mydata[8192][2];
	sdouble *mydata = (sdouble *)malloc(wsize * sizeof(sdouble));
	//static sdouble mydata[wsize];
  	std::chrono::duration<double> duration = std::chrono::duration<double>::zero();
  	std::chrono::time_point<std::chrono::system_clock> startTime, stopTime;
  	int i,j,k=0;
  	//for(i=0;i<16384;i++)
	//	mydata[i]=i;
        //fileOutStream->buildIndex(4, false);
	startTime = std::chrono::system_clock::now();
        //setarg(2,"test","DOUBLE", "v2", "INT");

        //for(j=0;j<100;j++){

        for(i=0;i<wsize;i++) {
               mydata[i].v1 = 1;
               mydata[i].v2 = 3;
        }

        startTime = std::chrono::system_clock::now();
	for(j=0;j<81920;j++){      
  	    //fileOutStream->write(mydata, sizeof(mydata));
  	    fileOutStream->write(mydata, wsize * sizeof(sdouble));
	}
        printf("size of mydata is : %d \n",sizeof(mydata[1]));     
  	fileOutStream->close();
        if(fileOutStream->shouldIndex()){
          printf("Index compute time is %f ms\n",fileOutStream->getBlockMax());
          printf("Index write time is %f ms\n",fileOutStream->getBlockMin());
        }else{
          printf("Error in generating index! \n");
        }
	stopTime =  std::chrono::system_clock::now();
  	duration = stopTime - startTime;
  	std::cout << "Write 5GB to TDMS in " << duration.count() << " seconds" << std::endl;
	long bd = 10*1024/duration.count();
	std::cout << "Write Bandwidth is " <<  bd << "MB/s" << std::endl;
	delete fileOutStream;
	return 0;

}

