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

int main(int argc, char *argv[]){

        char* testfile = "/lustre/testfile-c-jni";
        //char* testfile = "/tf";
        
	TDMSClientContext acc;
        printf("Context successed \n");
        TDMSFileSystem stackFS(acc);
        jTDMSFileSystem client = &stackFS;
        printf("Init jTDMSFileSystem  successed \n");
	
	const int wsize = 16384;
	static double mydata[wsize];
	int iter = atoi(argv[1]);
        std::chrono::duration<double> duration = std::chrono::duration<double>::zero();
        std::chrono::time_point<std::chrono::system_clock> startTime, stopTime;
        int i,j,k=0;
        char content[] = "hello, alluxio!!";

	int numWrites = (1 << iter) * 8;
	//startTime = std::chrono::system_clock::now();
        jFileInStream fileInStream = client->openFile(testfile);
	startTime = std::chrono::system_clock::now();
        for(j=0;j<numWrites;j++){
        fileInStream->read(mydata, sizeof(mydata));
        }
        stopTime =  std::chrono::system_clock::now();
        duration = stopTime - startTime;
	int Writesize = wsize * sizeof(double) * numWrites / 1024 / 1024;
        std::cout << "Read " << Writesize << " MB from TDMS in " << duration.count() << " seconds" << std::endl;
        fileInStream->close();
        delete fileInStream;

        return 0;
}
