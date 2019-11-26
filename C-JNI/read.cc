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

        char* testfile = "/query-output";
        //char* testfile = "/tf";
	TDMSClientContext acc;
        printf("Context successed \n");
        TDMSFileSystem stackFS(acc);
        jTDMSFileSystem client = &stackFS;
        printf("Init jTDMSFileSystem  successed \n");
	
	static double mydata[131072];
        std::chrono::duration<double> duration = std::chrono::duration<double>::zero();
        std::chrono::time_point<std::chrono::system_clock> startTime, stopTime;
        int i,j,k=0;
        char content[] = "hello, alluxio!!";
        //Query Info
        char* var = "test";
        double qmax = 30000100;
        double qmin = 30000010;
        printf("Ready to query \n");
        //jFileInStream fileInStream = client->queryFile(testfile, var, qmax, qmin);
        std::cout << "Get query successed! "  << std::endl;
        jFileInStream fileInStream = client->openFile(testfile); 
        startTime = std::chrono::system_clock::now();
        fileInStream->read(mydata, 131072*8);
        for(i=0;i<100;i++)
	  printf("mydata [%d] = %.2f ",i,mydata[i]);
        printf("\n");
        //for(j=0;j<8192;j++){
        //        fileInStream->read(mydata, 131072*8);
	//	if(j==0)
	//		std::cout << "mydata [20] = " << mydata[20]  << std::endl;
        //}
        stopTime =  std::chrono::system_clock::now();
        duration = stopTime - startTime;
        std::cout << "Read 16GB from TDMS in " << duration.count() << " seconds" << std::endl;
        long bd = 16384/duration.count();
        std::cout << "Read Bandwidth is " <<  bd << " MB/s" << std::endl;
        fileInStream->close();
        delete fileInStream;
        return 0;
}
