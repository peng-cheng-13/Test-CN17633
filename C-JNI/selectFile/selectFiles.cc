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
#include "mpi.h"
using namespace tdms;

int main(int argc, char *argv[]){

  MPI_Init(&argc, &argv);
  int rank,size;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  MPI_Status status;  
  
  TDMSClientContext acc;
  //printf("Context successed \n");
  TDMSFileSystem stackFS(acc);
  jTDMSFileSystem client = &stackFS;
  //printf("Init jTDMSFileSystem  successed \n");
  //Query Info
  const char* keylist[1];
  const char* valuelist[1];
  const char* typelist[1];
  keylist[0] = "size";
  valuelist[0] = "100";
  typelist[0] = "lt";
  int argsize = 1;
  std::chrono::duration<double> duration = std::chrono::duration<double>::zero();
  std::chrono::time_point<std::chrono::system_clock> startTime, stopTime;

  printf("Ready to query \n");
  startTime = std::chrono::system_clock::now();

  char** mydata = (char **)malloc(128 *  sizeof(char *));
  int numofpaths = client->selectFiles(mydata, keylist, valuelist, typelist, argsize, size, rank);
  std::cout << "Get query successed! "  << std::endl;
  stopTime =  std::chrono::system_clock::now();
  printf("Num of paths is %d\n", numofpaths);
  int len = sizeof(mydata);
  int i = 0;
  while(i < numofpaths) {
    printf("path-%d = %s\n", i, mydata[i]);
    i++;
  }

  duration = stopTime - startTime;
  std::cout << "Select files from TDMS in " << duration.count() << " seconds" << std::endl;
  free(mydata);
  MPI_Finalize();
  return 0;
}
