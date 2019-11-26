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
  char* keylist[1];
  char* valuelist[1];

  keylist[0] = "size";
  valuelist[0] = "100";

  int argsize = 1;
  std::chrono::duration<double> duration = std::chrono::duration<double>::zero();
  std::chrono::time_point<std::chrono::system_clock> startTime, stopTime;

  startTime = std::chrono::system_clock::now();


  client->addDatasetInfo("D1", keylist, valuelist, argsize);
  printf("Ready to query \n");
  char* testfile = "/f1";
  client->setDatasetInfo(testfile);

  stopTime =  std::chrono::system_clock::now();

  duration = stopTime - startTime;
  std::cout << "Add Dataset Info to TDMS in " << duration.count() << " seconds" << std::endl;

  MPI_Finalize();
  return 0;
}
