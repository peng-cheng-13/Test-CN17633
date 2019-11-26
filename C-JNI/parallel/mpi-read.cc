#include "Alluxio.h"
#include "Util.h"
#include <stdlib.h>
#include <string.h>
#include <fstream>
#include <iostream>
#include <ctime>
#include <iomanip>
#include <chrono>
#include <vector>
#include <functional>
#include <thread>
#include <unistd.h>
#include "JNIHelper.h"
#include "mpi.h"
using namespace tdms;

static const char *options = "f:F:v:V:t:T:q:Q:n:N";
static char *inputfile = 0;
static char *varname = 0;
static char *vartype = 0;
static char *query = 0;
static int np = 0;

static void parseArgs(int argc, char **argv){
  extern char *optarg;
  int c;
  while ((c = getopt(argc, argv, options)) != -1) {
    switch (c) {
      case 'f' :
      case 'F' : {
        inputfile = optarg;
        break;
      }
      case 'v' :
      case 'V' : {
        varname = optarg;
        break;
      }
      case 't' :
      case 'T' : {
        vartype = optarg;
        break;
      }
      case 'q' :
      case 'Q' : {
        query = optarg;
        break;
      }
      case 'n' :
      case 'N' : {
        np = atoi(optarg);
        break;
      }
      default : break;
    }
  }
}

int main(int argc, char *argv[]){

  parseArgs(argc, argv);
  if (inputfile == 0 ) {
    std::cerr << "Usage:\n" << " -f input-file-name\n"
        "-v var name to query\n"
        "-t var type to query\n"
        "-q query-conditions-in-a-single-string\n"
        "-n number-of-threads\n"
        "\n e.g.:  ./query -f /file -v v1 -t DOUBLE -q 'px < 0.3' -n 4\n"
        << std::endl;
    return -1;
  }
  printf("File: %s, Var: %s, Type: %s, Query: %s, N: %d\n", inputfile, varname, vartype, query, np);

  MPI_Init(&argc, &argv);
  int rank,size;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  MPI_Status status;
  printf("MPI size is %d, my rank is %d\n",size,rank);
  char* testfile = "/lustre/testfile-c-jni";
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
    
         
  //Query Info
  char* var = "test";
  double qmax = 22369800;
  double qmin = 22369700;
  printf("Ready to query \n");
  jFileInStream fileInStream = client->queryFile(testfile, var, qmax, qmin, false);
       
  std::cout << "Get query successed! "  << std::endl;
  //jFileInStream fileInStream = client->openFile(testfile); 
             
  startTime = std::chrono::system_clock::now();

  //Query processing
  std::vector<double> tmp;
  //tmp.push_back(520);
  i = rank;
  k = 1;
  long pos;
  long maxseekpos = fileInStream->maxSeekPosition();
  printf("File length is %ld \n", maxseekpos);
  while(k > 0) {
  pos = i*131072*8;
  if (pos < maxseekpos)
    fileInStream->seek(pos);
  else
    break;
  k = fileInStream->read(mydata, 131072*8); 
  if(i < 10) {
    //printf("Rank %d start with pos %ld\n",rank,pos);
    printf("mydata[0] is %f \n",mydata[0]);
  }
  if(k < 0)
     break;
  i = i + size;
  for(j=0;j<131072;j++){
    if(mydata[j] >= qmin && mydata[j] <= qmax)
       tmp.push_back(mydata[j]);
    }
  }

  //Rank 0 collects the data
  MPI_Barrier(MPI_COMM_WORLD);
  int count = tmp.size();
  int *rcounts = (int *)malloc(size*sizeof(int));
  MPI_Gather(&count,1,MPI_INT,rcounts,1,MPI_INT,0,MPI_COMM_WORLD);
     
  if(rank == 0) {
    for(i=0;i<size;i++)
      printf("Receive counts from rank %d is %d \n",i,rcounts[i]);
  }

  //MPI_IO        
  MPI_File fh;
  MPI_File_open(MPI_COMM_WORLD,"data",MPI_MODE_CREATE|MPI_MODE_WRONLY,MPI_INFO_NULL,&fh);       
  MPI_File_write_ordered(fh,&tmp[0],tmp.size(),MPI_DOUBLE,&status);    
  MPI_File_close(&fh);

  stopTime =  std::chrono::system_clock::now();
  duration = stopTime - startTime;
  if (rank ==0) {
    std::cout << "Read 16GB from TDMS in " << duration.count() << " seconds" << std::endl;
    long bd = 16384/duration.count();
    std::cout << "Read Bandwidth is " <<  bd << " MB/s" << std::endl;
  }
  fileInStream->close();
  delete fileInStream;
  MPI_Finalize();
  return 0;
}
