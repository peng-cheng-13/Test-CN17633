#include"hdf5.h"
#include <stdlib.h>
#include <string.h>
#include <fstream>
#include <iostream>
#include <ctime>
#include <iomanip>
#include <chrono>
#include <functional>
#include <thread>
#include <string>
#include <random>
#include <stdio.h>
#include<vector>
#include <sys/time.h>

int main(int argc, char *argv[]) {
  int Nfiles = 1;
  printf("Write HDF5 file\n");

  hsize_t dimsf[2];
  herr_t status;
  int NX = 5;
  int NY = 6;
  int data[NX][NY];
  int i, j;
  for (i = 0; i < NX; i++) {
    for (j = 0; j < NY; j++) {
      data[i][j] = i + j;
    }
  }
  //Create HDF5 file
  for (int i = 0; i < Nfiles; i++) {
    hid_t file, dataset, datatype, dataspace;
    std::string filename = "file-" + std::to_string(i);
    filename += ".h5";
    printf("%s\n",filename.data());
  
    char abs_path[1024];
    realpath(filename.data(), abs_path);
    printf("Absolute path: %s\n", abs_path);

    file = H5Fcreate(abs_path, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
    dimsf[0] = NX;
    dimsf[1] = NY;
    dataspace = H5Screate_simple(2, dimsf, NULL);
    datatype = H5Tcopy(H5T_NATIVE_INT);
    dataset = H5Dcreate(file, "D1", datatype, dataspace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    status = H5Dwrite(dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, data);
    //Set attribute
    int temperature =  rand() % 100;
    hid_t aid = H5Screate(H5S_SCALAR);
    hid_t attr = H5Acreate(dataset, "Temperature", H5T_NATIVE_INT, aid, H5P_DEFAULT, H5P_DEFAULT);
    status = H5Awrite(attr, H5T_NATIVE_INT, &temperature);
    H5Sclose(aid);
    H5Aclose(attr);
    H5Sclose(dataspace);

    H5Tclose(datatype);
    H5Dclose(dataset);
    H5Fclose(file);
  }
    //Close

    printf("Write HDF5 file succeed\n");
  
  return 0;
}
