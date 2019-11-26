#include "wrapped_calcer.h"
#include <stdbool.h>
#include <iostream>
#include <stdlib.h>

int main(int argc, char* args[]) {
  /*
  char* catFeatures[1] = {"mProjectPP"};
  float floatFeatures[5];
  floatFeatures[0] = 16;
  floatFeatures[1] = 4222384.0;
  floatFeatures[2] = 2;
  floatFeatures[3] = 1;
  floatFeatures[4] = 6;
  */
  //Expected ouput : 0
  //std::vector<std::string> catFeatures = {"mProjectPP"};
  //std::vector<float> floatFeatures = {16, 4222384.0, 2, 1, 6}; 

  //Expected ouput : 2
  //std::vector<std::string> catFeatures = {"mJPEG"};
  //std::vector<float> floatFeatures = {1, 3827844.0, 1, 1, 0};
  

  //Expected ouput : 1
  //std::vector<std::string> catFeatures = {"mConcatFit"};
  //std::vector<float> floatFeatures = {1, 338034.0, 1, 1, 1};

  //Input
  std::vector<std::string> catFeatures;
  catFeatures.push_back(args[1]);
  std::vector<float> floatFeatures;
  int i;
  for (i = 2; i < argc; i++) {
    float tmp = (float)atof(args[i]);
    floatFeatures.push_back(tmp);
  }

  ModelCalcerWrapper calcer("/home/condor/alluxio/journal/AdaptiveStorage/model.bin");
  std::cout << calcer.Calc(floatFeatures, catFeatures) << std::endl;
  return 0;
  /*
  if (!LoadFullModelFromFile(tmpmodelHandle, "model.cbm")) {
    printf("LoadFullModelFromFile error message: %s\n", GetErrorString());
  }
  if (!CalcModelPrediction(
        tmpmodelHandle,
        1,
        &catFeatures, 1,
        &floatFeatures, 5,
        &result, 1
    )) {
    printf("CalcModelPrediction error message: %s\n", GetErrorString());
  } else {
    printf("Prediction %f\n", result[0]);
  }
  ModelCalcerDelete(tmpmodelHandle);
  */
}
