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
#include <string>
#include <random>
#include <stdio.h>
#include<vector>
using namespace tdms;

struct geo_data{
  double v1;
  double v2;
  double v3;
};

double getRandData(int min,int max)
{
      double m1=(double)(rand()%101)/101;
      min++;
      double m2=(double)((rand()%(max-min+1))+min);    
      m2=m2-1;
      return m1+m2;     
}

int main(int argc, char *argv[]){
	if(argc < 3){
		printf("invalid argment\n");
		return -1;
	}
	// Initilize input argument 
	int nrow = atoi(argv[1]);
	int ncol = atoi(argv[2]);
	int i,j;
	int num = 0;
	int nGO = 60000;
	int nbases = 3000000000;
	int min_probe_length = 25;
	int max_probe_length = 1000;
	char tmp[20];
	int start_id = nrow * (atoi(argv[4]) - 1);
	std::string datapath = argv[3];
	std::string jobid = argv[4];
	std::string GEO_table = datapath + "/GEO-" + argv[1] + "-" + argv[2] + "-" + jobid + ".txt";
	std::string GO_table  = datapath + "/GO-" + argv[1] + "-" + argv[2] + "-" + jobid + ".txt";	
	std::string GM_table  = datapath + "/GeneMetaData-" + argv[1] + "-" + argv[2] + "-" + jobid + ".txt";
	std::string PM_table  = datapath + "/PatientMetaData-" + argv[1] + "-" + argv[2] + "-" + jobid + ".txt";

	std::cout << "Path of file is " << GEO_table << std::endl;
	
	if(GEO_table.find("/home/condor/alluxio-data")<GEO_table.length()){
		GEO_table = GEO_table.replace(0,26,"/");
		GO_table = GO_table.replace(0,26,"/");
		GM_table = GM_table.replace(0,26,"/");
		PM_table = PM_table.replace(0,26,"/");
	}
        
	std::cout << "Path of file is " << GEO_table << std::endl;
	
	char* GEO_file = (char*)GEO_table.data(); 
	char* GO_file = (char*)GO_table.data();
	char* GM_file = (char*)GM_table.data();
	char* PM_file = (char*)PM_table.data();
	
	// Initilize TDMS
	TDMSClientContext acc;
	TDMSFileSystem stackFS(acc);
        jTDMSFileSystem client = &stackFS;
        printf("Init jTDMSFileSystem  successed \n");
	TDMSCreateFileOptions* options = TDMSCreateFileOptions::getCreateFileOptions();
	options->setFileInfo(3,"test","DOUBLE","id","DOUBLE","geneid","DOUBLE");
	// Create GEO matrix: min and max got from actual GEO data
	jFileOutStream fileOutStream; 
	fileOutStream = client->createFile(GEO_file, options);
        //fileOutStream->buildIndex(4);
        static std::vector<geo_data> geoarray;
        geo_data gtmp;
        static std::string GEO_data = "geneid, patientid, expression value\n";
        static char* GEO_data_final;
	double n1;
	
	for(i=0;i<nrow;i++){
               	for(j=0;j<ncol;j++){
                       	n1=getRandData(-186677, 2005274);
                       	gtmp.v1 = (double) i;
                        gtmp.v2 = (double) j;
                        gtmp.v3 = n1;
                        geoarray.push_back(gtmp);
			num ++;
			if(num >= 409600){
		        	fileOutStream->write(&geoarray[0], 409600*sizeof(geoarray));
				num = 0;
				geoarray.clear();
			}
               	}
        }
	fileOutStream->write(&geoarray[0], geoarray.size()*sizeof(geoarray));
        printf("Size of geo array is %d \n",sizeof(geoarray));
        geoarray.clear();
	fileOutStream->close();
        
	/* Create GO matrix: assume 60K terms. Currently randomly distributed
	fileOutStream = client->createFile(GO_file, options);
	static std::string GO_data = "geneid, goid, whether gene belongs to go\n";	
	static char* GO_data_final;
	int n2 = 0;
	num = 0;
	for(i=0;i<nrow;i++){
                for(j=0;j<nGO;j++){
                        n2=(int)getRandData(0, 2);
                        sprintf(tmp,"%d, %d, %d\n",i,j,n2);
                        GO_data.append(tmp);
                        num ++;
                        if(num >= 20000){
                                GO_data_final = (char*)GO_data.data();
                                fileOutStream->write(GO_data_final, strlen(GO_data_final));
                                num = 0;
                                GO_data = "";
                        }
                }
        }
	GO_data_final = (char*)GO_data.data();
        fileOutStream->write(GO_data_final, strlen(GO_data_final));
        fileOutStream->close();
	*/	

	//Create gene metadata matrix: gene id (same as in GEO and GO matrix -- currently just an index)
	//t: target gene if any (again an index)
	//p: position
	//l: length
	//func: function (an index for now)
	fileOutStream = client->createFile(GM_file, options);
	static std::string GM_data = "id, target, position, length, function\n";
	static char* GM_data_final;
	int t,p,l,func;
	double th;
	num = 0;
	for(i=0;i<nrow;i++){
		if (getRandData(0, 1) < 0.5)
			t = -1;
		else
			t = (int)getRandData(0, nrow);
		p = (int)getRandData(0, nbases);
		l = (int)getRandData(min_probe_length, max_probe_length);
		func = (int)getRandData(0, 1000);
		sprintf(tmp,"%d, %d, %d, %d, %d\n",i,t,p,l,func);
		GM_data.append(tmp);
		num ++;
                if(num >= 20000){
                        GM_data_final = (char*)GM_data.data();
                        fileOutStream->write(GM_data_final, strlen(GM_data_final));
                        num = 0;
                        GM_data = "";
                }
	}
	GM_data_final = (char*)GM_data.data();
        fileOutStream->write(GM_data_final, strlen(GM_data_final));
        fileOutStream->close();

	//Create patient metadata matrix: sample if (same as in GEO matrix)
	//a: age
	//g: gender
	//z: zipcode
	//d: disease
	//dr: drug response
	fileOutStream = client->createFile(PM_file, options);
	static std::string PM_data = "id, age, gender, zipcode, disease, drug response\n";
	static char* PM_data_final;
	int a,g,z,d;
	double dr;
	for(i=0;i<ncol;i++){
		a = (int)getRandData(15, 95);
		g = (int)getRandData(0, 2);
		z = (int)getRandData(1,100000);
		d = (int)getRandData(0, 20);
		dr = getRandData(0, 100);
		sprintf(tmp,"%d, %d, %d, %d, %d, %.2f\n",i,a,g,z,d,dr);
		PM_data.append(tmp);
		num ++;
                if(num >= 20000){
                        PM_data_final = (char*)PM_data.data();
                        fileOutStream->write(PM_data_final, strlen(PM_data_final));
                        num = 0;
                        PM_data = "";
                }
	}
	PM_data_final = (char*)PM_data.data();
        fileOutStream->write(PM_data_final, strlen(PM_data_final));
	fileOutStream->close();

	delete fileOutStream;
	return 0;
}

