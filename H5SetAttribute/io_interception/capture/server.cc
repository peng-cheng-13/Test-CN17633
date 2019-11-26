#include <stdio.h> 
#include <stdlib.h> 
#include <unistd.h> 
#include <string.h> 
#include <string>
#include <sys/types.h> 
#include <sys/socket.h> 
#include <arpa/inet.h> 
#include <netinet/in.h> 
#include "Alluxio.h"
#include "Util.h"
#include "JNIHelper.h"

#define PORT     8080 
#define MAXLINE 1024
#define MOUNT_POINT "/home/condor/lustre"
#define NEW_POINT "/lustre/"

using namespace tdms;
int main() { 

    /*Init TDMS*/
    TDMSClientContext acc;
    TDMSFileSystem stackFS(acc);
    jTDMSFileSystem client = &stackFS;
    TDMSCreateFileOptions* options = TDMSCreateFileOptions::getCreateFileOptions();
    printf("Init jTDMSFileSystem  successed \n");

    int sockfd; 
    char buffer[MAXLINE]; 
    struct sockaddr_in servaddr, cliaddr; 

    if ( (sockfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0 ) { 
        perror("socket creation failed"); 
        exit(EXIT_FAILURE); 
    } 
      
    memset(&servaddr, 0, sizeof(servaddr)); 
    memset(&cliaddr, 0, sizeof(cliaddr)); 

    servaddr.sin_family    = AF_INET; // IPv4 
    servaddr.sin_addr.s_addr = INADDR_ANY; 
    servaddr.sin_port = htons(PORT); 


    if ( bind(sockfd, (const struct sockaddr *)&servaddr,  
            sizeof(servaddr)) < 0 ) 
    { 
        perror("bind failed"); 
        exit(EXIT_FAILURE); 
    } 
    printf("Length of MOUNT_POINT is %d\n",sizeof(MOUNT_POINT));
    while (1) {
      int len, n; 
      n = recvfrom(sockfd, (char *)buffer, MAXLINE,  
                MSG_WAITALL, ( struct sockaddr *) &cliaddr, 
                (socklen_t *) &len); 
      buffer[n] = '\0';
      std::string fpath = buffer;
      if (fpath.find("create:") < fpath.length()) {
        if (fpath.find(MOUNT_POINT) < fpath.length()) {
          fpath.replace(0, sizeof("create:") + sizeof(MOUNT_POINT) - 1, NEW_POINT);
        }
        printf("Client create file: %s\n", fpath.data());
     
        if(client->exists(fpath.data()))
          client->deleteAlluxioPath(fpath.data(),true);
 
        client->loadMetadata(fpath.data());
      } else if (fpath.find("delete:") < fpath.length()) {
         if (fpath.find(MOUNT_POINT) < fpath.length()) {
          fpath.replace(0, sizeof("delete:") + sizeof(MOUNT_POINT) - 1, NEW_POINT);
        }
        printf("Client delete file: %s\n", fpath.data());
        client->deletePath(fpath.data());
      }
    }
    return 0; 

}
