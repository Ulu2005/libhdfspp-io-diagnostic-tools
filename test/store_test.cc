// basic test for log some messages and print logged pointer address.

#include <iostream>

#include "LibhdfsppLog.h"

using namespace hdfs;

int main(int argc, char *argv[])
{
    if (argc != 3) {
        std::cout << "Usage: " << argv[0] 
                  << " <log file> <index file>" << std::endl;
        return 0;
    } 

    Logging::startLog(argv[1], argv[2]);
    
    int flags(1), bufferSize(1), replication(1), blockSize(1);
    int *fs(&flags), *ret(&bufferSize);
    char path[64];
    strcpy(path, "this is open path");

    LOG_OPEN();
    LOG_OPEN_RET(ret);
    LOG_CLOSE_RET(ret);
    LOG_READ_RET(ret);
   
    return 0;
}
