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
    
    int a(1024), b(1), c(1);
    char str[1024];
    strcpy(str, "this is open path");

    LOG_OPEN(&a, str, 11, 21, 31, 41);
    LOG_OPEN_RET(&a);
    LOG_CLOSE_RET(&b);
    LOG_READ_RET(&c);
   
    return 0;
}
