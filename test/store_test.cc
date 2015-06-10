// basic test for log some messages and print logged pointer address.

#include <iostream>

#include "LibhdfsppLog.h"

using namespace iotools;

int main(int argc, char *argv[])
{
    if (argc != 3) {
        std::cout << "Usage: " << argv[0] 
                  << " <log file> <index file>" << std::endl;
        return 0;
    } 

    if (!Logger::startLog(argv[1], argv[2])) {
        std::cerr << "Failed to start logging." << std::endl;
        return 0; 
    }
    
    int a(1), b(1), c(1);
    LOG_OPEN_RET(&a);
    LOG_CLOSE_RET(&b);
    LOG_READ_RET(&c);
   
    return 0;
}
