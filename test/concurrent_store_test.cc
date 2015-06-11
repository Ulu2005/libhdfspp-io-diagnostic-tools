// test for logging messages concurrently

#include <iostream>
#include <thread>

#include "LibhdfsppLog.h"

using namespace iotools;

void test(int n);

int main(int argc, char *argv[])
{
    if (argc != 3) {
        std::cout << "Usage: " << argv[0] 
                  << " <log file> <index file>" << std::endl;
        return 0;
    } 

    Logging::startLog(argv[1], argv[2]);
    
     
    std::thread th[10]; 
    for (int i = 0; i < 10; ++i) {
        th[i] = std::thread(test, i);
    }

    for (int i = 0; i < 10; ++i) {
        th[i].join();
    }

    return 0;
}

void test(int n)
{
    LOG_OPEN_RET(n);
}
