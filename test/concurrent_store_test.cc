// test for logging messages concurrently

#include <iostream>
#include <thread>
#include <string>

#include "LibhdfsppLog.h"

using namespace hdfs;

void test(int n);

int main(int argc, char *argv[])
{
    if (argc != 3) {
        std::cout << "Usage: " << argv[0] 
                  << " <log file> <index file>" << std::endl;
        return 0;
    } 

    Logging::startLog(argv[1], argv[2]);
     
    std::thread th[100]; 
    for (int i = 0; i < 100; ++i) {
        th[i] = std::thread(test, i);
    }

    for (int i = 0; i < 100; ++i) {
        th[i].join();
    }

    return 0;
}

void test(int n)
{
    char path[64];
    snprintf(path, sizeof(path), "crazy path #%d", n);
     
    int *fs(&n), *ret(&n);
    int flags(n), bufferSize(n), replication(n), blockSize(n);

    LOG_OPEN();
    LOG_OPEN_RET(ret);
}
