// A basic reader for log file. 

#include <iostream>
#include <string>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <unistd.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include "log.pb.h"

namespace pbio = ::google::protobuf::io;

void printLogInfo(const hadoop::hdfs::log &msg);
std::string getLogType(const hadoop::hdfs::log &msg);

int main(int argc, const char* argv[]) {
    if (argc != 3) {
        std::cout << "Usage: " << argv[0] 
                  << " <log file> <index file>" << std::endl;
        return 0;
    } 

    int logFd = open(argv[1], O_RDONLY);
    int indexFd = open(argv[2], O_RDONLY);
    if ((logFd == -1) || (indexFd == -1)) {
        std::cerr << "Failed to open file." << std::endl;
        return 0;
    }

    pbio::ZeroCopyInputStream* logFile = new pbio::FileInputStream(logFd);
    FILE* indexFile = fdopen(indexFd, "r"); 
    
    int size;
    char buf[32];
    hadoop::hdfs::log msg;

    while(!feof(indexFile)) {
        if (fgets(buf, sizeof(buf), indexFile) == NULL) {
            break;
        } 

        size = std::atoi(buf);
        if (!msg.ParseFromBoundedZeroCopyStream(logFile, size)) {
            std::cerr << "failed to parse file" << std::endl;
            break;    
        }

        printLogInfo(msg);
    }

    close(logFd);
    close(indexFd);
    return 0;
}

void printLogInfo(const hadoop::hdfs::log &msg)
{
    std::cout << "date: " << msg.date() << std::endl; 
    std::cout << "time: " << msg.time() << std::endl; 
    std::cout << "thread id: " <<  msg.threadid() << std::endl; 
    std::cout << "type: " << getLogType(msg) << std::endl; 
    if (msg.type() == hadoop::hdfs::log_FuncType_OPEN) {
        std::cout << "path: " << msg.path() << std::endl; 
    }

    std::cout << "argu size: " << msg.argument_size() << std::endl; 
    for (int i = 0; i < msg.argument_size(); ++i) {
        if (msg.argument(i) > (long)(1 << 31)) {
            std::cout << std::hex << "\t" << msg.argument(i) << std::endl; 
        } else {
            std::cout << std::dec << "\t" << msg.argument(i) << std::endl; 
        }
    }
    
    std::cout << std::dec << " " << std::endl; 
}

std::string getLogType(const hadoop::hdfs::log &msg)
{
    switch (msg.type()) {
        case hadoop::hdfs::log_FuncType_OPEN:
            return "OPEN";   
        case hadoop::hdfs::log_FuncType_OPEN_RET:
            return "OPEN_RET";
        case hadoop::hdfs::log_FuncType_CLOSE:
            return "CLOSE";
        case hadoop::hdfs::log_FuncType_CLOSE_RET:
            return "CLOSE_RET";
        case hadoop::hdfs::log_FuncType_READ:
            return "READ";
        case hadoop::hdfs::log_FuncType_READ_RET:
            return "READ_RET";
        default:
            return "unknown"; 
    }
}
