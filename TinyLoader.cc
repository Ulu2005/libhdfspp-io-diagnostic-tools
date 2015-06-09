#include <iostream>
#include <string>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include "log.pb.h"

namespace pbio = ::google::protobuf::io;

int main(int argc, const char* argv[]) {
    if (argc != 3) {
        std::cout << "Usage: " << argv[0] 
            << " logfile indexfile" << std::endl;
        return 0;
    } 

    int logFd = open(argv[1], O_RDONLY);
    int indexFd = open(argv[2], O_RDONLY);
    if ((logFd == -1) || (indexFd == -1)) {
        std::cerr << "Errno " << errno;
        std::cerr << ". Failed to open file " << logFd << " " 
                                              << indexFd << std::endl;
        return 0;
    }

    pbio::ZeroCopyInputStream* logFile = new pbio::FileInputStream(logFd);
    FILE* indexFile = fdopen(indexFd, "r"); 
    
    int size;
    fscanf(indexFile, "%d", &size); //maybe fgets would be better

    proto::log msg;
    if (!msg.ParseFromBoundedZeroCopyStream(logFile, size)) {
        std::cerr << "failed to parse file" << std::endl;
        close(logFd);
        close(indexFd);
        return 0;    
    }


    std::cout << msg.threadid() << std::endl; 
    std::cout << msg.path() << std::endl; 
    std::cout << msg.argument_size() << std::endl; 
    
    close(logFd);
    close(indexFd);
    return 0;
}
