// A basic reader for log file. 

#include <iostream>
#include <memory>
#include <string>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include "log.pb.h"
#include "LogReader.h"

void printLogInfo(const hadoop::hdfs::log &msg);
std::string getLogType(const hadoop::hdfs::log &msg);

int main(int argc, const char* argv[]) {
    if (argc != 3) {
        std::cout << "Usage: " << argv[0] 
                  << " <log file> <index file>" << std::endl;
        return 0;
    } 
    
    hdfs::LogReader reader(argv[1], argv[2]);
    int index(0);
    hadoop::hdfs::log* msg = nullptr;

    while((msg = reader.next()) != nullptr) {
        index++;
        std::cout << "#" << index << std::endl;
       
        printLogInfo(*msg);
        delete msg;
    }

    if (!reader.isEOF()) {
        std::cerr << "Failed to parse log #" << (++index) << std::endl;
    }
    
    reader.close();
    return 0;
}

/* Print log message */
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

/* Get string of log FuncType */
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
