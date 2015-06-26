#include <map>
#include <iostream>

#include "chdfs.h"
#include "LogReader.h"
#include "LogWorker.h"

int main(int argc, const char* argv[]) {
    if (argc != 5) {
        std::cout << "Usage: " << argv[0] << " <log file> <index file> ";
        std::cout << "<host> <port>" << std::endl;
        return 0;
    } 
   
    hdfs::LogReader reader(argv[1], argv[2]);
    hdfsFS fs = hdfsConnect(argv[3], std::atoi(argv[4])); 

    int index(0);
    hadoop::hdfs::log* msg(nullptr);
    std::map<long, std::unique_ptr<hdfs::LogWorker>> workers;

    while((msg = reader.next()) != nullptr) {
        long id = msg->threadid();
        auto found = workers.find(id);
        
        if (found == workers.end()) {
            workers[id].reset(new hdfs::LogWorker(fs));
            workers[id]->start();
        }

        workers[id]->addJob(msg);
        index++;
    }

    if (!reader.isEOF()) {
        std::cerr << "Failed to parse log #" << (++index) << std::endl;
    }
    
    reader.close();
    hdfsDisconnect(fs);

    return 0;
}
