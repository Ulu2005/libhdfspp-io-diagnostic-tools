#include <map>
#include <chrono>
#include <vector>
#include <thread>
#include <iostream>

#include "chdfs.h"
#include "LogReader.h"

static const unsigned threads_max = std::thread::hardware_concurrency() * 2;
static hdfsFS fs(nullptr);
static std::map<long, hdfsFile> files; //mapping between logged hdfsFile --> hdfsFile
static std::vector<hadoop::hdfs::log*> jobs;

void handleJobs();

void handleOpen(const hadoop::hdfs::log* msg);
void handleOpenRet(const hadoop::hdfs::log* msg);
void handleRead(const hadoop::hdfs::log* msg);
void handleClose(const hadoop::hdfs::log* msg);

int main(int argc, const char* argv[]) {
    if (argc != 5) {
        std::cout << "Usage: " << argv[0] << " <log file> <index file> ";
        std::cout << "<host> <port>" << std::endl;
        return 0;
    } 
   
    hdfs::LogReader reader(argv[1], argv[2]);
    fs = hdfsConnect(argv[3], std::atoi(argv[4])); 

    int index(0);
    hadoop::hdfs::log* msg(nullptr);
    std::chrono::time_point<std::chrono::system_clock> start, end;

    std::cout << "Start replaying file operations." << std::endl;
    start = std::chrono::system_clock::now();
    
    while((msg = reader.next()) != nullptr) {
        switch (msg->type()) {
            case hadoop::hdfs::log_FuncType_OPEN:
                handleOpen(msg);
                break;
            case hadoop::hdfs::log_FuncType_OPEN_RET:
                handleOpenRet(msg);
                break;
            case hadoop::hdfs::log_FuncType_CLOSE:
                handleJobs(); //handle all jobs in vector and join the work threads here
                handleClose(msg);
                break;
            case hadoop::hdfs::log_FuncType_READ:
                jobs.push_back(msg); //push into vector and handle before close
                break;
            default:
                delete msg;
        } 

        index++;
    }

    end = std::chrono::system_clock::now();
    std::chrono::duration<double> time = end - start;

    if (!reader.isEOF()) {
        std::cerr << "Failed to parse log #" << (++index) << std::endl;
    } else {
        std::cout << "Total time: " << time.count() << " seconds. Total file operations: " << index / 2 << "." << std::endl; 
    }
    
    reader.close();
    hdfsDisconnect(fs);

    return 0;
}

/* spawn threads to handle file operation and wait them to finish */
void handleJobs()
{
    if (jobs.empty()) {
        return;
    }

    std::vector<std::thread> threads;
    for (auto msg : jobs) {
        switch (msg->type()) { //TODO: add other types of operations, like write, stat
            case hadoop::hdfs::log_FuncType_READ:
                threads.push_back(std::thread(handleRead, msg)); 
                break;
            default:
                ;
        }

        if (threads.size() == threads_max) { //avoid too many requests to hdfs
            for (int i = 0; i < (int)threads.size(); ++i) {
                threads[i].join(); 
            }

            threads.clear(); 
        }
    }
    
    for (int i = 0; i < (int)threads.size(); ++i) {
        threads[i].join(); 
    }
    
    jobs.clear();
}

void handleOpen(const hadoop::hdfs::log* msg)
{
    hdfsFile file = hdfsOpenFile(fs, msg->path().c_str(), 
                                (int)msg->argument(1), 
                                (int)msg->argument(2), 
                                (short)msg->argument(3), 
                                (int)msg->argument(4));
    
    files[msg->threadid()] = file;
    delete msg;
}

void handleOpenRet(const hadoop::hdfs::log* msg)
{
    files[msg->argument(0)] = files[msg->threadid()];
    files.erase(msg->threadid());
    delete msg;
}

void handleRead(const hadoop::hdfs::log* msg)
{
    auto file = files.find(msg->argument(1));
    if (file != files.end()) {
        size_t buf_size = msg->argument(4);
        char* buffer = new char[buf_size];

        auto ret = hdfsPread(fs, file->second, 
                             (off_t)msg->argument(2), 
                             reinterpret_cast<void*>(buffer), 
                             buf_size);
    
        (void)ret;//make gcc happy
        delete[] buffer;
    } else {
        std::cerr << "Read: file " 
                  << msg->argument(1) 
                  << "not found." << std::endl;
    }
    
    delete msg;
}

void handleClose(const hadoop::hdfs::log* msg)
{
    auto file = files.find(msg->argument(1));
    if (file != files.end()) {
        auto ret = hdfsCloseFile(fs, file->second);
        (void)ret;//make gcc happy
    } else {
        std::cerr << "Close: file " 
                  << msg->argument(1) 
                  << "not found." << std::endl;
    }
    
    delete msg;
}
