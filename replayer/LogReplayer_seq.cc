#include <map>
#include <iostream>

#include "chdfs.h"
#include "LogReader.h"

static std::map<long, hdfsFile> files; //mapping between logged hdfsFile --> hdfsFile
static std::map<long, int> read_count; //mapping between logged thread id --> current reads
static hdfsFS fs(nullptr);

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
    
    while((msg = reader.next()) != nullptr) {
        switch (msg->type()) {
            case hadoop::hdfs::log_FuncType_OPEN:
                handleOpen(msg);
                break;
            case hadoop::hdfs::log_FuncType_OPEN_RET:
                handleOpenRet(msg);
                break;
            case hadoop::hdfs::log_FuncType_CLOSE:
                handleClose(msg);
                break;
            case hadoop::hdfs::log_FuncType_READ:
                handleRead(msg);
                break;
            default:
                ;
        } 

        index++;
    }

    if (!reader.isEOF()) {
        std::cerr << "Failed to parse log #" << (++index) << std::endl;
    }
    
    reader.close();
    hdfsDisconnect(fs);

    return 0;
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
        //allocate buffer if current one is not enough
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
