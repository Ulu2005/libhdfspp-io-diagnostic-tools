/* Copyright (c) 2005 - 2015, Hewlett-Packard Development Co., L.P. */

#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <iostream>

#include "Logger.h"
#include "log.pb.h"

using namespace iotools;
namespace pbio = google::protobuf::io;


Logger::Logger()
    : _current_day(-1)
    , _indexFile(nullptr)
    , _logFile(nullptr)
{
}

Logger::~Logger()
{
    if (_indexFile!= nullptr) {
        fclose(_indexFile);
        _indexFile = nullptr;
    }
    
    if (_logFile != nullptr) {
        _logFile->Close();
        delete _logFile;
        _logFile = nullptr;
    }
}


bool Logger::startLog(const char* logFile, const char* indexFile)
{
    if (!logFile || !indexFile) {
        return false;
    }
    
    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;

    int logFileFd = open(logFile, O_CREAT | O_WRONLY | O_TRUNC, mode);
    if (logFileFd == -1) {
        return false;
    } 
    _logFile = new pbio::FileOutputStream(logFileFd);

    _indexFile = fopen(indexFile, "w+"); 
    if (_indexFile == NULL) {
        return false;
    }

    return true;
}

bool Logger::logOpen(const char* path)
{
    //TODO: add lock
    proto::log msg; //in actual code this object can be reused
    msg.set_time(getTime());
    msg.set_date(_current_day); //should always set after time
    msg.set_threadid(1024); //TODO: get thread id
    msg.set_type(proto::log_FuncType_OPEN);
    msg.set_path(path);
    msg.add_argument(1);
    msg.add_argument(2);

    if (!msg.SerializeToZeroCopyStream(_logFile)) {
        std::cerr << "failed to serialize log message." << std::endl;
        return false;
    }
    
    int size = msg.ByteSize();
    if (fprintf(_indexFile, "%d\n", size) <= 0) {
        std::cerr << "failed to write message size." << std::endl;
        return false;
    }

    return true;
}

int Logger::getTime()
{
    struct timespec now; 
    struct tm tm; 
    clock_gettime(CLOCK_REALTIME, &now);
    localtime_r(&now.tv_sec, &tm);

    if (_current_day != tm.tm_yday) {   
        _current_day = tm.tm_yday;
    }   
    
    int mm_second = 0;
    mm_second += tm.tm_hour * 3600 * 1000;
    mm_second += tm.tm_min * 60 * 1000;
    mm_second += tm.tm_sec * 1000;
    mm_second += int(now.tv_nsec / 1000000);

    return mm_second; 
}

int main(int argc, char *argv[])
{
    if (argc != 3) {
        std::cout << "Usage: " << argv[0] 
            << " logfile indexfile" << std::endl;
        return 0;
    } 

    Logger test;
    if (!test.startLog(argv[1], argv[2])) {
        std::cerr << "Failed to start logging." << std::endl;
        return 0; 
    }
    
    test.logOpen("fake path");
    return 0;
}
