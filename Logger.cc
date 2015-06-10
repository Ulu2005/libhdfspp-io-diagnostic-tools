/* Copyright (c) 2005 - 2015, Hewlett-Packard Development Co., L.P. */

#include <iostream>
#include <ctime>
#include <cstdarg>
#include <unistd.h>
#include <fcntl.h>

#include "Logger.h"

using namespace iotools;
namespace pbio = google::protobuf::io;

//static initializatoin
int Logger::_current_day(-1);
FILE* Logger::_indexFile(nullptr);
pbio::FileOutputStream* Logger::_logFile(nullptr);
Logger logger;

Logger::Logger()
{
}

Logger::~Logger()
{
    if (_indexFile != nullptr) {
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

bool Logger::logMessage(FuncType type, ...)
{
    proto::log msg; 
    msg.set_time(getTime());
    msg.set_date(_current_day);     //should always set after time
    msg.set_threadid(1024);         //TODO: get thread id
    
    va_list va;
    va_start(va, type);
    switch (type) {
        case OPEN:
            msg.set_type(proto::log_FuncType_OPEN);
            msg.add_argument(va_arg(va, long));
            msg.add_argument(va_arg(va, long));
            msg.add_argument(va_arg(va, long));
            msg.add_argument(va_arg(va, long));
            msg.add_argument(va_arg(va, long));
            msg.add_argument(va_arg(va, long));
            break;
        case OPEN_RET:
            msg.set_type(proto::log_FuncType_OPEN_RET);
            msg.add_argument(va_arg(va, long));
            break;
        case CLOSE:
            msg.set_type(proto::log_FuncType_CLOSE);
            msg.add_argument(va_arg(va, long));
            msg.add_argument(va_arg(va, long));
            break;
        case CLOSE_RET:
            msg.set_type(proto::log_FuncType_CLOSE_RET);
            msg.add_argument(va_arg(va, long));
            break;
        case READ:
            msg.set_type(proto::log_FuncType_READ);
            msg.add_argument(va_arg(va, long));
            msg.add_argument(va_arg(va, long));
            msg.add_argument(va_arg(va, long));
            msg.add_argument(va_arg(va, long));
            msg.add_argument(va_arg(va, long));
            break;
        case READ_RET:
            msg.set_type(proto::log_FuncType_READ_RET);
            msg.add_argument(va_arg(va, long));
            break;
    }
    va_end(va);
    
    //write log message and message size onto disk
    int size = msg.ByteSize();
    //TODO: add lock
    if (fprintf(_indexFile, "%d\n", size) <= 0) {
        std::cerr << "failed to write message size." << std::endl;
        return false;
    }
    fflush(_indexFile);
    
    if (!msg.SerializeToZeroCopyStream(_logFile)) {
        //TODO: error handling; 
        //delete the last line of index file? or stop logging
        std::cerr << "failed to serialize log message." << std::endl;
        return false;
    }
    _logFile->Flush(); 

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

