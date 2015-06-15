/* Copyright (c) 2005 - 2015, Hewlett-Packard Development Co., L.P. */

#include <iostream>
#include <ctime>
#include <unistd.h>
#include <fcntl.h>

#include "Logger.h"

using namespace hdfs;
namespace pbio = google::protobuf::io;

Logger logger;

Logger::Logger()
    : _mutex(PTHREAD_MUTEX_INITIALIZER)
    , _current_day(-1)
    , _indexFile(nullptr)
    , _logFile(nullptr)
{
}

Logger::~Logger()
{
    pthread_mutex_destroy(&_mutex);

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

bool Logger::logMessage(FuncType type, va_list &va)
{
    hadoop::hdfs::log msg; 
    msg.set_time(getTime());
    msg.set_date(_current_day);     //should always set after time
    msg.set_threadid((long)pthread_self()); 
    
    switch (type) {
        case OPEN:
            msg.set_type(hadoop::hdfs::log_FuncType_OPEN);
            msg.add_argument(va_arg(va, long));
            msg.set_path(va_arg(va, char*)); 
            msg.add_argument(va_arg(va, long));
            msg.add_argument(va_arg(va, long));
            msg.add_argument(va_arg(va, long));
            msg.add_argument(va_arg(va, long));
            break;
        case OPEN_RET:
            msg.set_type(hadoop::hdfs::log_FuncType_OPEN_RET);
            msg.add_argument(va_arg(va, long));
            break;
        case CLOSE:
            msg.set_type(hadoop::hdfs::log_FuncType_CLOSE);
            msg.add_argument(va_arg(va, long));
            msg.add_argument(va_arg(va, long));
            break;
        case CLOSE_RET:
            msg.set_type(hadoop::hdfs::log_FuncType_CLOSE_RET);
            msg.add_argument(va_arg(va, long));
            break;
        case READ:
            msg.set_type(hadoop::hdfs::log_FuncType_READ);
            msg.add_argument(va_arg(va, long));
            msg.add_argument(va_arg(va, long));
            msg.add_argument(va_arg(va, long));
            msg.add_argument(va_arg(va, long));
            msg.add_argument(va_arg(va, long));
            break;
        case READ_RET:
            msg.set_type(hadoop::hdfs::log_FuncType_READ_RET);
            msg.add_argument(va_arg(va, long));
            break;
    }
    va_end(va);
    
    //write log message and message size onto disk
    pthread_mutex_lock(&_mutex);
   
    int size = msg.ByteSize();
    if (fprintf(_indexFile, "%d\n", size) <= 0) {
        std::cerr << "failed to write message size." << std::endl;
        pthread_mutex_unlock(&_mutex);
        return false;
    }
    fflush(_indexFile);
    
    if (!msg.SerializeToZeroCopyStream(_logFile)) {
        std::cerr << "failed to serialize log message." << std::endl;
        pthread_mutex_unlock(&_mutex);
        return false;
    }
    _logFile->Flush(); 

    pthread_mutex_unlock(&_mutex);
    
    return true;
}

long Logger::getTime()
{
    struct timespec now; 
    struct tm tm; 

    clock_gettime(CLOCK_REALTIME, &now);
    localtime_r(&now.tv_sec, &tm);

    if (_current_day != tm.tm_yday) {   
        _current_day = tm.tm_yday;
    }   
    
    long nano_second = 0;
    nano_second += tm.tm_hour * 3600;
    nano_second += tm.tm_min * 60;
    nano_second += tm.tm_sec;
    nano_second *= 1000000000;
    nano_second += now.tv_nsec;
    
    return nano_second; 
}

