/* Copyright (c) 2005 - 2015, Hewlett-Packard Development Co., L.P. */

// Logger for libhdfs++.

#ifndef LIBHDFSPP_LOGGER_H_
#define LIBHDFSPP_LOGGER_H_ 

#include <google/protobuf/io/zero_copy_stream_impl.h>

namespace iotools
{

class Logger
{
public:
    /* Public type */
    typedef enum {                  //function type of different file operation
        OPEN,
        OPEN_RET,
        CLOSE,
        CLOSE_RET,
        READ,
        READ_RET
    } FuncType;

    /* Public methods */
    Logger ();
    virtual ~Logger ();
    
    bool startLog(const char* logFile, const char* indexFile);
    bool logMessage(FuncType type, ...);
    
    bool logOpen(const char* path);     //for initial test only, will be deleted
private:
    int getTime();                  //get time in milesecond and refresh date

    int _current_day;
    FILE* _indexFile;
    google::protobuf::io::FileOutputStream* _logFile;
};
 
} /* iotools */ 

#endif
