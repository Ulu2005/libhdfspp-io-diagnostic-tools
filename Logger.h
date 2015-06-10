/* Copyright (c) 2005 - 2015, Hewlett-Packard Development Co., L.P. */

// Logger for libhdfs++.

#ifndef LIBHDFSPP_LOGGER_H_
#define LIBHDFSPP_LOGGER_H_ 

#include <google/protobuf/io/zero_copy_stream_impl.h>

#include "log.pb.h"

namespace iotools
{

class Logger
{
public:
    /* Public type */
    typedef enum {              //function type of different file operation
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
    
    static bool startLog(const char* logFile, const char* indexFile);
    static bool logMessage(FuncType type, ...);
    
private:
    static int getTime();              //get time in millisecond and refresh current day

    static int _current_day;
    static FILE* _indexFile;
    static google::protobuf::io::FileOutputStream* _logFile;
};
 
extern Logger logger;

} /* iotools */ 

#endif
