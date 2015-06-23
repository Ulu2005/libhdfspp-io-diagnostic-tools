/* Copyright (c) 2005 - 2015, Hewlett-Packard Development Co., L.P. */

//This class works as a reader to log, which includes a log file and 
//a index file 

#ifndef LIBHDFSPP_READER_H_
#define LIBHDFSPP_READER_H_ 

#include <memory>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include "log.pb.h"

namespace hdfs
{
  
class LogReader
{
public:
    LogReader();
    LogReader(const char* logPath, const char* indexPath);
    virtual ~LogReader();
    
    void close(); 
    bool isEOF();
    bool setPath(const char* logPath, const char* indexPath); 
    ::hadoop::hdfs::log* next();

private:

    bool _isOK;
    bool _isEOF;
    FILE* _indexFile; 
    ::google::protobuf::io::FileInputStream* _logFile;
};

} /* hdfs */ 

#endif
