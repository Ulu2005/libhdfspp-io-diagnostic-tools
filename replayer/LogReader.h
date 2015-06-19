/* Copyright (c) 2005 - 2015, Hewlett-Packard Development Co., L.P. */

// 

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
    
    bool isOK();    
    bool setPath(const char* logPath, const char* indexPath); 
    std::shared_ptr<hadoop::hdfs::log> next();

private:

    bool _isOK; 
    FILE* _indexFile; 
    google::protobuf::io::FileInputStream* _logFile;
};

} /* hdfs */ 

#endif
