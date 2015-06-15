/* Copyright (c) 2005 - 2015, Hewlett-Packard Development Co., L.P. */

// Static wrapper class for logger.

#ifndef LIBHDFSPP_LOGGING_H
#define LIBHDFSPP_LOGGING_H 

#include "Logger.h"

namespace hdfs
{

class Logging
{
public:
    Logging ();
    virtual ~Logging ();

    static void startLog(const char* logFile, const char* indexFile);
    static void logMessage(Logger::FuncType type, ...);

private:
    static std::string indexFilePath;
    static std::string logFilePath;
};

} /* iotools */ 

#endif
