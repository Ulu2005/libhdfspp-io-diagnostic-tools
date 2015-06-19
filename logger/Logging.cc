/* Copyright (c) 2005 - 2015, Hewlett-Packard Development Co., L.P. */

#include <cstdarg>
#include <unistd.h>

#include "Logging.h"

using namespace hdfs;

// static initialization
extern Logger ioLogger;

std::string Logging::indexFilePath("");
std::string Logging::logFilePath("");


void Logging::startLog(const char* logFile, const char* indexFile)
{
    if (indexFilePath != "") {
    //TODO: close file and get a new name? 
    }

    if (logFilePath != "") {
    //TODO: close file and get a new name? 
    }
    
    logFilePath = logFile;
    indexFilePath = indexFile;
   
    appendPid(logFilePath);
    appendPid(indexFilePath);

    if (!ioLogger.startLog(logFilePath.c_str(), indexFilePath.c_str())) {
        exit(1);  
    }

    //TODO: more initialization work
}

void Logging::logMessage(Logger::FuncType type, ...)
{
    va_list va;
    va_start(va, type);
    ioLogger.logMessage(type, va);
    va_end(va);
}

void Logging::appendPid(std::string &str)
{
    int pid = (int) getpid();
    
    std::size_t found = str.find(".log");
    if (found != std::string::npos) {
        str = str.substr(0, found);
    } 

    str.append("_");
    str.append(std::to_string(pid));
    str.append(".log");
}
