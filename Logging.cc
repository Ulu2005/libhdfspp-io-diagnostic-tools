/* Copyright (c) 2005 - 2015, Hewlett-Packard Development Co., L.P. */

#include <cstdarg>
#include "Logging.h"

using namespace iotools;

// static initialization
extern Logger logger;

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

    if (!logger.startLog(logFilePath.c_str(), indexFilePath.c_str())) {
        exit(1);  
    }

    //TODO: more initialization work
}

void Logging::logMessage(Logger::FuncType type, ...)
{
    va_list va;
    va_start(va, type);
    logger.logMessage(type, va);
    va_end(va);
}

