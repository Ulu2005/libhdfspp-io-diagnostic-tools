/* Copyright (c) 2005 - 2015, Hewlett-Packard Development Co., L.P. */

#include <cstdarg>
#include <iostream>
#include <unistd.h>

#include "Logging.h"

using namespace hdfs;

// static initialization
extern Logger ioLogger;

std::string Logging::indexFilePath("");
std::string Logging::logFilePath("");
bool Logging::failed = false;

void Logging::startLog(const char* logFile, const char* indexFile)
{
    logFilePath = logFile;
    indexFilePath = indexFile;
   
    appendPid(logFilePath);
    appendPid(indexFilePath);

    if (!ioLogger.startLog(logFilePath.c_str(), indexFilePath.c_str())) {
        std::cerr << "Failed to start IO logger." << std::endl;
        failed = true;
    }
}

void Logging::logMessage(Logger::FuncType type, ...)
{
    if (failed) {
        return;
    }

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
