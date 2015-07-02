/* Copyright (c) 2005 - 2015, Hewlett-Packard Development Co., L.P. */

#include <fcntl.h>
#include <unistd.h>

#include "LogReader.h"

#define BUFSIZE 32

using namespace hdfs;
namespace pbio = ::google::protobuf::io;

LogReader::LogReader()
    : _isOK(false)
    , _isEOF(false)
    , _indexFile(nullptr)
    , _logFile(nullptr)
{
}

LogReader::LogReader (const char* logPath, const char* indexPath)
    : LogReader()
{
   _isOK = this->setPath(logPath, indexPath); 
}

LogReader::~LogReader()
{
    if (_indexFile != nullptr) {
        _indexFile = nullptr;
    }

    if (_logFile != nullptr) {
        _logFile = nullptr;
    }
}

void LogReader::close()
{
    if (_indexFile != nullptr) {
        fclose(_indexFile);
    }

    if (_logFile != nullptr) {
        _logFile->Close();
        delete _logFile;
    }
}

bool LogReader::isEOF()
{
    return _isEOF;
}

bool LogReader::setPath(const char* logPath, const char* indexPath)
{
    if (!logPath || !indexPath) {
        return false;
    }

    int logFd = open(logPath, O_RDONLY);
    int indexFd = open(indexPath, O_RDONLY);

    if ((logFd == -1) || (indexFd == -1)) {
        return false;
    }

    _logFile = new pbio::FileInputStream(logFd);
    _indexFile = fdopen(indexFd, "r"); 

    return true;
}

std::unique_ptr<hadoop::hdfs::log> LogReader::next()
{
    if (_isEOF || (!_isOK)) {
        return nullptr;
    }

    char buf[BUFSIZE];
    if (fgets(buf, sizeof(buf), _indexFile) == NULL) {
        if (feof(_indexFile)) {
            _isEOF = true;  
        } else {
            _isOK = false;
        }

        return nullptr;
    }

    int size = std::atoi(buf);
    std::unique_ptr<hadoop::hdfs::log> msg(new hadoop::hdfs::log());
    if (!msg->ParseFromBoundedZeroCopyStream(_logFile, size)) {
        _isOK = false;
        return nullptr; 
    }

    return msg;
}
