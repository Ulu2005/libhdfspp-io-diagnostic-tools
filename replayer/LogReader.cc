#include <memory>
#include <fcntl.h>
#include <unistd.h>

#include "LogReader.h"

#define BUFSIZE 32

using namespace hdfs;
namespace pbio = ::google::protobuf::io;

LogReader::LogReader()
    : _isOK(false)
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
       fclose(_indexFile);
       _indexFile = nullptr;
    }
    
    if (_logFile != nullptr) {
        _logFile->Close();
        delete _logFile;
        _logFile = nullptr;
    }
}

bool LogReader::isOK()
{
    return _isOK;
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

std::shared_ptr<hadoop::hdfs::log> LogReader::next()
{
    if (!_isOK || feof(_indexFile)) {
        _isOK = false;
        return nullptr; 
    }

    char buf[BUFSIZE];
    if (fgets(buf, sizeof(buf), _indexFile) == NULL) {
        _isOK = false;
        return nullptr; 
    }

    std::shared_ptr<hadoop::hdfs::log> msg = std::make_shared<hadoop::hdfs::log>();
    int size = std::atoi(buf);

    if (!msg->ParseFromBoundedZeroCopyStream(_logFile, size)) {
        _isOK = false;
        return nullptr; 
    }
    
    return msg;
}
