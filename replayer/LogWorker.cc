#include <iostream>

#include "LogWorker.h"

using namespace hdfs;

LogWorker::LogWorker(const hdfsFS fs)
    : _end(false)
    , _thread()
    , _mtx()
    , _cv()
    , _queue()
    , _fs(fs)
    , _files()
{
}

LogWorker::~LogWorker()
{
    _end = true;
    _cv.notify_all();
    _thread.join();
}

void LogWorker::start()
{
    _thread = std::thread(&LogWorker::run, this);
}

void LogWorker::run()
{
    std::unique_lock<std::mutex> lock(_mtx);

    while(true) {
        if (_queue.empty() && !_end) {
            _cv.wait(lock);
            
            if (_queue.empty() && _end) {
                break;
            }
        } else if (_queue.empty() && _end) {
            break;
        }

        auto msg = _queue.front();
        _queue.pop_front();

        switch (msg->type()) {
            case hadoop::hdfs::log_FuncType_OPEN:
                handleOpen(msg);
                break;
            case hadoop::hdfs::log_FuncType_OPEN_RET:
                handleOpenRet(msg);
                break;
            case hadoop::hdfs::log_FuncType_CLOSE:
                handleClose(msg);
                break;
            case hadoop::hdfs::log_FuncType_READ:
                handleRead(msg);
                break;
            default:
                ;
        } 
    }
    
}

void LogWorker::addJob(hadoop::hdfs::log* job)
{
    std::lock_guard<std::mutex> lock(_mtx);
    _queue.push_back(job);
    _cv.notify_all();
}

void LogWorker::handleOpen(const hadoop::hdfs::log* msg)
{
    hdfsFile file = hdfsOpenFile(_fs, msg->path().c_str(), 
                                (int)msg->argument(1), 
                                (int)msg->argument(2), 
                                (short)msg->argument(3), 
                                (int)msg->argument(4));
    
    _files[msg->threadid()] = file; //TODO: use a safer way to map between opened file and SOME_INDICATOR
    delete msg;
}

void LogWorker::handleOpenRet(const hadoop::hdfs::log* msg)
{
    _files[msg->argument(0)] = _files[msg->threadid()];
    _files.erase(msg->threadid());
    delete msg;
}

void LogWorker::handleRead(const hadoop::hdfs::log* msg)
{
    auto file = _files.find(msg->argument(1));
    if (file != _files.end()) {
        size_t buf_size = msg->argument(4);
        char* buffer = new char[buf_size];

        auto ret = hdfsPread(_fs, file->second, 
                             (off_t)msg->argument(2), 
                             reinterpret_cast<void*>(buffer), 
                             buf_size);
    
        (void)ret;//make gcc happy
        delete[] buffer;
    } else {
        std::cerr << "Read: file " 
                  << msg->argument(1) 
                  << " not found." << std::endl;
    }
    
    delete msg;
}

void LogWorker::handleClose(const hadoop::hdfs::log* msg)
{
    auto file = _files.find(msg->argument(1));
    if (file != _files.end()) {
        auto ret = hdfsCloseFile(_fs, file->second);
        (void)ret;//make gcc happy
    } else {
        std::cerr << "Close: file " 
                  << msg->argument(1) 
                  << " not found." << std::endl;
    }
    
    delete msg;
}
