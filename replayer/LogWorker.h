#ifndef LIBHDFSPP_LOGWORKER_H
#define LIBHDFSPP_LOGWORKER_H

#include <thread>
#include <condition_variable>
#include <mutex>
#include <deque>

#include "log.pb.h"
#include "chdfs.h"

namespace hdfs
{

class LogWorker
{
    public:
        LogWorker (const hdfsFS fs);
        virtual ~LogWorker ();
                
        void start();//start the background work thread
        void run();  //main work loop
        void addJob(hadoop::hdfs::log* job);
        
        void handleOpen(const hadoop::hdfs::log* msg);
        void handleOpenRet(const hadoop::hdfs::log* msg);
        void handleRead(const hadoop::hdfs::log* msg);
        void handleClose(const hadoop::hdfs::log* msg);

    private:
        bool _end;
        std::thread _thread;
        std::mutex _mtx;
        std::condition_variable _cv;
        std::deque<hadoop::hdfs::log*> _queue;

        hdfsFS _fs;
        std::map<long, hdfsFile> _files; //mapping between logged hdfsFile --> hdfsFile
};

} /* hdfs */ 

#endif
