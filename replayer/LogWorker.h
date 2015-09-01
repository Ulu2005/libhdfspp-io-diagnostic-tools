#ifndef LIBHDFSPP_LOGWORKER_H
#define LIBHDFSPP_LOGWORKER_H

#include <thread>
#include <condition_variable>
#include <mutex>
#include <map>
#include <deque>
#include <memory>

#include "log.pb.h"
#include "libhdfs++/chdfs.h"

namespace hdfs
{

class LogWorker;
typedef std::map<long, std::unique_ptr<LogWorker>> WorkerMap;

class LogWorker
{
 public:
  LogWorker (int id, hdfsFS fs, 
      std::map<long, hdfsFile> *files, 
      std::map<hdfsFile, int> *ref_count, 
      std::mutex *file_mtx, std::mutex *ref_mtx, WorkerMap *workers);
  virtual ~LogWorker ();

  void start();//start the background work thread
  void run();  //main work loop
  void addJob(std::unique_ptr<hadoop::hdfs::log> job);

  void handleOpen(const hadoop::hdfs::log &msg);
  void handleOpenRet(const hadoop::hdfs::log &msg);
  void handleRead(const hadoop::hdfs::log &msg);
  void handleClose(const hadoop::hdfs::log &msg);

 private:
  int id_;
  std::thread thread_;
  std::mutex cv_mtx_;
  std::mutex queue_mtx_;
  std::mutex *file_mtx_;
  std::mutex *ref_mtx_;
  std::condition_variable cv_;
  std::deque<std::unique_ptr<hadoop::hdfs::log> > queue_;

  hdfsFS fs_;
  std::map<long, hdfsFile> *files_;
  std::map<hdfsFile, int> *ref_count_;
  WorkerMap *workers_;
};

} /* hdfs */ 

#endif

