#include <iostream>

#include "LogWorker.h"

using namespace hdfs;

LogWorker::LogWorker(hdfsFS fs, 
    std::map<long, hdfsFile> *files, 
    std::map<hdfsFile, int> *ref_count, 
    std::mutex *file_mtx, std::mutex *ref_mtx)
  : end_(false)
  , thread_()
  , cv_mtx_()
  , queue_mtx_()
  , file_mtx_(file_mtx)
  , ref_mtx_(ref_mtx)
  , cv_()
  , queue_()
  , fs_(fs)
  , files_(files)
  , ref_count_(ref_count)
{
}

LogWorker::~LogWorker()
{
  end_ = true;
  cv_.notify_all();
  thread_.join();
}

void LogWorker::start()
{
  thread_ = std::thread(&LogWorker::run, this);
}

void LogWorker::run()
{
  std::unique_lock<std::mutex> cv_lock(cv_mtx_);

  while(true) {
    std::unique_ptr<hadoop::hdfs::log> msg;
    
    {
      std::lock_guard<std::mutex> queue_lk(queue_mtx_);
      if (queue_.empty() && !end_) {
        cv_.wait(cv_lock);

        // thread could be awaken here by destructor
        if (queue_.empty() && end_) {
          break;
        }
      } else if (queue_.empty() && end_) {
        break;
      }

      msg = std::move(queue_.front());
      queue_.pop_front();
    } // reduce the scope of locking

    switch (msg->type()) {
      case hadoop::hdfs::log_FuncType_OPEN:
        handleOpen(*msg);
        break;
      case hadoop::hdfs::log_FuncType_OPEN_RET:
        handleOpenRet(*msg);
        break;
      case hadoop::hdfs::log_FuncType_CLOSE:
        handleClose(*msg);
        break;
      case hadoop::hdfs::log_FuncType_READ:
        handleRead(*msg);
        break;
      default:
        ;
    } 
  }

}

void LogWorker::addJob(std::unique_ptr<hadoop::hdfs::log> job)
{
  {
    std::lock_guard<std::mutex> queue_lk(queue_mtx_);
    queue_.push_back(std::move(job));
  } 

  cv_.notify_all();
}

void LogWorker::handleOpen(const hadoop::hdfs::log &msg)
{
  hdfsFile file = hdfsOpenFile(fs_, msg.path().c_str(), 
      (int)msg.argument(1), 
      (int)msg.argument(2), 
      (short)msg.argument(3), 
      (int)msg.argument(4));
  
  std::lock_guard<std::mutex> file_lk(*file_mtx_);
  (*files_)[msg.threadid()] = file;

}

void LogWorker::handleOpenRet(const hadoop::hdfs::log &msg)
{
  hdfsFile temp;
  
  {
    std::lock_guard<std::mutex> file_lk(*file_mtx_);
    temp = (*files_)[msg.threadid()];
    (*files_)[msg.argument(0)] = temp;
    files_->erase(msg.threadid());
  }

  std::lock_guard<std::mutex> ref_lk(*ref_mtx_);
  (*ref_count_)[temp] = 1;
}

void LogWorker::handleRead(const hadoop::hdfs::log &msg)
{
  decltype(files_->end()) file;

  {
    std::lock_guard<std::mutex> file_lk(*file_mtx_);
    file = files_->find(msg.argument(1));
  } 
 
  { 
    std::lock_guard<std::mutex> ref_lk(*ref_mtx_);
    (*ref_count_)[file->second]++;
  }

  if (file != files_->end()) {
    size_t buf_size = msg.argument(4);
    char* buffer = new char[buf_size];

    auto ret = hdfsPread(fs_, file->second, 
        (off_t)msg.argument(2), 
        reinterpret_cast<void*>(buffer), 
        buf_size);
    
    { 
      std::lock_guard<std::mutex> ref_lk(*ref_mtx_);
      (*ref_count_)[file->second]--;
    }

    (void)ret;//make gcc happy
    delete[] buffer;
  } else {
    std::cerr << "Read: file " 
      << msg.argument(1) 
      << " not found." << std::endl;
  }
}

void LogWorker::handleClose(const hadoop::hdfs::log &msg)
{
  decltype(files_->end()) file;

  {
    std::lock_guard<std::mutex> file_lk(*file_mtx_);
    file = files_->find(msg.argument(1));
  }

  if (file != files_->end()) {
    while ((*ref_count_)[file->second] != 1) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));   
    }

    auto ret = hdfsCloseFile(fs_, file->second);
    (void)ret;//make gcc happy
  } else {
    std::cerr << "Close: file " 
      << msg.argument(1) 
      << " not found." << std::endl;
  }
}

