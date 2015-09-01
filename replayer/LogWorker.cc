#include <iostream>
#include <chrono>

#include "LogWorker.h"

using namespace hdfs;

LogWorker::LogWorker(int id, hdfsFS fs, 
    std::map<long, hdfsFile> *files, 
    std::map<hdfsFile, int> *ref_count, 
    std::mutex *file_mtx, std::mutex *ref_mtx, WorkerMap *workers)
  : id_(id)
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
  , workers_(workers)
{
}

// the worker's life cycle is managed by itself, when the queue has been empty 
// for some time, the worker will destroy itself. Otherwise, it should keep 
// waiting until new jobs were added to the queue.
LogWorker::~LogWorker()
{
  thread_.join();
}

void LogWorker::start()
{
  thread_ = std::thread(&LogWorker::run, this);
}

void LogWorker::run()
{
  std::unique_lock<std::mutex> cv_lock(cv_mtx_);
  std::unique_lock<std::mutex> queue_lk(queue_mtx_, std::defer_lock);
  bool loop = true;
  auto empty =[this, &queue_lk](){ // ensure that when cv is waiting, queue mutex is not hold
    queue_lk.lock(); 
    bool ret = queue_.empty();
    queue_lk.unlock();
    return ret;};

  while(true) {
    std::unique_ptr<hadoop::hdfs::log> msg;
    auto now = std::chrono::system_clock::now();

    while (empty()) {
      if (cv_.wait_until(cv_lock, now + std::chrono::seconds(10)) == std::cv_status::timeout) {
        loop = false;
        break; 
      }
    } 

    if (!loop) break;
   
    queue_lk.lock(); 
    msg = std::move(queue_.front());
    queue_.pop_front();  
    queue_lk.unlock();

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

  // remove worker itself from workers hashtable, as the worker is wrapped by
  // unique pointer, the descrutor will be automatically called when GC works.
  //TODO: add lock here
  workers_->erase(id_);
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

