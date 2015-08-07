/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Log replayer replays file operations by reading log file. All
// open and close operations would be done in main thread. Other
// operations will be performed in seperate threads. And there is a
// background thread printing bandwidth information every second.

#include <map>
#include <mutex>
#include <chrono>
#include <vector>
#include <thread>
#include <iostream>
#include <unistd.h>

#include "libhdfs++/chdfs.h"
#include "LogReader.h"

//constant
static const int MB = 1024 * 1024;

//program options
static bool wait_before_new_thread = false;
static std::string parent_folder = "";
static unsigned max_threads = std::thread::hardware_concurrency() * 2;

//global variables
static std::mutex mtx;
static bool need_count = true;
static int read_bytes = 0;
static double run_time = 0;
static hdfsFS fs = nullptr;
static std::map<long, hdfsFile> files;
static std::vector<std::unique_ptr<hadoop::hdfs::log>> jobs;

void getReadInfo(int bytes, double seconds);
void printBandwidth();
void handleJobs();
void handleOpen(const hadoop::hdfs::log &msg);
void handleOpenRet(const hadoop::hdfs::log &msg);
void handleRead(const hadoop::hdfs::log &msg);
void handleClose(const hadoop::hdfs::log &msg);

int main(int argc, char* argv[]) {
  int opt;

  while((opt = getopt(argc, argv, "swp:")) != -1) {
    switch (opt) {
      case 's':
        need_count = false;
        max_threads = 1;
        break;
      case 'w':
        wait_before_new_thread = true;
        break;
      case 'p':
        parent_folder = optarg;
        break;
      default:
        std::cout << "Usage: " << argv[0] << " [-s] [-w] [-p parent-folder]";
        std::cout << " <log file> " << "<host> <port>" << std::endl;
        std::cout << "Options:" << std::endl;
        std::cout << "  -s          Enable sequential mode. All file operations are executed sequentially." << std::endl;
        std::cout << "  -w          Enable wait mode. Replayer will reproduce time gap between original file operations." << std::endl;
        std::cout << "  -p <arg>    Specify the parent foder for the data set." << std::endl;
        return 0;
    }
  }

  if (optind >= argc) {
        std::cout << "Usage: " << argv[0] << " [-s] [-w] [-p parent-folder]";
        std::cout << " <log file> " << "<host> <port>" << std::endl;
        std::cout << "Options:" << std::endl;
        std::cout << "  -s          Enable sequential mode. All file operations are executed sequentially." << std::endl;
        std::cout << "  -w          Enable wait mode. Replayer will reproduce time gap between original file operations." << std::endl;
        std::cout << "  -p <arg>    Specify the parent foder for the data set." << std::endl;
        return 0;
  }

  hdfs::LogReader reader(argv[optind]);
  fs = hdfsConnect(argv[optind + 1], std::atoi(argv[optind + 2])); 

  int index(0);
  std::unique_ptr<hadoop::hdfs::log> msg;
  std::chrono::time_point<std::chrono::system_clock> start, end;

  std::cout << "Start replaying file operations." << std::endl;
  start = std::chrono::system_clock::now();
  std::thread count_thread(printBandwidth);

  while((msg = reader.next()) != nullptr) {
    switch (msg->type()) {
      case hadoop::hdfs::log_FuncType_OPEN:
        handleOpen(*msg);
        break;
      case hadoop::hdfs::log_FuncType_OPEN_RET:
        handleOpenRet(*msg);
        break;
      case hadoop::hdfs::log_FuncType_CLOSE:
        handleJobs(); //handle all jobs in vector and join the work threads here
        handleClose(*msg);
        break;
      case hadoop::hdfs::log_FuncType_CLOSE_RET:
        break;
      case hadoop::hdfs::log_FuncType_READ:
        jobs.push_back(std::move(msg));
        break;
      case hadoop::hdfs::log_FuncType_READ_RET:
        break;
      default:
        std::cerr << "#" << (index + 1); 
        std::cerr << ": Unknown file operation." << std::endl;
    } 

    index++;
  }
  end = std::chrono::system_clock::now();
  std::chrono::duration<double> time = end - start;
  need_count = false;
  count_thread.join();

  if (!reader.isEOF()) {
    std::cerr << "Failed to parse log #" << (++index) << std::endl;
  } else {
    std::cout << "Total time: " << time.count() << " seconds. ";
    std::cout << "Total file operations: " << index / 2 << "." << std::endl; 
  }

  reader.close();
  hdfsDisconnect(fs);

  return 0;
}

/* Pass read info to the thread */
void getReadInfo(int bytes, double seconds)
{
  mtx.lock();
  read_bytes += bytes;
  run_time += seconds;
  mtx.unlock();
}

/* Calculate and print bandwidth info */
void printBandwidth()
{
  while(need_count) {
    mtx.lock(); 
    if (run_time > 0) {
      std::cout << "Current bandwidth: ";
      std::cout << read_bytes / run_time / MB << " MB/s" << std::endl; 
      read_bytes = 0;
      run_time = 0; 
    }
    mtx.unlock();

    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

/* spawn threads to handle file operation and wait them to finish */
void handleJobs()
{
  if (jobs.empty()) {
    return;
  }

  std::vector<std::thread> threads;
  long last_time = jobs[0]->time();

  for (int i = 0; i < (int)jobs.size(); ++i) {
    if (wait_before_new_thread) {
      long time = jobs[i]->time();
      std::this_thread::sleep_for(std::chrono::nanoseconds(time - last_time));
      last_time = time;
    }

    switch (jobs[i]->type()) {
      case hadoop::hdfs::log_FuncType_READ:
        threads.push_back(std::thread(handleRead, *jobs[i])); 
        break;
      default:
        ;
    }

    if (threads.size() == max_threads) { //avoid too many requests to hdfs
      for (int i = 0; i < (int)threads.size(); ++i) {
        threads[i].join(); 
      }

      threads.clear(); 
    }
  }

  for (int i = 0; i < (int)threads.size(); ++i) {
    threads[i].join(); 
  }

  jobs.clear();
}

void handleOpen(const hadoop::hdfs::log &msg)
{
  std::string path = msg.path();
  if (parent_folder != "") {
    if (path.at(0) == '/') {
      path = "/" +  parent_folder + path;
    } else {
      path = parent_folder + "/" + path;
    }
  }

  hdfsFile file = hdfsOpenFile(fs, path.c_str(), 
      (int)msg.argument(1), 
      (int)msg.argument(2), 
      (short)msg.argument(3), 
      (int)msg.argument(4));

  // Using thread id as key to temporarily store file here is safe.
  // In the same thread all operations are sequential, thus a OPEN
  // must be followed by an OPEN_RET. It's impossible in a single
  // thread to have multiple OPENs on halfway without return. In
  // addition, the value of thread id can hardly be equal to an
  // address value. So we don't need to worry about file being
  // overwritten by other things.
  files[msg.threadid()] = file;
}

void handleOpenRet(const hadoop::hdfs::log &msg)
{
  files[msg.argument(0)] = files[msg.threadid()];
  files.erase(msg.threadid());// safely delete the item
}

void handleRead(const hadoop::hdfs::log &msg)
{
  auto file = files.find(msg.argument(1));
  if (file != files.end()) {
    size_t buf_size = msg.argument(4);
    char* buffer = new char[buf_size];
    auto start = std::chrono::system_clock::now();

    auto ret = hdfsPread(fs, file->second, 
        (off_t)msg.argument(2), 
        reinterpret_cast<void*>(buffer), 
        buf_size);

    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    getReadInfo(ret, elapsed.count());

    delete[] buffer;
  } else {
    std::cerr << "Read: file " 
      << msg.argument(1) 
      << "not found." << std::endl;
  }
}

void handleClose(const hadoop::hdfs::log &msg)
{
  auto file = files.find(msg.argument(1));
  if (file != files.end()) {
    auto ret = hdfsCloseFile(fs, file->second);
    (void)ret;//make gcc happy
  } else {
    std::cerr << "Close: file " 
      << msg.argument(1) 
      << "not found." << std::endl;
  }
}

