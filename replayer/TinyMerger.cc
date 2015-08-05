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

#include <vector>
#include <queue>
#include <string>
#include <iostream>
#include <chrono>
#include <sys/types.h>
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>

#include "LogReader.h"
#include "Logger.h"

#define LOG_NAME "libhdfspp_merged.log"

using namespace hdfs;
using logPtr = std::shared_ptr<hadoop::hdfs::log>;
using item = std::pair<logPtr, int>;

static Logger logger;

std::vector<LogReader> getReaders(DIR* dir, std::string parent);
void mergeLog(std::vector<LogReader> &readers);

int main(int argc, char *argv[])
{
  if (argc != 3) {
    std::cout << "Usage: " << argv[0] << " <input log file directory> ";
    std::cout << "<merged log file directory>" << std::endl;
    return 0;
  } 

  //initialize readers and output file stream
  DIR* dir = opendir(argv[1]);
  if (dir == NULL) {
    std::cout << "Failed to open log file directory." << std::endl;
    return 0; 
  }

  std::string inDir(argv[1]), outDir(argv[2]), output; 
  auto append_slash = [](std::string &str) {
    if (str.at(str.size() - 1) != '/') str.append("/");
  };
  append_slash(inDir);
  append_slash(outDir);

  std::vector<LogReader> readers = getReaders(dir, inDir);

  output = outDir + LOG_NAME;
  if (!logger.startLog(output.c_str())) {
    std::cout << "Failed to create merged log file." << std::endl;
    return 0; 
  }

  //merge log files 
  std::chrono::time_point<std::chrono::system_clock> start, end;

  std::cout << "Start to merge log files." << std::endl;
  start = std::chrono::system_clock::now();

  mergeLog(readers);

  //close files 
  for (auto r : readers) {
    r.close();
  }
  closedir(dir);

  end = std::chrono::system_clock::now();
  std::chrono::duration<double> time = end - start;
  std::cout << "Finish merging log files in " << time.count();
  std::cout << "seconds." << std::endl;

  return 0;
}

/* Get readers for all log file in directory */
std::vector<LogReader> getReaders(DIR* dir, std::string parent)
{
  std::vector<std::string> files;
  std::vector<LogReader> readers;
  struct dirent* entry;

  while ((entry = readdir(dir)) != NULL) {
    std::string name(entry->d_name);

    if (name.find(".log") != std::string::npos) {
      files.push_back(name);
      std::cout << "Reading " << name << std::endl; 
    }
  }
  
  for (auto filename : files) {
    readers.push_back(LogReader((parent + filename).c_str()));
  }

  return readers;
}

/* Read from multiple log files and merge them into a new one */
void mergeLog(std::vector<LogReader> &readers)
{
  // initialize the min heap
  auto item_comp = [](const item &l, const item &r){ 
    return (l.first->date() >= r.first->date()) 
      && (l.first->time() > r.first->time()); 
  };

  using heap = std::priority_queue<item, std::vector<item>, decltype(item_comp)>;
  heap  min_heap(item_comp);
  logPtr msg;

  for (int i = 0; i < (int)readers.size(); ++i) {
    msg.reset(readers.at(i).next().release());
    if (msg == nullptr) continue;
    min_heap.push(std::make_pair(msg, i)); 
  }

  // start n-way merge sort
  while (!min_heap.empty()) {
    auto min = min_heap.top();
    int index = min.second;
      
    logger.writeDelimitedLog(*min.first);
    min_heap.pop();

    msg.reset(readers.at(index).next().release());
    if (msg == nullptr) continue;
    min_heap.push(std::make_pair(msg, index)); 
  }
}

