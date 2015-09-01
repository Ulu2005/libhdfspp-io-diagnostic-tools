#include <map>
#include <iostream>

#include "libhdfs++/chdfs.h"
#include "LogReader.h"
#include "LogWorker.h"

int main(int argc, const char* argv[]) {
  if (argc != 4) {
    std::cout << "Usage: " << argv[0];
    std::cout << " <log file> <host> <port>" << std::endl;
    return 0;
  } 

  hdfs::LogReader reader(argv[1]);
  auto fs = hdfsConnect(argv[2], std::atoi(argv[3])); 

  int index(0);
  std::mutex file_mtx, ref_mtx;
  std::unique_ptr<hadoop::hdfs::log> msg;
  std::map<long, hdfsFile> files;
  std::map<hdfsFile, int> ref_count;
  hdfs::WorkerMap workers;

  while((msg = reader.next()) != nullptr) {
    long id = msg->threadid();
    auto found = workers.find(id);

    if (found == workers.end()) {
      workers[id].reset(
        new hdfs::LogWorker(
          id, fs, &files, &ref_count, &file_mtx, &ref_mtx, &workers));
      workers[id]->start();
    }

    workers[id]->addJob(std::move(msg));
    index++;
  }

  if (!reader.isEOF()) {
    std::cerr << "Failed to parse log #" << (++index) << std::endl;
  }

  reader.close();
  hdfsDisconnect(fs);

  return 0;
}

