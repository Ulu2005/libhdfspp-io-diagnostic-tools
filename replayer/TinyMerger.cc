#include <vector>
#include <string>
#include <iostream>
#include <sys/types.h>
#include <dirent.h>

#include "LogReader.h" 
using namespace hdfs;

void listFiles(DIR* dir, std::vector<std::string> &vec);
std::string getPid(std::string str);

int main(int argc, char *argv[])
{
    if (argc != 3) {
        std::cout << "Usage: " << argv[0] << " "
                  << "<log file directory> " 
                  << "<output file directory>" << std::endl;
        return 0;
    } 

    DIR* dir = opendir(argv[1]);
    if (dir == NULL) {
        std::cout << "Failed to open directory." << std::endl;
        return 0; 
    }
   
    std::vector<std::string> files;
    listFiles(dir, files);
    for (auto name : files) {
        std::cout << name << std::endl;
    }
    
    //TODO: parse file names and get pairs of index file and log file,
    //then merge log files

    closedir(dir);
    return 0;
}

/* Put all file names in a folder into a vector */
void listFiles(DIR* dir, std::vector<std::string> &vec)
{
    std::vector<std::string> tmp;
    struct dirent* entry;
    while ((entry = readdir(dir)) != NULL) {
        tmp.push_back(std::string(entry->d_name));
    }

    for (auto s : tmp) {
        if (s.find("log_") != std::string::npos) {
            vec.push_back(s); 
        } else if (s.find("index_") != std::string::npos) {
            vec.push_back(s); 
        }
    } 
}

/* Extract pid from string with format "log_xxxx.log" and "index_xxxx.log" */
std::string getPid(std::string str)
{
    std::size_t head;
    if ((head = str.find("log_")) == std::string::npos) {
        head = str.find("index_");
        head += 5; 
    } else {
        head += 4;
    }

    return str.substr(head, (str.size() - 4 - head));
}
