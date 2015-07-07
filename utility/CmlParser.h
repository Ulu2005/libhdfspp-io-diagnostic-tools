#include <string>
#include <map>
#include <vector>

namespace hdfs
{
   
class CmlParser
{
public:
    CmlParser (const int argc, const char *argv[])
        : flags()
        , args()
    {
        prog = argv[0];
        prog = prog.substr(prog.find_last_of("/") + 1);
        
        for (int i = 1; i < argc; ++i) {
            std::string arg(argv[i]);
            auto begin = arg.find("--");

            if (begin != std::string::npos) {
                arg = arg.substr(begin + 2);
                flags[arg] = true;
            } else if ((begin = arg.find("-")) != std::string::npos) {
                arg = arg.substr(begin + 1);
                flags[arg] = true;
            } else {
                args.push_back(arg);  
            }
        } 
    }

    virtual ~CmlParser (){}

    std::string getProgName()
    {
        return prog;
    }

    bool getFlag(std::string flag)
    {
        if (flags.find(flag) == flags.end()) {
            return false;
        }

        return true;
    }

    std::string getArg(unsigned index)
    {
        return args.at(index); 
    }

    unsigned getArgSize()
    {
        return args.size();
    }

private:
    std::string prog;
    std::map<std::string, bool> flags;
    std::vector<std::string> args;
}; 

} /* hdfs */ 

