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
        , opts()
        , args()
    {
        prog = argv[0];
        prog = prog.substr(prog.find_last_of("/") + 1);
        
        for (int i = 1; i < argc; ++i) {
            std::string arg(argv[i]);
            auto begin = arg.find("--");

            if (begin == 0) {//start with "--"
                auto isOpt = arg.find("=");

                if (isOpt == std::string::npos) {//is flag
                    arg = arg.substr(begin + 2);
                    flags[arg] = true;
                    return;
                }

                //is option
                std::string option = arg.substr(begin + 2, (isOpt - begin - 2));
                std::string value = arg.substr(isOpt + 1);
                if (value.at(0) == '/') {
                    value.erase(value.begin());//remove heading '/'
                }

                if (value.at(value.length() - 1) == '/') {
                    value.erase(value.length() - 1);//remove trailing '/'
                }

                opts[option] = value;
            } else if ((begin = arg.find("-")) == 0) {//start with "-"
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

    bool getOption(std::string option, std::string& value)
    {
        if (opts.find(option) == opts.end()) {
            return false;
        }

        value = opts.find(option)->second;
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
    std::map<std::string, std::string> opts;
    std::vector<std::string> args;
}; 

} /* hdfs */ 

