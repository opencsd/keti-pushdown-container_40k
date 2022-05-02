#include <iostream>
#include <unordered_map>

using namespace std;

class MetaManager{
    MetaManager(){MetaManagerInit();}
    void MetaManagerInit();
    string getCSDID(string sstname);
    private:
        unordered_map<string,string> sstcsd_map_;
};