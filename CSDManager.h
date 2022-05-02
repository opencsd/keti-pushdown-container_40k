#include <iostream>
#include <unordered_map>

using namespace std;

class CSDManager{
    CSDManager(){CSDManagerInit();}
    struct CSDInfo{
        string CSDIP;
        string CSDReplica;
        int CSDWorkingBlock;
    };

    CSDInfo getCSDInfo(string CSDID);
    void CSDManagerInit();
    private:
        unordered_map<string,CSDManager::CSDInfo> CSD_Map_;
};