#include "CSDManager.h"


void CSDManager::CSDManagerInit(){
    CSDInfo initinfo;
    initinfo.CSDIP = "10.0.5.119+10.1.1.2";
    initinfo.CSDReplica = "4";
    initinfo.CSDWorkingBlock = 0;
    CSD_Map_.insert(make_pair("1",initinfo));
    initinfo.CSDIP = "10.0.5.119+10.1.2.2";
    initinfo.CSDReplica = "5";
    initinfo.CSDWorkingBlock = 0;
    CSD_Map_.insert(make_pair("2",initinfo));
    initinfo.CSDIP = "10.0.5.120+10.1.2.2";
    initinfo.CSDReplica = "5";
    initinfo.CSDWorkingBlock = 0;
    CSD_Map_.insert(make_pair("3",initinfo));
    initinfo.CSDIP = "10.0.5.120+10.1.1.2";
    initinfo.CSDReplica = "";
    initinfo.CSDWorkingBlock = 0;
    CSD_Map_.insert(make_pair("4",initinfo));
    initinfo.CSDIP = "10.0.5.119+10.1.3.2";
    initinfo.CSDReplica = "";
    initinfo.CSDWorkingBlock = 0;
    CSD_Map_.insert(make_pair("5",initinfo));
}

CSDManager::CSDInfo CSDManager::getCSDInfo(string CSDID){
    return CSD_Map_[CSDID];
}