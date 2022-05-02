#include "MetaManager.h"

void MetaManager::MetaManagerInit(){
    sstcsd_map_.insert(make_pair("000288.sst","1"));
    sstcsd_map_.insert(make_pair("000291.sst","2"));
    sstcsd_map_.insert(make_pair("000287.sst","3"));
    sstcsd_map_.insert(make_pair("000051.sst","3"));

}

string MetaManager::getCSDID(string sstname){
    return sstcsd_map_[sstname];
}