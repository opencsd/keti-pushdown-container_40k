#include "buffer_manager.h"

vector<string> split(string str, char Delimiter);

int64_t testoffset[10] = {43673280512, 43673284512, 43673288571, 43673292610, 43673296610, 43673300573, 43673304610, 43673308634, 43673312682, 43673316768};


void Scheduler::init_scheduler(){
    //여긴 csd 기본 데이터 가져오기 ip랑 등등
    //현재 구현 모듈 없음 추후 ngd 사용 시 추가 예정
    csdname_.push_back("1");
    csdname_.push_back("2");
    csdname_.push_back("3");
    csdname_.push_back("4");
    csdname_.push_back("5");
    // csd_.insert(make_pair("1","10.0.5.119+10.1.1.2")); //primary1
    // csd_.insert(make_pair("2","10.0.5.119+10.1.2.2")); //primary2 V
    // csd_.insert(make_pair("3","10.0.5.119+10.1.3.2")); //primary3
    // csd_.insert(make_pair("4","10.0.5.120+10.1.1.2")); //replica1
    // csd_.insert(make_pair("5","10.0.5.120+10.1.2.2")); //replica2,3

    csd_.insert(make_pair("1","10.0.5.119+10.1.1.2")); //primary1
    csd_.insert(make_pair("2","10.0.5.119+10.1.2.2")); //primary2 V
    csd_.insert(make_pair("3","10.0.5.120+10.1.2.2")); //primary3
    csd_.insert(make_pair("4","10.0.5.120+10.1.1.2")); //replica1
    csd_.insert(make_pair("5","10.0.5.119+10.1.1.2")); //replica2,3
    csdworkblock_.insert(make_pair("1",0));
    csdworkblock_.insert(make_pair("2",0));
    csdworkblock_.insert(make_pair("3",0));
    csdworkblock_.insert(make_pair("4",0));
    csdworkblock_.insert(make_pair("5",0));
    csdreaplicamap_.insert(make_pair("1","4"));
    csdreaplicamap_.insert(make_pair("2","5"));
    csdreaplicamap_.insert(make_pair("3","5"));
    sstcsd_.insert(make_pair("000288.sst","1"));
    sstcsd_.insert(make_pair("000291.sst","2"));
    sstcsd_.insert(make_pair("000287.sst","3"));

    sstcsd_.insert(make_pair("000051.sst","3"));
    sstcsd_.insert(make_pair("003140.sst","3"));
    blockcount_ = 0;

}

void Scheduler::sched(int workid, Value& blockinfo,vector<int> offset, vector<int> offlen, vector<int> datatype, vector<string> tablecol, Value& filter, string sstfilename, string tablename, string res){

    cout << "CSD SST File Map" << endl;
    for (auto i = sstcsd_.begin(); i != sstcsd_.end(); i++){
        pair<string,string> a = *i;
        cout << a.first << " " << a.second << endl;
    }
    
    cout << "CSD Primary CSD Replica" << endl;
    for(auto i = csdreaplicamap_.begin(); i != csdreaplicamap_.end();i++){
        pair<string,string> a = *i;
        cout << right << setw(5) << a.first << "          " << a.second << endl;
    }
    
    cout << "CSD Map : " << endl;
    cout << "CSD Name CSD IP" << endl;
    for(auto i = csd_.begin(); i != csd_.end(); i++){
        pair<std::string, std::string> a = *i;
        cout <<"   "<< a.first << "     " << a.second << endl;
    }
    //sst 파일 이름을 기준으로 그걸 가지고 있는 primary를 찾고, 그 primary에서 replica를 찾는다
    int blockworkcount = blockinfo.Size();
    cout << endl;
    cout << "SST File Name is : " << sstfilename << endl;
    cout << "Primary CSD is : " << sstcsd_[sstfilename] << endl;
    cout << "Replica CSD is : " << csdreaplicamap_[sstcsd_[sstfilename]] << endl;
    
    for (auto i = csdworkblock_.begin(); i != csdworkblock_.end(); i++){
        pair<string,int> a = *i;
        cout << "CSD " << a.first << " ";
    }
    cout << endl;
    
    for (auto i = csdworkblock_.begin(); i != csdworkblock_.end(); i++){
        pair<string,int> a = *i;
        cout << "  " << a.second << "   ";
    }
    cout << endl;
    
    string bestcsd = BestCSD(sstfilename,blockworkcount);
    for (auto i = csdworkblock_.begin(); i != csdworkblock_.end(); i++){
        pair<string,int> a = *i;
        cout << "CSD " << a.first << " ";
    }
    cout << endl;
    for (auto i = csdworkblock_.begin(); i != csdworkblock_.end(); i++){
        pair<string,int> a = *i;
        cout << "  " << a.second << "  ";
        if (a.second == 0){
            cout << " ";
        }
    }
    cout << endl;
    cout << "Best CSD is : " << bestcsd << endl;
    
    cout << "Column Name                                       ColOff   ColLength ColType" << endl;
    for (int i =0 ; i < tablecol.size();i++){
        cout << left << setw(50)<< tablecol[i] << " " << setw(7)<< offset[i] << " " << setw(9)<< offlen[i] << " " << setw(8)<< datatype[i] << endl;
    }
    
    Snippet snippet(workid,sstfilename,blockinfo,tablecol,filter,offset,offlen,datatype);
    // cout << filter[0]["LV"].GetInt() << endl;
    StringBuffer snippetbuf;
    Writer<StringBuffer> writer(snippetbuf);
    cout << res << endl;
    
    // bestcsd = "10.1.3.2";
    Serialize(writer, snippet,split(csd_[bestcsd],'+')[1],tablename,bestcsd);
    cout << "Snippet is : " << endl;
    cout << snippetbuf.GetString() << endl;
    
    
    // csd_[bestcsd] = "10.1.2.2";
    sendsnippet(snippetbuf.GetString(), split(csd_[bestcsd],'+')[0]);
    // cout << "234234" << endl;
}

void Scheduler::sendsnippet(string snippet, string ipaddr){
    int sock;
    struct sockaddr_in serv_addr;
    sock = socket(PF_INET, SOCK_STREAM, 0);
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    // string storageip = split(ipaddr,'+')[0];
    // ipaddr = "11+10.1.1.2";
    // string csdip = split(ipaddr,'+')[1];
    snippet = snippet;
    // serv_addr.sin_addr.s_addr = inet_addr(storageip.c_str());
    // serv_addr.sin_port = htons(8080);
    // cout << 1 << endl;
    serv_addr.sin_addr.s_addr = inet_addr("10.0.5.119");
    serv_addr.sin_port = htons(10100);
    connect(sock,(struct sockaddr*)&serv_addr,sizeof(serv_addr));
    // cout << 2 << endl;
    // send(sock,(char*)&len, sizeof(int),0);
	
	size_t len = strlen(snippet.c_str());
	send(sock,&len,sizeof(len),0);
    send(sock,(char*)snippet.c_str(),strlen(snippet.c_str()),0);
    // cout << 3 << endl;
    // sleep(1);
    // connect(sock,(struct sockaddr*)&serv_addr,sizeof(serv_addr));
    // send(sock,(char*)csdip.c_str(),strlen(csdip.c_str()),0);
}


string Scheduler::BestCSD(string sstname, int blockworkcount){
    string sstincsd = sstcsd_[sstname];
    string csdreplica = csdreaplicamap_[sstincsd];
    
    // csd 구조 잡고서 수정해야함, work도 가중치 둬야함(스캔과 필터)
    cout << "Primary Working Block Num is : " << csdworkblock_[sstincsd] << endl;
    cout << "Replica Working Block Num is : " << csdworkblock_[csdreplica] << endl;
    if (csdworkblock_[csdreplica] > csdworkblock_[sstincsd]){
        csdworkblock_[sstincsd] = csdworkblock_[sstincsd] + blockworkcount;
        return sstincsd;
    }
    csdworkblock_[csdreplica] = csdworkblock_[csdreplica] + blockworkcount;
    return csdreplica;
}

void Scheduler::csdworkdec(string csdname){
    csdworkblock_[csdname] = csdworkblock_[csdname] - 1;
}


void Scheduler::Serialize(Writer<StringBuffer>& writer, Snippet& s, string csd_ip,string tablename, string CSDName) {
    // cout << 1111111111 << endl;
    writer.StartObject();
    writer.Key("Snippet");
    writer.StartObject();
    writer.Key("WorkID");
    writer.Int(s.work_id);
    writer.Key("table_name");
    writer.String(tablename.c_str());
    writer.Key("table_col");
    writer.StartArray();
    for (int i = 0; i < s.table_col.size(); i ++){
        writer.String(s.table_col[i].c_str());
    }
    writer.EndArray();
    writer.Key("table_filter");
    writer.StartArray();
    for (int i =0; i < s.table_filter.Size(); i ++){
        writer.StartObject();
        if(s.table_filter[i].HasMember("LV")){
            writer.Key("LV");
            if(s.table_filter[i]["LV"].IsString()){
                writer.String(s.table_filter[i]["LV"].GetString());
            }else{
                writer.Int(s.table_filter[i]["LV"].GetInt());
            }
        }
        writer.Key("OPERATOR");
        writer.Int(s.table_filter[i]["OPERATOR"].GetInt());
        if(s.table_filter[i].HasMember("RV")){
            writer.Key("RV");
            if(s.table_filter[i]["RV"].IsString()){
                writer.String(s.table_filter[i]["RV"].GetString());
            }else{
                writer.Int(s.table_filter[i]["RV"].GetInt());
            }
        }else if (s.table_filter[i].HasMember("Extra")){
            writer.Key("EXTRA");
            writer.StartArray();
            for(int j = 0; j < s.table_filter[i]["Extra"].Size(); j++){
                if(s.table_filter[i]["Extra"][j].IsString()){
                    writer.String(s.table_filter[i]["Extra"][j].GetString());
                }else{
                    writer.Int(s.table_filter[i]["Extra"][j].GetInt());
                }
            }
            writer.EndArray();
        }
        writer.EndObject();
    }
    writer.EndArray();
    writer.Key("table_offset");
    writer.StartArray();
    for (int i =0; i < s.table_offset.size(); i ++){
        writer.Int(s.table_offset[i]);
    }
    writer.EndArray();
    writer.Key("table_offlen");
    writer.StartArray();
    for (int i =0; i < s.table_offlen.size(); i ++){
        writer.Int(s.table_offlen[i]);
    }
    writer.EndArray();
    writer.Key("table_datatype");
    writer.StartArray();
    for (int i =0; i < s.table_datatype.size(); i ++){
        writer.Int(s.table_datatype[i]);
    }
    writer.EndArray();
    writer.Key("BlockList");
    writer.StartArray();
    for (int i =0; i < s.block_info_list.Size(); i ++){
        writer.StartObject();
        writer.Key("BlockID");
        writer.Int(blockcount_);
        blockvec.push_back(blockcount_);
        blockcount_++;
        //cout << s.block_info_list[i]["SEQ ID"].GetInt() << endl;
        writer.Key("Offset");
        writer.Int64(s.block_info_list[i]["Offset"].GetInt64());
        // writer.Int64(testoffset[i]);
        writer.Key("Length");
        writer.Int(s.block_info_list[i]["Length"].GetInt());
        writer.EndObject();
    }
    writer.EndArray();
    writer.Key("CSD Name");
    writer.String(CSDName.c_str());
    writer.EndObject();
    writer.Key("CSD IP");
    writer.String(csd_ip.c_str());
    writer.EndObject();
}

vector<string> split(string str, char Delimiter)
{
    istringstream iss(str); // istringstream에 str을 담는다.
    string buffer;          // 구분자를 기준으로 절삭된 문자열이 담겨지는 버퍼

    vector<string> result;

    // istringstream은 istream을 상속받으므로 getline을 사용할 수 있다.
    while (getline(iss, buffer, Delimiter))
    {
        result.push_back(buffer); // 절삭된 문자열을 vector에 저장
    }

    return result;
}