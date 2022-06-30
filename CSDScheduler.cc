#include "buffer_manager.h"

vector<string> split(string str, char Delimiter);

int64_t testoffset[10] = {43673280512, 43673284512, 43673288571, 43673292610, 43673296610, 43673300573, 43673304610, 43673308634, 43673312682, 43673316768};

void Scheduler::init_scheduler()
{
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

    csd_.insert(make_pair("1", "10.0.5.119+10.1.1.2")); // primary1
    csd_.insert(make_pair("2", "10.0.5.119+10.1.2.2")); // primary2 V
    csd_.insert(make_pair("3", "10.0.5.119+10.1.3.2")); // primary3
    csd_.insert(make_pair("4", "10.0.5.119+10.1.4.2")); // replica1
    csd_.insert(make_pair("5", "10.0.5.119+10.1.5.2")); // replica2,3
    csd_.insert(make_pair("6", "10.0.5.119+10.1.6.2")); // replica2,3
    csd_.insert(make_pair("7", "10.0.5.119+10.1.7.2")); // replica2,3
    csd_.insert(make_pair("8", "10.0.5.119+10.1.8.2")); // replica2,3
    csdworkblock_.insert(make_pair("1", 0));
    csdworkblock_.insert(make_pair("2", 0));
    csdworkblock_.insert(make_pair("3", 0));
    csdworkblock_.insert(make_pair("4", 0));
    
    csdworkblock_.insert(make_pair("5", 0));
    // csdreaplicamap_.insert(make_pair("1","4"));
    // csdreaplicamap_.insert(make_pair("2","5"));
    // csdreaplicamap_.insert(make_pair("3","1"));
    sstcsd_.insert(make_pair("000288.sst", "1"));
    sstcsd_.insert(make_pair("000291.sst", "2"));
    sstcsd_.insert(make_pair("000287.sst", "3"));

    sstcsd_.insert(make_pair("000027.sst", "3"));
    sstcsd_.insert(make_pair("000287.sst", "3"));
    sstcsd_.insert(make_pair("001095.sst", "1"));

    sstcsd_.insert(make_pair("000051.sst", "3"));
    sstcsd_.insert(make_pair("003140.sst", "1"));

    sstcsd_.insert(make_pair("008011.sst", "1"));
    sstcsd_.insert(make_pair("007996.sst", "1"));
    sstcsd_.insert(make_pair("006230.sst", "1"));
    sstcsd_.insert(make_pair("006226.sst", "1"));

    sstcsd_.insert(make_pair("000868.sst", "1"));
    sstcsd_.insert(make_pair("001849.sst", "1"));
    sstcsd_.insert(make_pair("006184.sst", "1"));
    sstcsd_.insert(make_pair("007088.sst", "1"));
    sstcsd_.insert(make_pair("007497.sst", "1"));
    sstcsd_.insert(make_pair("007629.sst", "1"));
    sstcsd_.insert(make_pair("007876.sst", "1"));
    sstcsd_.insert(make_pair("009154.sst", "1"));

    sstcsd_.insert(make_pair("000883.sst", "2"));
    sstcsd_.insert(make_pair("001865.sst", "2"));
    sstcsd_.insert(make_pair("006195.sst", "2"));
    sstcsd_.insert(make_pair("007104.sst", "2"));
    sstcsd_.insert(make_pair("007513.sst", "2"));
    sstcsd_.insert(make_pair("007645.sst", "2"));
    sstcsd_.insert(make_pair("007877.sst", "2"));
    sstcsd_.insert(make_pair("009170.sst", "2"));

    sstcsd_.insert(make_pair("000926.sst", "3"));
    sstcsd_.insert(make_pair("001882.sst", "3"));
    sstcsd_.insert(make_pair("006196.sst", "3"));
    sstcsd_.insert(make_pair("007134.sst", "3"));
    sstcsd_.insert(make_pair("007530.sst", "3"));
    sstcsd_.insert(make_pair("007662.sst", "3"));
    sstcsd_.insert(make_pair("007878.sst", "3"));
    sstcsd_.insert(make_pair("009187.sst", "3"));

    sstcsd_.insert(make_pair("000943.sst", "4"));
    sstcsd_.insert(make_pair("001898.sst", "4"));
    sstcsd_.insert(make_pair("006208.sst", "4"));
    sstcsd_.insert(make_pair("007150.sst", "4"));
    sstcsd_.insert(make_pair("007546.sst", "4"));
    sstcsd_.insert(make_pair("007678.sst", "4"));
    sstcsd_.insert(make_pair("007879.sst", "4"));
    sstcsd_.insert(make_pair("009203.sst", "4"));

    sstcsd_.insert(make_pair("000973.sst", "5"));
    sstcsd_.insert(make_pair("001915.sst", "5"));
    sstcsd_.insert(make_pair("006209.sst", "5"));
    sstcsd_.insert(make_pair("007167.sst", "5"));
    sstcsd_.insert(make_pair("007563.sst", "5"));
    sstcsd_.insert(make_pair("007695.sst", "5"));
    sstcsd_.insert(make_pair("007880.sst", "5"));
    sstcsd_.insert(make_pair("009220.sst", "5"));

    sstcsd_.insert(make_pair("000989.sst", "6"));
    sstcsd_.insert(make_pair("001931.sst", "6"));
    sstcsd_.insert(make_pair("006214.sst", "6"));
    sstcsd_.insert(make_pair("007183.sst", "6"));
    sstcsd_.insert(make_pair("007579.sst", "6"));
    sstcsd_.insert(make_pair("007711.sst", "6"));
    sstcsd_.insert(make_pair("007881.sst", "6"));
    sstcsd_.insert(make_pair("009236.sst", "6"));

    sstcsd_.insert(make_pair("001019.sst", "7"));
    sstcsd_.insert(make_pair("001948.sst", "7"));
    sstcsd_.insert(make_pair("007200.sst", "7"));
    sstcsd_.insert(make_pair("007596.sst", "7"));
    sstcsd_.insert(make_pair("007728.sst", "7"));
    sstcsd_.insert(make_pair("007882.sst", "7"));
    sstcsd_.insert(make_pair("009253.sst", "7"));

    sstcsd_.insert(make_pair("001035.sst", "8"));
    sstcsd_.insert(make_pair("001964.sst", "8"));
    sstcsd_.insert(make_pair("007216.sst", "8"));
    sstcsd_.insert(make_pair("007612.sst", "8"));
    sstcsd_.insert(make_pair("007875.sst", "8"));
    sstcsd_.insert(make_pair("007883.sst", "8"));
    sstcsd_.insert(make_pair("009269.sst", "8"));

    blockcount_ = 0;
}

void Scheduler::sched(int indexdata)
{

    // cout << "CSD SST File Map" << endl;
    // for (auto i = sstcsd_.begin(); i != sstcsd_.end(); i++){
    //     pair<string,string> a = *i;
    //     cout << a.first << " " << a.second << endl;
    // }

    // cout << "CSD Primary CSD Replica" << endl;
    // for(auto i = csdreaplicamap_.begin(); i != csdreaplicamap_.end();i++){
    //     pair<string,string> a = *i;
    //     cout << right << setw(5) << a.first << "          " << a.second << endl;
    // }

    // cout << "CSD Map : " << endl;
    // cout << "CSD Name CSD IP" << endl;
    // for(auto i = csd_.begin(); i != csd_.end(); i++){
    //     pair<std::string, std::string> a = *i;
    //     cout <<"   "<< a.first << "     " << a.second << endl;
    // }
    // sst 파일 이름을 기준으로 그걸 가지고 있는 primary를 찾고, 그 primary에서 replica를 찾는다
    // int blockworkcount = blockinfo.Size();
    // cout << 1 << endl;
    int blockworkcount = snippetdata.block_info_list[indexdata][snippetdata.sstfilelist[indexdata].c_str()].Size();
    // cout << 2 << endl;
    // cout << endl;
    // cout << "SST File Name is : " << sstfilename << endl;
    // cout << "Primary CSD is : " << sstcsd_[sstfilename] << endl;
    // cout << "Replica CSD is : " << csdreaplicamap_[sstcsd_[sstfilename]] << endl;

    // for (auto i = csdworkblock_.begin(); i != csdworkblock_.end(); i++){
    //     pair<string,int> a = *i;
    //     cout << "CSD " << a.first << " ";
    // }
    // cout << endl;

    // for (auto i = csdworkblock_.begin(); i != csdworkblock_.end(); i++){
    //     pair<string,int> a = *i;
    //     cout << "  " << a.second << "   ";
    // }
    // cout << endl;

    string bestcsd = BestCSD(snippetdata.sstfilelist[indexdata], blockworkcount);
    // for (auto i = csdworkblock_.begin(); i != csdworkblock_.end(); i++){
    //     pair<string,int> a = *i;
    //     cout << "CSD " << a.first << " ";
    // }
    // cout << endl;
    // for (auto i = csdworkblock_.begin(); i != csdworkblock_.end(); i++){
    //     pair<string,int> a = *i;
    //     cout << "  " << a.second << "  ";
    //     if (a.second == 0){
    //         cout << " ";
    //     }
    // }
    // cout << endl;
    // cout << "Best CSD is : " << bestcsd << endl;

    // cout << "Column Name                                       ColOff   ColLength ColType" << endl;
    // for (int i =0 ; i < tablecol.size();i++){
    //     cout << left << setw(50)<< tablecol[i] << " " << setw(7)<< offset[i] << " " << setw(9)<< offlen[i] << " " << setw(8)<< datatype[i] << endl;
    // }
    // cout << snippetdata.sstfilelist[indexdata].c_str() << endl;
    Snippet snippet(snippetdata.work_id, snippetdata.sstfilelist[indexdata], snippetdata.block_info_list[indexdata][snippetdata.sstfilelist[indexdata].c_str()], snippetdata.table_col, snippetdata.table_filter, snippetdata.table_offset, snippetdata.table_offlen, snippetdata.table_datatype);
    // cout << filter[0]["LV"].GetInt() << endl;
    StringBuffer snippetbuf;
    snippetbuf.Clear();
    Writer<StringBuffer> writer(snippetbuf);
    // cout << res << endl;

    // bestcsd = "10.1.3.2";
    Serialize(writer, snippet, split(csd_[bestcsd], '+')[1], snippetdata.tablename, bestcsd, threadblocknum[indexdata]);
    // cout << "Snippet is : " << endl;
    // cout << snippetbuf.GetString() << endl;
    
    // csd_[bestcsd] = "10.1.2.2";
    sendsnippet(snippetbuf.GetString(), split(csd_[bestcsd], '+')[0]);
}

void Scheduler::Serialize(Writer<StringBuffer> &writer, Snippet &s, string csd_ip, string tablename, string CSDName, int blockidnum)
{
    // vector<string>
    writer.StartObject();
    writer.Key("Snippet");
    writer.StartObject();
    writer.Key("WorkID");
    writer.Int(s.work_id);
    writer.Key("table_name");
    writer.String(tablename.c_str());
    writer.Key("table_col");
    writer.StartArray();
    for (int i = 0; i < s.table_col.size(); i++)
    {
        writer.String(s.table_col[i].c_str());
    }
    writer.EndArray();
    // cout << s.table_filter.HasMember("filter") << endl;
    // cout << 1 << endl;
    // if (!s.table_filter.IsNull())
    // {
    // cout << 1 << endl;
    // cout << s.table_filter.HasMember("filter") << endl;
    // s.table_filter = s.table_filter["filter"];
    // cout << 1 << endl;
    // cout << s.table_filter[0]["LV"].GetString() << endl;
    // if(s.table_filter.Size() != )

    // for (int i = 0; i < s.table_filter.Size(); i++)
    // {
    //     //여기서 먼저 확인을 진행 후 빼야할 필터 절 빼기
    //     if (!s.table_filter[i].HasMember("LV") || !s.table_filter[i].HasMember("RV"))
    //     {
    //         continue;
    //     }
    //     if (!s.table_filter[i]["LV"].IsString() || !s.table_filter[i]["RV"].IsString())
    //     {
    //         continue;
    //     }
    //     string LV = s.table_filter[i]["LV"].GetString();
    //     int filteroper = s.table_filter[i]["OPERATOR"].GetInt();
    //     string cmpoper = "=";
    //     char SubLV = LV[0];
    //     string RV = s.table_filter[i]["RV"].GetString();
    //     string tmpv;
    //     char SubRV = RV[0];
    //     // cout << find(s.table_col.begin(), s.table_col.end(), LV) << endl;
    //     // for(auto it = s.table_col.begin(); it != s.table_col.end(); it++){
    //     //     cout << *it << endl;
    //     // }
    //     // cout << LV << endl;
    //     if ((SubLV != '+' && SubRV != '+' && filteroper == 4) && find(s.table_col.begin(), s.table_col.end(), LV) == s.table_col.end())
    //     {
    //         //내 테이블이 아닐때의 조건도 추가를 해야할듯?
    //         cout << 1123123123 << endl;
    //         if (find(s.table_col.begin(), s.table_col.end(), RV) != s.table_col.end())
    //         {
    //             tmpv = RV;
    //             RV = LV;
    //             LV = tmpv;
    //         }
    //         else
    //         {
    //             continue;
    //         }
    //         savedfilter.push_back(make_tuple(LV, cmpoper, RV));
    //         passindex.push_back(i);
    //         passindex.push_back(i + 1);
    //     }
    //     else if ((SubLV != '+' && SubRV != '+' && filteroper == 4) && find(s.table_col.begin(), s.table_col.end(), LV) != s.table_col.end())
    //     {
    //         // cout << "LV : " << LV << endl;
    //         if (find(s.table_col.begin(), s.table_col.end(), RV) == s.table_col.end())
    //         {
    //             savedfilter.push_back(make_tuple(LV, cmpoper, RV));
    //             passindex.push_back(i);
    //             passindex.push_back(i + 1);
    //         }
    //     }
    // }


    if (s.table_filter.Size() != passindex.size())
    {
        writer.Key("table_filter");
        writer.StartArray();
        for (int i = 0; i < s.table_filter.Size(); i++)
        {
            if (find(passindex.begin(), passindex.end(), i) != passindex.end())
            {
                // cout << 2 << endl;
                continue;
            }
            writer.StartObject();
            if (s.table_filter[i].HasMember("LV"))
            {
                writer.Key("LV");
                if (s.table_filter[i]["LV"].IsString())
                {
                    writer.String(s.table_filter[i]["LV"].GetString());
                }
                else
                {
                    writer.Int(s.table_filter[i]["LV"].GetInt());
                }
            }
            // if(i != s.table_filter.Size() -1){
            writer.Key("OPERATOR");
            writer.Int(s.table_filter[i]["OPERATOR"].GetInt());
            // }
            // cout << 2 << endl;
            if (s.table_filter[i].HasMember("RV"))
            {
                writer.Key("RV");
                if (s.table_filter[i]["RV"].IsString())
                {
                    writer.String(s.table_filter[i]["RV"].GetString());
                }
                else
                {
                    writer.Int(s.table_filter[i]["RV"].GetInt());
                }
            }
            else if (s.table_filter[i].HasMember("Extra"))
            {
                writer.Key("EXTRA");
                writer.StartArray();
                for (int j = 0; j < s.table_filter[i]["Extra"].Size(); j++)
                {
                    if (s.table_filter[i]["Extra"][j].IsString())
                    {
                        writer.String(s.table_filter[i]["Extra"][j].GetString());
                    }
                    else
                    {
                        writer.Int(s.table_filter[i]["Extra"][j].GetInt());
                    }
                }
                writer.EndArray();
            }
            writer.EndObject();
        }
    
    writer.EndArray();
    }

    writer.Key("table_offset");
    writer.StartArray();
    for (int i = 0; i < s.table_offset.size(); i++)
    {
        writer.Int(s.table_offset[i]);
    }
    writer.EndArray();

    writer.Key("table_offlen");
    writer.StartArray();
    for (int i = 0; i < s.table_offlen.size(); i++)
    {
        writer.Int(s.table_offlen[i]);
    }
    writer.EndArray();

    writer.Key("table_datatype");
    writer.StartArray();
    for (int i = 0; i < s.table_datatype.size(); i++)
    {
        writer.Int(s.table_datatype[i]);
    }
    writer.EndArray();

    writer.Key("BlockInfo");
    writer.StartObject();

    writer.Key("BlockID");
    writer.Int(blockidnum);

    writer.Key("BlockList");
    writer.StartArray();
    writer.StartObject();
    for (int i = 0; i < s.block_info_list.Size(); i++)
    {
        // blockvec.push_back(blockcount_);
        // cout << blockcount_ << endl;
        // blockidnum++;
        // cout << s.block_info_list[i]["SEQ ID"].GetInt() << endl;
        if(i > 0){
            // cout << s.block_info_list[i-1]["Offset"].GetInt64() << " " << 
            if(s.block_info_list[i-1]["Offset"].GetInt64()+s.block_info_list[i-1]["Length"].GetInt() != s.block_info_list[i]["Offset"].GetInt64()){
                writer.EndArray();
                writer.EndObject();
                // writer.StartObject();
                writer.StartObject();
                writer.Key("Offset");
                // cout << s.block_info_list[i]["Offset"].GetInt64() << endl;
                writer.Int64(s.block_info_list[i]["Offset"].GetInt64());
                writer.Key("Length");
                
                writer.StartArray();
            }
        }else{
            writer.Key("Offset");
            writer.Int64(s.block_info_list[i]["Offset"].GetInt64());
            writer.Key("Length");
            writer.StartArray();
        }

        writer.Int(s.block_info_list[i]["Length"].GetInt());

        if(i == s.block_info_list.Size() - 1){
            writer.EndArray();
            writer.EndObject();
        }
    }
    writer.EndArray();
    writer.EndObject();

    writer.Key("primary_key");
    writer.Int(0);

    writer.Key("CSD Name");
    writer.String(CSDName.c_str());
    writer.EndObject();

    writer.Key("CSD IP");
    writer.String(csd_ip.c_str());
    writer.EndObject();
}

void Scheduler::sched(int workid, Value &blockinfo, vector<int> offset, vector<int> offlen, vector<int> datatype, vector<string> tablecol, Value &filter, string sstfilename, string tablename, string res)
{
    // cout << "CSD SST File Map" << endl;
    // for (auto i = sstcsd_.begin(); i != sstcsd_.end(); i++){
    //     pair<string,string> a = *i;
    //     cout << a.first << " " << a.second << endl;
    // }

    // cout << "CSD Primary CSD Replica" << endl;
    // for(auto i = csdreaplicamap_.begin(); i != csdreaplicamap_.end();i++){
    //     pair<string,string> a = *i;
    //     cout << right << setw(5) << a.first << "          " << a.second << endl;
    // }

    // cout << "CSD Map : " << endl;
    // cout << "CSD Name CSD IP" << endl;
    // for(auto i = csd_.begin(); i != csd_.end(); i++){
    //     pair<std::string, std::string> a = *i;
    //     cout <<"   "<< a.first << "     " << a.second << endl;
    // }
    // sst 파일 이름을 기준으로 그걸 가지고 있는 primary를 찾고, 그 primary에서 replica를 찾는다
    int blockworkcount = blockinfo.Size();
    // cout << endl;
    // cout << "SST File Name is : " << sstfilename << endl;
    // cout << "Primary CSD is : " << sstcsd_[sstfilename] << endl;
    // cout << "Replica CSD is : " << csdreaplicamap_[sstcsd_[sstfilename]] << endl;

    // for (auto i = csdworkblock_.begin(); i != csdworkblock_.end(); i++){
    //     pair<string,int> a = *i;
    //     cout << "CSD " << a.first << " ";
    // }
    // cout << endl;

    // for (auto i = csdworkblock_.begin(); i != csdworkblock_.end(); i++){
    //     pair<string,int> a = *i;
    //     cout << "  " << a.second << "   ";
    // }
    // cout << endl;

    string bestcsd = BestCSD(sstfilename, blockworkcount);
    // for (auto i = csdworkblock_.begin(); i != csdworkblock_.end(); i++){
    //     pair<string,int> a = *i;
    //     cout << "CSD " << a.first << " ";
    // }
    // cout << endl;
    // for (auto i = csdworkblock_.begin(); i != csdworkblock_.end(); i++){
    //     pair<string,int> a = *i;
    //     cout << "  " << a.second << "  ";
    //     if (a.second == 0){
    //         cout << " ";
    //     }
    // }
    // cout << endl;
    // cout << "Best CSD is : " << bestcsd << endl;

    // cout << "Column Name                                       ColOff   ColLength ColType" << endl;
    // for (int i =0 ; i < tablecol.size();i++){
    //     cout << left << setw(50)<< tablecol[i] << " " << setw(7)<< offset[i] << " " << setw(9)<< offlen[i] << " " << setw(8)<< datatype[i] << endl;
    // }

    Snippet snippet(workid, sstfilename, blockinfo, tablecol, filter, offset, offlen, datatype);
    // cout << filter[0]["LV"].GetInt() << endl;
    StringBuffer snippetbuf;
    PrettyWriter<StringBuffer> writer(snippetbuf);

    // bestcsd = "10.1.3.2";
    Serialize(writer, snippet, split(csd_[bestcsd], '+')[1], tablename, bestcsd);
    // cout << "Snippet is : " << endl;
    // cout << snippetbuf.GetString() << endl;

    // csd_[bestcsd] = "10.1.2.2";
    sendsnippet(snippetbuf.GetString(), split(csd_[bestcsd], '+')[0]);
}

void Scheduler::sendsnippet(string snippet, string ipaddr)
{
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
    serv_addr.sin_addr.s_addr = inet_addr("10.0.5.119");
    serv_addr.sin_port = htons(10100);
    connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr));
    // send(sock,(char*)&len, sizeof(int),0);

    size_t len = strlen(snippet.c_str());
    send(sock, &len, sizeof(len), 0);
    send(sock, (char *)snippet.c_str(), strlen(snippet.c_str()), 0);
    // sleep(1);
    // connect(sock,(struct sockaddr*)&serv_addr,sizeof(serv_addr));
    // send(sock,(char*)csdip.c_str(),strlen(csdip.c_str()),0);
}

string Scheduler::BestCSD(string sstname, int blockworkcount)
{
    string sstincsd = sstcsd_[sstname];
    string csdreplica = csdreaplicamap_[sstincsd];

    // csd 구조 잡고서 수정해야함, work도 가중치 둬야함(스캔과 필터)
    // cout << "Primary Working Block Num is : " << csdworkblock_[sstincsd] << endl;
    // cout << "Replica Working Block Num is : " << csdworkblock_[csdreplica] << endl;

    // test code
    return sstincsd;

    if (csdworkblock_[csdreplica] > csdworkblock_[sstincsd])
    {
        csdworkblock_[sstincsd] = csdworkblock_[sstincsd] + blockworkcount;
        return sstincsd;
    }
    csdworkblock_[csdreplica] = csdworkblock_[csdreplica] + blockworkcount;
    return csdreplica;
}

void Scheduler::csdworkdec(string csdname, int num)
{
    csdworkblock_[csdname] = csdworkblock_[csdname] - num;
}

void Scheduler::Serialize(PrettyWriter<StringBuffer> &writer, Snippet &s, string csd_ip, string tablename, string CSDName)
{
    // cout << 1111111111 << endl;
    // vector<string>
    writer.StartObject();
    writer.Key("Snippet");
    writer.StartObject();
    writer.Key("WorkID");
    writer.Int(s.work_id);
    writer.Key("table_name");
    writer.String(tablename.c_str());
    writer.Key("table_col");
    writer.StartArray();
    for (int i = 0; i < s.table_col.size(); i++)
    {
        writer.String(s.table_col[i].c_str());
    }
    writer.EndArray();
    // cout << 1 << endl;
    if (!s.table_filter.IsNull())
    {
        // cout << 1 << endl;
        // cout << s.table_filter.HasMember("filter") << endl;
        // s.table_filter = s.table_filter["filter"];
        for (int i = 0; i < s.table_filter.Size(); i++)
        {
            //여기서 먼저 확인을 진행 후 빼야할 필터 절 빼기
            if (!s.table_filter[i].HasMember("LV") || !s.table_filter[i].HasMember("RV"))
            {
                continue;
            }
            if (!s.table_filter[i]["LV"].IsString() || !s.table_filter[i]["RV"].IsString())
            {
                continue;
            }
            string LV = s.table_filter[i]["LV"].GetString();
            int filteroper = s.table_filter[i]["OPERATOR"].GetInt();
            string cmpoper = "=";
            char SubLV = LV[0];
            string RV = s.table_filter[i]["RV"].GetString();
            string tmpv;
            char SubRV = RV[0];
            if ((SubLV != '+' && SubRV != '+' && filteroper == 4) && find(s.table_col.begin(), s.table_col.end(), LV) == s.table_col.end())
            {
                //내 테이블이 아닐때의 조건도 추가를 해야할듯?
                if (find(s.table_col.begin(), s.table_col.end(), RV) != s.table_col.end())
                {
                    tmpv = RV;
                    RV = LV;
                    LV = tmpv;
                }
                else
                {
                    continue;
                }
                savedfilter.push_back(make_tuple(LV, cmpoper, RV));
                passindex.push_back(i);
                passindex.push_back(i + 1);
            }
            else if ((SubLV != '+' && SubRV != '+' && filteroper == 4) && find(s.table_col.begin(), s.table_col.end(), LV) != s.table_col.end())
            {
                if (find(s.table_col.begin(), s.table_col.end(), RV) == s.table_col.end())
                {
                    savedfilter.push_back(make_tuple(LV, cmpoper, RV));
                    passindex.push_back(i);
                    passindex.push_back(i + 1);
                }
            }
        }
        // cout << 1 << endl;
        if (s.table_filter.Size() > passindex.size())
        {
            writer.Key("table_filter");
            writer.StartArray();
            for (int i = 0; i < s.table_filter.Size(); i++)
            {
                if (find(passindex.begin(), passindex.end(), i) != passindex.end())
                {
                    continue;
                }
                writer.StartObject();
                if (s.table_filter[i].HasMember("LV"))
                {
                    writer.Key("LV");
                    if (s.table_filter[i]["LV"].IsString())
                    {
                        writer.String(s.table_filter[i]["LV"].GetString());
                    }
                    else
                    {
                        writer.Int(s.table_filter[i]["LV"].GetInt());
                    }
                }
                writer.Key("OPERATOR");
                writer.Int(s.table_filter[i]["OPERATOR"].GetInt());
                if (s.table_filter[i].HasMember("RV"))
                {
                    writer.Key("RV");
                    if (s.table_filter[i]["RV"].IsString())
                    {
                        writer.String(s.table_filter[i]["RV"].GetString());
                    }
                    else
                    {
                        writer.Int(s.table_filter[i]["RV"].GetInt());
                    }
                }
                else if (s.table_filter[i].HasMember("Extra"))
                {
                    writer.Key("EXTRA");
                    writer.StartArray();
                    for (int j = 0; j < s.table_filter[i]["Extra"].Size(); j++)
                    {
                        if (s.table_filter[i]["Extra"][j].IsString())
                        {
                            writer.String(s.table_filter[i]["Extra"][j].GetString());
                        }
                        else
                        {
                            writer.Int(s.table_filter[i]["Extra"][j].GetInt());
                        }
                    }
                    writer.EndArray();
                }
                writer.EndObject();
            }
            // cout << 2 << endl;
            passindex.clear();
            // cout << 1 << endl;
            writer.EndArray();
        }
    }
    writer.Key("table_offset");
    writer.StartArray();
    for (int i = 0; i < s.table_offset.size(); i++)
    {
        writer.Int(s.table_offset[i]);
    }
    writer.EndArray();
    writer.Key("table_offlen");
    writer.StartArray();
    for (int i = 0; i < s.table_offlen.size(); i++)
    {
        writer.Int(s.table_offlen[i]);
    }
    writer.EndArray();
    writer.Key("table_datatype");
    writer.StartArray();
    for (int i = 0; i < s.table_datatype.size(); i++)
    {
        writer.Int(s.table_datatype[i]);
    }
    writer.EndArray();
    writer.Key("BlockList");
    writer.StartArray();
    // for (int i = 0; i < s.block_info_list.Size(); i++)
    // {
    //     writer.StartObject();
    //     writer.Key("BlockID");
    //     writer.Int(blockcount_);
    //     blockvec.push_back(blockcount_);
    //     blockcount_++;
    //     // cout << s.block_info_list[i]["SEQ ID"].GetInt() << endl;
    //     writer.Key("Offset");
    //     writer.Int64(s.block_info_list[i]["Offset"].GetInt64());
    //     // writer.Int64(testoffset[i]);
    //     writer.Key("Length");
    //     writer.Int(s.block_info_list[i]["Length"].GetInt());
    //     writer.EndObject();
    // }
    for (int i = 0; i < s.block_info_list.Size(); i++)
    {
        writer.StartObject();
        writer.Key("BlockID");
        writer.Int(blockcount_);
        blockvec.push_back(blockcount_);
        blockcount_++;
        // blockvec.push_back(blockcount_);
        // cout << blockcount_ << endl;
        // blockidnum++;
        // cout << s.block_info_list[i]["SEQ ID"].GetInt() << endl;
        if(i > 0){
        if(s.block_info_list[i-1]["Offset"].GetInt64()+s.block_info_list[i-1]["Length"].GetInt() != s.block_info_list[i]["Offset"].GetInt64()){
            writer.Key("Offset");
            writer.Int64(s.block_info_list[i]["Offset"].GetInt64());
        }
        }else{
            writer.Key("Offset");
            writer.Int64(s.block_info_list[i]["Offset"].GetInt64());
        }
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