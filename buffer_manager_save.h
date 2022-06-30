#include <vector>
#include <unordered_map>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <thread>
#include <fstream>
#include <string>
#include <algorithm>
#include <queue>
#include <tuple>
#include <sys/socket.h>
#include <netinet/in.h> 
#include <arpa/inet.h>
#include <mutex>
#include <condition_variable>
#include <string.h>
#include <cstdlib>
#include <iomanip>
#include <sstream>
#include <list>
#include <unordered_set>

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/prettywriter.h" 

#include "TableManager.h"

using namespace std;
using namespace rapidjson;

#define NUM_OF_BLOCKS 15
#define BUFF_SIZE (NUM_OF_BLOCKS * 4096)
#define PORT_BUF 8888
#define NCONNECTION 8

template <typename T>
class WorkQueue;
struct Block_Buffer;
struct Work_Buffer;
struct Query_Buffer;

struct SnippetData{
        int work_id;
        string sstfilename;
        Value block_info_list;
        vector<string> table_col;
        Value table_filter;
        vector<int> table_offset;
        vector<int> table_offlen;
        vector<int> table_datatype;
        vector<string> sstfilelist;
        string tablename;
};

class Scheduler{

    public:
        Scheduler() {init_scheduler();}
        vector<int> blockvec;
        vector<tuple<string,string,string>> savedfilter;
        vector<int> passindex;
        SnippetData snippetdata;
        vector<int> threadblocknum;
    struct Snippet{
        int work_id;
        string sstfilename;
        Value& block_info_list;
        vector<string> table_col;
        Value& table_filter;
        vector<int> table_offset;
        vector<int> table_offlen;
        vector<int> table_datatype;
        vector<string> sstfilelist;
        

        Snippet(int work_id_, string sstfilename_,
            Value& block_info_list_,
            vector<string> table_col_, Value& table_filter_, 
            vector<int> table_offset_, vector<int> table_offlen_,
            vector<int> table_datatype_)
            : work_id(work_id_), sstfilename(sstfilename_),
            block_info_list(block_info_list_),
            table_col(table_col_),
            table_filter(table_filter_),
            table_offset(table_offset_),
            table_offlen(table_offlen_),
            table_datatype(table_datatype_) {};
    };

        typedef enum work_type{
            SCAN = 4,
            SCAN_N_FILTER = 5,
            REQ_SCANED_BLOCK = 6,
            WORK_END = 9
        }KETI_WORK_TYPE;

        void init_scheduler();
        void sched(int workid, Value& blockinfo,vector<int> offset, vector<int> offlen, vector<int> datatype, vector<string> tablecol, Value& filter,string sstfilename, string tablename, string res);
        void sched(int indexdata);
        void csdworkdec(string csdname, int num);
        void Serialize(PrettyWriter<StringBuffer>& writer, Snippet& s, string csd_ip, string tablename, string CSDName);
        void Serialize(Writer<StringBuffer>& writer, Snippet& s, string csd_ip, string tablename, string CSDName, int blockidnum);
        string BestCSD(string sstname, int blockworkcount);
        void sendsnippet(string snippet, string ipaddr);
        // void addcsdip(Writer<StringBuffer>& writer, string s);
        void printcsdblock(){
          for(auto i = csdworkblock_.begin(); i != csdworkblock_.end(); i++){
            pair<std::string, int> k = *i;
            cout << k.first << " " << k.second << endl;
          }
        }
    private:
        unordered_map<string,string> csd_; //csd의 ip정보가 담긴 맵 <csdname, csdip>
        unordered_map<string, int> csdworkblock_; //csd의 block work 수 가 담긴 맵 <csdname, csdworknum>
        vector<string> csdname_;
        unordered_map<string,string> sstcsd_; //csd의 sst파일 보유 내용 <sstname, csdlist>
        vector<string> csdpair_;
        unordered_map<string,string> csdreaplicamap_;
        int blockcount_;
};

struct BlockResult{
    int work_id;
    vector<pair<int,list<int>>> block_id_list; 
    // <is full 10 block, block id list>
    vector<int> row_offset; 
    int rows;
    char data[BUFF_SIZE]; //40k
    int length;
    string csd_name;
    int last_valid_block_id;
    int result_block_count;

    BlockResult(const char* json_, char* data_){

        Document document;
        document.Parse(json_); 
        
        work_id = document["Work ID"].GetInt();

        Value &blockList = document["Block ID"];
        for(int i = 0; i < blockList.Size(); i++){
            int is_full = blockList[i][0].GetInt();
            list<int> b_list;
            for(int j = 0; j < blockList[i][1].Size(); j++){
                b_list.push_back(blockList[i][1][j].GetInt());
            }
            block_id_list.push_back({is_full,b_list});
        }

        rows = document["nrows"].GetInt();

        Value &row_offset_ = document["Row Offset"];
        int row_offset_size = row_offset_.Size();
        for(int i = 0; i<row_offset_size; i++){
            row_offset.push_back(row_offset_[i].GetInt());
        }

        length = document["Length"].GetInt();

        memcpy(data, data_, length);

        csd_name = document["CSD Name"].GetString();
        last_valid_block_id = document["Last valid block id"].GetInt();
        result_block_count = document["Result block count"].GetInt();
    }
};

template <typename T>
class WorkQueue{
  condition_variable work_available;
  mutex work_mutex;
  queue<T> work;

public:
  void push_work(T item){
    unique_lock<mutex> lock(work_mutex);

    bool was_empty = work.empty();
    work.push(item);

    lock.unlock();

    if (was_empty){
      work_available.notify_one();
    }    
  }

  T wait_and_pop(){
    unique_lock<mutex> lock(work_mutex);
    while (work.empty()){
      work_available.wait(lock);
    }

    T tmp = work.front();
	
    work.pop();
    return tmp;
  }

  bool is_empty(){
    return work.empty();
  }

  void qclear(){
    work = queue<T>();
  }

  int get_size(){
    return work.size();
  }
};

struct Block_Buffer {
    int work_id;
    int nrows;
    int length;
    char data[BUFF_SIZE];
    vector<int> row_offset;
    bool last_merging_buffer;
    unordered_map<string,vector<int>> col_offset_list;

    Block_Buffer(){}
    Block_Buffer(int work_id_){
      work_id = work_id_;
      nrows = 0;
      length = 0;
      row_offset.clear();
      last_merging_buffer = false;
    }

    void InitBlockBuffer(){
      nrows = 0;
      length = 0;
      memset(&data, 0, sizeof(BUFF_SIZE));
      row_offset.clear();
      last_merging_buffer = false;
    }
};

/*     
                                Work Type
  ----------------------------------------------------------------------
  | JoinX                  : Join X                                    |
  | JoinO_HasMapX_MakeMapO : Join O / Has_Map X           / Make_Map O |
  | JoinO_HasMapO_MakeMapX : Join O / Has_Map O = Join O  / Make_Map X |
  | JoinO_HasMapO_MakeMapO : Join O / Has_Map O = Join O  / Make_Map O |
  ----------------------------------------------------------------------
*/

enum Buffer_Work_Type{
  JoinX,
  JoinO_HasMapX_MakeMapO,
  JoinO_HasMapO_MakeMapX,
  JoinO_HasMapO_MakeMapO
};

struct Work_Buffer {
    int work_id;
    string table_name;
    vector<string> make_map_col;//map을 만들어야하는 컬럼
    vector<pair<string,string>> join_col;
    //         <my_col,opp_col> -> join 해야하는 컬럼
    unordered_set<int> need_block_list;
    Block_Buffer merging_block_buffer;
    WorkQueue<Block_Buffer> return_block_queue;
    WorkQueue<Block_Buffer> make_map_queue;
    bool is_done;
    int work_type;
    int row_all;
    int total_block_count;

    Work_Buffer(int work_id_, string table_name_, 
                vector<string> make_map_col_,
                vector<pair<string,string>> join_col_,
                vector<int> block_list_, int work_type_){
      work_id = work_id_;
      table_name = table_name_;
      make_map_col.assign(make_map_col_.begin(),make_map_col_.end());
      join_col.assign(join_col_.begin(),join_col_.end());
      need_block_list = unordered_set<int>(block_list_.begin(), block_list_.end());
      merging_block_buffer = Block_Buffer(work_id_);
	    is_done = false;
      work_type = work_type_;
      row_all = 0;
      total_block_count = need_block_list.size();
    }

    void PushWork();
    void WorkBufferMakeMap(unordered_map<string, unordered_map<int,int>> col_map, TableManager tblManager);

};

// template <class T>
// class my_unordered_map :
//     public std::unordered_map<T, int>
// {
// };


// struct my_unordered_map{
//   template <typename T>
//   unordered_map<T,int> m;
// };

// struct my_map{
//   string col_name;
//   template <typename T> unordered_set<T> set;
// };

struct Query_Buffer{
  string query;
  unordered_map<int,Work_Buffer*> work_buffer_list;
  unordered_map<string, unordered_set<int>> col_map;
  //현재 14번 쿼리의 조인 컬럼이 int 형식이라 임시로 int로 지정함
  // unordered_map<string, my_unordered_map> col_map;
  // list<my_map> col_map2;

  Query_Buffer(string query_)
  :query(query_){}
};

class BufferManager{	
public:
    BufferManager(Scheduler &scheduler, TableManager &tblManager){
      InitBufferManager(scheduler, tblManager);
    }

    void BlockBufferInput();
    void BufferRunning(Scheduler &scheduler, TableManager &tblManager);
    void MergeBlock(BlockResult result, Scheduler &scheduler, TableManager &tblManager);
    int InitBufferManager(Scheduler &scheduler, TableManager &tblManager);
    int Join();
    int GetData(Block_Buffer &dest);
    int SetWork(string query, int work_id, string table_name,
        vector<tuple<string,string,string>> join, vector<int> block_list);
    int InitQuery(string query);

private:
    unordered_map<string, struct Query_Buffer*> m_BufferManager;
    unordered_map<int, string> m_WorkIDManager;
    WorkQueue<BlockResult> BufferQueue;
    TableManager m_TableManager;
    thread BufferManager_Input_Thread;
    thread BufferManager_Thread;
};

class sumtest {
    public:
        sumtest(){}
        void sum1(Block_Buffer bbuf);
        float decimalch(char b[]);
        float data = 0;
        int offset[16] = {0,4,8,12,16,23,30,37,44,45,46,49,52,55,82,90};
        int offlen[16] = {4,4,4,4,7,7,7,7,1,1,3,3,3,25,10,45};
    //
    // 6번 7번의 데이터를 변환하면 됨 인덱스로는 5,6
};




