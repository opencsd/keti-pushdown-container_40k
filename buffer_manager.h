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
#include <map>
#include <cstdlib>
#include <iomanip>
#include <sstream>

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/prettywriter.h" 

using namespace std;
using namespace rapidjson;

#define PORT_BUF 8888
#define BUFF_SIZE 4096

template <typename T>
class WorkQueue;
//struct Block_Buffer;

class Scheduler{

    public:
        Scheduler() {init_scheduler();}
        vector<int> blockvec;
    struct Snippet{
        int work_id;
        string sstfilename;
        Value& block_info_list;
        vector<string> table_col;
        Value& table_filter;
        vector<int> table_offset;
        vector<int> table_offlen;
        vector<int> table_datatype;

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
        void csdworkdec(string csdname);
        void Serialize(Writer<StringBuffer>& writer, Snippet& s, string csd_ip, string tablename, string CSDName);
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
    vector<tuple<int, int, int, bool>> block_id_list; 
    // block_id : block start offset : block size : is full block
    // block start offset이 -1이면 데이터 없음
    vector<int> row_offset; 
    int rows;
    int data[BUFF_SIZE]; //4k
    int length;
    string csd_name;

    BlockResult(const char* json_, char* data_){

        Document document;
        document.Parse(json_);
        
        work_id = document["Work ID"].GetInt();

        Value &blockList = document["Block ID"];
        int blockSize = blockList.Size();
        for(int i = 0; i<blockSize; i++){
            int block_id_ = blockList[i][0].GetInt();
            int block_offset_ = blockList[i][1].GetInt();
            int block_size_ = blockList[i][2].GetInt();
            bool is_full_block_ = blockList[i][3].GetBool();
            block_id_list.push_back(make_tuple(block_id_,block_offset_,block_size_,is_full_block_));
        }

        rows = document["nrows"].GetInt();

        Value &row_offset_ = document["Row Offset"];
        int row_offset_size = row_offset_.Size();
        for(int i = 0; i<row_offset_size; i++){
            row_offset.push_back(row_offset_[i].GetInt());
        }

        length = document["Length"].GetInt();
        //data?
        for(int i=0; i<BUFF_SIZE; i++){
            data[i] = data_[i];
        }

        csd_name = document["CSD Name"].GetString();
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
};

struct Block_Buffer {
    int work_id;
    int nrows;
    int length;
    char rowData[BUFF_SIZE];
    vector<int> rowoffset;
};

struct Work_Buffer {
    int WorkID;
    vector<int> need_block_list;
    vector<int> merged_block_list;
    Block_Buffer merging_block_buffer;
    WorkQueue<Block_Buffer> merged_block_buffer;
    bool is_done;
    condition_variable work_available;
    mutex work_mutex;

    Work_Buffer(int workid,vector<int> blocklist, Block_Buffer blockBuffer){
      blockBuffer.length = 0;
      blockBuffer.nrows = 0;
      blockBuffer.work_id = workid;
      blockBuffer.rowoffset.clear();

      WorkID = workid;
      need_block_list.assign(blocklist.begin(), blocklist.end());
      merged_block_list.clear();
      merging_block_buffer = blockBuffer;
	  is_done = false;
    }

    // void push_work(Block_Buffer item){
    //   unique_lock<mutex> lock(work_mutex);

    //   bool was_empty = merged_block_buffer.empty();
    //   merged_block_buffer.push(item);
    //   cout << "unlock" << endl;
    //   lock.unlock();

    //   if (was_empty){
    //     work_available.notify_one();
    //   }    
    // }

    // Block_Buffer wait_and_pop(){
    //   unique_lock<mutex> lock(work_mutex);
    //   if (merged_block_buffer.empty()){
    //       cout << "lock" << endl;
    //   }
    //   while (merged_block_buffer.empty()){
    //     work_available.wait(lock);
    //   }

    //   Block_Buffer tmp = merged_block_buffer.front();
    //   merged_block_buffer.pop();
    //   return tmp;
    // }
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
    // 6번 7번의 데이터를 변환하면댐 인덱스로는 5,6
};



class BufferManager{	
public:
    BufferManager(Scheduler &scheduler){
      InitBufferManager(scheduler);
    }

    void BlockBufferInput();
    void BufferRunning(Scheduler &scheduler);
    void MergeBlock(BlockResult result, Scheduler &scheduler);
    int InitBufferManager(Scheduler &scheduler);
    int join();
    int GetData(Block_Buffer &dest);
    int SetWork(int work_id, vector<int> Block_list);

    // thread BlockBufferInputThread(){ 
    //     return thread([=] {BlockBufferInput();});
    // }

    // thread BufferRunningThread(Scheduler &scheduler){ 
    //     return thread([=] {BufferRunning(scheduler);});
    // }

private:
    unordered_map<int,struct Work_Buffer*> m_BufferManager;
    thread BufferManager_Input_Thread;
    thread BufferManager_Thread;
	WorkQueue<BlockResult> BufferQueue;
};




