#include <unistd.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>

#include <vector>
#include <unordered_map>
#include <iostream>
#include <thread>
#include <atomic>

#include <boost/thread.hpp>
#include <boost/asio.hpp>

#include "rapidjson/document.h"

#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/prettywriter.h" 

//#include "TableManager.h"
// #include "CSDScheduler.h"
#include "keti_type.h"
#include "buffer_manager.h"

#define PORT 8080
#define LBA2PBAPORT 8081
#define MAXLINE 256

using namespace rapidjson;
using namespace std;

void accept_connection(int server_fd);
//int my_LBA2PBA(char* req,char* res);
int my_LBA2PBA(std::string &req_json,std::string &res_json);

TableManager tblManager;
Scheduler csdscheduler;
BufferManager bufma(csdscheduler,tblManager);
// sumtest sumt;

atomic<int> WorkID;
unordered_map<string,int> fullquerymap;
int totalrow = 0;
int key;

int main(int argc, char const *argv[])
{
	//init table manager	
	tblManager.init_TableManager();
	//init WorkID
	WorkID=0;
	
    int server_fd;
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);
    // char *hello = "Hello from server";
       
    // Creating socket file descriptor
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0)
    {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }
       
    // Forcefully attaching socket to the port 8080
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT,
                                                  &opt, sizeof(opt)))
    {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons( PORT );
       
    // Forcefully attaching socket to the port 8080
    if (bind(server_fd, (struct sockaddr *)&address, 
                                 sizeof(address))<0)
    {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }
    if (listen(server_fd, NCONNECTION) < 0)
    {
        perror("listen");
        exit(EXIT_FAILURE);
    }
	
	thread(accept_connection, server_fd).detach();

	while (1);
	
	close(server_fd);
	bufma.Join();

    //send(new_socket , test_buf , 1024 , 0 );
    //printf("Hello message sent\n");
	
    return 0;
}
void accept_connection(int server_fd){
	while (1) {
		int new_socket;
		struct sockaddr_in address;
		int addrlen = sizeof(address);
		char buffer[4096] = {0};

		if ((new_socket = accept(server_fd, (struct sockaddr *)&address, 
			(socklen_t*)&addrlen))<0)
		{
			perror("accept");
			exit(EXIT_FAILURE);
		}
		
		read( new_socket , buffer, 4096);
		std::cout << buffer << std::endl;
				
		//parse json	
		Document document;
		document.Parse(buffer);
		
		Value &type = document["type"];
		string full_query;
		if(document.HasMember("full_query")){
			full_query = document["full_query"].GetString();
			if(fullquerymap.find(full_query) == fullquerymap.end()){
				bufma.InitQuery(full_query);
				fullquerymap.insert(make_pair(full_query,1));
			}
		}
		boost::thread_group tg;

		// key = document["key"].GetInt();
		switch(type.GetInt()){
			case KETI_WORK_TYPE::SCAN :{
				std::cout << "Do SCAN Pushdown" << std::endl;
				
				std::string req_json;
				std::string res_json;
				int work_id = WorkID.load();
				WorkID++;

				Value &table_name = document["table_name"];
				//gen req_json
				if( tblManager.generate_req_json(table_name.GetString(),req_json) == -1 ){
					int ret = -1;
					send(new_socket,&ret,sizeof(ret),0);
					break;
				}
				// std::cout << req_json << std::endl;
				
				//do LBA2PBA
				my_LBA2PBA(req_json,res_json);
				// std::cout << res_json << std::endl;
				Document reqdoc;
				reqdoc.Parse(req_json.c_str());
				vector<string> sstfilename;
				for (int i = 0; i < reqdoc["REQ"]["Chunk List"].Size(); i ++){
					sstfilename.push_back(reqdoc["REQ"]["Chunk List"][i]["filename"].GetString());
				}
				
				// cout << sstfilename << endl;
				Document blockdoc;
				blockdoc.Parse(res_json.c_str());
				//string printres(res_json);
				// Value &Blcokinfo = blockdoc["RES"]["Chunk List"][0][sstfilename[0].c_str()];
				// Value &filter = document["Extra"];
				
				//get table schema
				vector<TableManager::ColumnSchema> schema;
				tblManager.get_table_schema(table_name.GetString(),schema);
				vector<int> offset;
				vector<int> offlen;
				vector<int> datatype;
				vector<string> colname;
				string comma = ".";
				vector<TableManager::ColumnSchema>::iterator itor = schema.begin();
				for(; itor != schema.end(); itor++){
					// std::cout << "column_name : " << (*itor).column_name << " " << (*itor).type << " " << (*itor).length << " " << (*itor).offset << std::endl;
					offset.push_back((*itor).offset);
					offlen.push_back((*itor).length);
					datatype.push_back((*itor).type);
					colname.push_back(table_name.GetString() + comma + (*itor).column_name);
					// cout << table_name.GetString() << endl;
				}
				// cout << endl;
				// cout << "Table Manager Tables" << endl;
				// tblManager.print_TableManager();
				// cout << endl;
				// sleep(0.3);
				// Value &Blcokinfo = blockdoc["RES"]["Chunk List"];

				Value &Blcokinfo = blockdoc["RES"]["Chunk List"];
				Value &filter = document["Extra"]["filter"];
				int count = 0;
				// cout << Blcokinfo.Size() << endl;
				for(int i = 0; i < Blcokinfo.Size(); i ++){
					// cout << sstfilename[i].c_str() << endl;
					// cout << Blcokinfo[i][sstfilename[i].c_str()].Size() << endl;
					csdscheduler.threadblocknum.push_back(count);
					// cout << count << endl;
					for (int j = 0; j < Blcokinfo[i][sstfilename[i].c_str()].Size(); j++){
						csdscheduler.blockvec.push_back(count);
						// cout << count << endl;
						count++;
					}
				}
				bufma.SetWork(full_query,work_id,table_name.GetString(),csdscheduler.savedfilter,csdscheduler.blockvec);

				csdscheduler.snippetdata.work_id = work_id;
				csdscheduler.snippetdata.table_offset = offset;
				csdscheduler.snippetdata.table_offlen = offlen;
				csdscheduler.snippetdata.table_filter = filter;
				csdscheduler.snippetdata.table_datatype = datatype;
				csdscheduler.snippetdata.table_col = colname;
				csdscheduler.snippetdata.sstfilelist = sstfilename;
				csdscheduler.snippetdata.block_info_list = Blcokinfo;
				csdscheduler.snippetdata.tablename = table_name.GetString();

				for(int i = 0; i < sstfilename.size(); i++){
					// csdscheduler.sched(work_id,Blcokinfo[i][sstfilename[i].c_str()],offset,offlen,datatype,colname,filter,sstfilename[i],table_name.GetString(),res_json);
					tg.create_thread(boost::bind(&Scheduler::sched,&csdscheduler,i));
				}

				tg.join_all();
				// csdscheduler.sched(WorkID,Blcokinfo,offset,offlen,datatype,colname,filter,sstfilename[0],table_name.GetString());
				// for (int i = 0; i < csdscheduler.blockvec.size(); i++){
				// 	cout << csdscheduler.blockvec[i] << endl;
				// }
				// bufma.SetWork(full_query,work_id,table_name.GetString(),csdscheduler.savedfilter,csdscheduler.blockvec);
				// csdscheduler.blockcount_ = 0;
				csdscheduler.savedfilter.clear();
				csdscheduler.threadblocknum.clear();
				csdscheduler.blockvec.clear();
				// cout << table_name.GetString() << endl;
				//after sched
				send(new_socket,&work_id,sizeof(work_id),0);
				std::cout << "WorkID : " << work_id << std::endl;

				break;
				
				//get table schema
				// vector<TableManager::ColumnSchema> schema;
				// tblManager.get_table_schema(table_name.GetString(),schema);
				
				// vector<TableManager::ColumnSchema>::iterator itor = schema.begin();
				// // for(; itor != schema.end(); itor++){
				// // 	std::cout << "column_name : " << (*itor).column_name << " " << (*itor).type << " " << (*itor).length << " " << (*itor).offset << std::endl;
				// // }

				// //after sched
				// int tmp = WorkID.load();
				// send(new_socket,&tmp,sizeof(tmp),0);
				// // std::cout << "WorkID : " << WorkID << std::endl;
				// WorkID++;

				// break;
			}
			case KETI_WORK_TYPE::SCAN_N_FILTER :{
				std::cout << "Do SCAN_N_FILTER Pushdown" << std::endl;

				int work_id = WorkID.load();
				WorkID++;
				
				std::string req_json;
				std::string res_json;

				Value &table_name = document["table_name"];
				//gen req_json
				
				tblManager.generate_req_json(table_name.GetString(),req_json);
				// std::cout << req_json << std::endl;
				// cout << table_name.GetString() << endl;
				
				//do LBA2PBA
				my_LBA2PBA(req_json,res_json);
				// std::cout << res_json << std::endl;
				Document reqdoc;
				reqdoc.Parse(req_json.c_str());
				vector<string> sstfilename;


				for (int i = 0; i < reqdoc["REQ"]["Chunk List"].Size(); i ++){
					sstfilename.push_back(reqdoc["REQ"]["Chunk List"][i]["filename"].GetString());
					// cout << reqdoc["REQ"]["Chunk List"][i]["filename"].GetString() << endl;
				}
				
				// cout << sstfilename << endl;
				Document blockdoc;
				blockdoc.Parse(res_json.c_str());
				//string printres(res_json);
				// Value &Blcokinfo = blockdoc["RES"]["Chunk List"][0][sstfilename[0].c_str()];
				// Value &filter = document["Extra"];
				// if(filter.HasMember("filter")){
				// 	filter = filter["filter"];
				// }else{
				// 	filter = Value("MemberWhoseValueIsNULL");
				// }
				
				//get table schema
				vector<TableManager::ColumnSchema> schema;
				tblManager.get_table_schema(table_name.GetString(),schema);
				vector<int> offset;
				vector<int> offlen;
				vector<int> datatype;
				vector<string> colname;
				string comma = ".";
				// tblManager.print_TableManager();
				vector<TableManager::ColumnSchema>::iterator itor = schema.begin();
				for(; itor != schema.end(); itor++){
					// std::cout << "column_name : " << (*itor).column_name << " " << (*itor).type << " " << (*itor).length << " " << (*itor).offset << std::endl;
					offset.push_back((*itor).offset);
					offlen.push_back((*itor).length);
					datatype.push_back((*itor).type);
					colname.push_back(table_name.GetString() + comma + (*itor).column_name);
					// cout << table_name.GetString() << endl;
				}
				// cout << endl;
				// cout << "Table Manager Tables" << endl;
				// tblManager.print_TableManager();
				// cout << endl;
				// sleep(0.3);
				// Value &Blcokinfo = blockdoc["RES"]["Chunk List"];

				Value &Blcokinfo = blockdoc["RES"]["Chunk List"];
				Value &filter = document["Extra"]["filter"];
				int count = 0;
				// cout << Blcokinfo.Size() << endl;
				for(int i = 0; i < Blcokinfo.Size(); i ++){
					// cout << sstfilename[i].c_str() << endl;
					// cout << Blcokinfo[i][sstfilename[i].c_str()].Size() << endl;
					csdscheduler.threadblocknum.push_back(count);
					for (int j = 0; j < Blcokinfo[i][sstfilename[i].c_str()].Size(); j++){
						csdscheduler.blockvec.push_back(count);
						// cout << count << endl;
						count++;
					}
				}
					// bufma.SetWork(work_id,csdscheduler.blockvec);
				
				csdscheduler.snippetdata.work_id = work_id;
				csdscheduler.snippetdata.table_offset = offset;
				csdscheduler.snippetdata.table_offlen = offlen;
				csdscheduler.snippetdata.table_filter = filter;
				csdscheduler.snippetdata.table_datatype = datatype;
				csdscheduler.snippetdata.sstfilelist = sstfilename;
				csdscheduler.snippetdata.table_col = colname;
				csdscheduler.snippetdata.block_info_list = Blcokinfo;
				// cout << 123 << endl;
				csdscheduler.snippetdata.tablename = table_name.GetString();
				// cout << 456 << endl;

				for (int i = 0; i < csdscheduler.snippetdata.table_filter.Size(); i++)
				{
					//여기서 먼저 확인을 진행 후 빼야할 필터 절 빼기
					if (!csdscheduler.snippetdata.table_filter[i].HasMember("LV") || !csdscheduler.snippetdata.table_filter[i].HasMember("RV"))
					{
						continue;
					}
					if (!csdscheduler.snippetdata.table_filter[i]["LV"].IsString() || !csdscheduler.snippetdata.table_filter[i]["RV"].IsString())
					{
						continue;
					}
					string LV = csdscheduler.snippetdata.table_filter[i]["LV"].GetString();
					int filteroper = csdscheduler.snippetdata.table_filter[i]["OPERATOR"].GetInt();
					string cmpoper = "=";
					char SubLV = LV[0];
					string RV = csdscheduler.snippetdata.table_filter[i]["RV"].GetString();
					string tmpv;
					char SubRV = RV[0];
					// cout << find(s.table_col.begin(), s.table_col.end(), LV) << endl;
					// for(auto it = s.table_col.begin(); it != s.table_col.end(); it++){
					//     cout << *it << endl;
					// }
					// cout << LV << endl;
					if ((SubLV != '+' && SubRV != '+' && filteroper == 4) && find(csdscheduler.snippetdata.table_col.begin(), csdscheduler.snippetdata.table_col.end(), LV) == csdscheduler.snippetdata.table_col.end())
					{
						//내 테이블이 아닐때의 조건도 추가를 해야할듯?
						if (find(csdscheduler.snippetdata.table_col.begin(), csdscheduler.snippetdata.table_col.end(), RV) != csdscheduler.snippetdata.table_col.end())
						{
							tmpv = RV;
							RV = LV;
							LV = tmpv;
						}
						else
						{
							continue;
						}
						csdscheduler.savedfilter.push_back(make_tuple(LV, cmpoper, RV));
						csdscheduler.passindex.push_back(i);
						csdscheduler.passindex.push_back(i + 1);
					}
					else if ((SubLV != '+' && SubRV != '+' && filteroper == 4) && find(csdscheduler.snippetdata.table_col.begin(), csdscheduler.snippetdata.table_col.end(), LV) != csdscheduler.snippetdata.table_col.end())
					{
						// cout << "LV : " << LV << endl;
						if (find(csdscheduler.snippetdata.table_col.begin(), csdscheduler.snippetdata.table_col.end(), RV) == csdscheduler.snippetdata.table_col.end())
						{
							csdscheduler.savedfilter.push_back(make_tuple(LV, cmpoper, RV));
							csdscheduler.passindex.push_back(i);
							csdscheduler.passindex.push_back(i + 1);
						}
					}
				}
				bufma.SetWork(full_query,work_id,table_name.GetString(),csdscheduler.savedfilter,csdscheduler.blockvec);
				// cout << "Sstfile size : " << sstfilename.size() << endl;
				for(int i = 0; i < sstfilename.size(); i++){
					// cout << "mycount : " << i << endl;
					// csdscheduler.sched(work_id,Blcokinfo[i][sstfilename[i].c_str()],offset,offlen,datatype,colname,filter,sstfilename[i],table_name.GetString(),res_json);
					tg.create_thread(boost::bind(&Scheduler::sched,&csdscheduler,i));
				}
				// cout << "check" << endl;
				tg.join_all();
				// csdscheduler.sched(WorkID,Blcokinfo,offset,offlen,datatype,colname,filter,sstfilename[0],table_name.GetString());
				// for (int i = 0; i < csdscheduler.blockvec.size(); i++){
				// 	cout << csdscheduler.blockvec[i] << endl;
				// }
				// bufma.SetWork(full_query,work_id,table_name.GetString(),csdscheduler.savedfilter,csdscheduler.blockvec);

				csdscheduler.savedfilter.clear();
				csdscheduler.passindex.clear();
				csdscheduler.threadblocknum.clear();
				csdscheduler.blockvec.clear();
				// cout << table_name.GetString() << endl;
				//after sched
				send(new_socket,&work_id,sizeof(work_id),0);
				std::cout << "WorkID : " << work_id << std::endl;

				break;
			}
			case KETI_WORK_TYPE::REQ_SCANED_BLOCK : {
				std::cout << "Return Merged data" << std::endl;
				
				//check workid
				Value &work_id = document["work_id"];
				std::cout << "Scanned data requested by WorkID : " << work_id.GetInt() << std::endl;
				Block_Buffer Bbuffer;
				memset(&Bbuffer,0,sizeof(Bbuffer));
				Bbuffer.work_id = work_id.GetInt();
				Bbuffer.nrows = -1;
				bufma.GetData(Bbuffer);
				// cout << "Buffer n_rows : " << Bbuffer.nrows << endl;
				// cout << "Buffer length : " << Bbuffer.length << endl;
				// totalrow += Bbuffer.nrows;
				// cout << totalrow << endl;
				send(new_socket,&Bbuffer.nrows,sizeof(Bbuffer.nrows),0);
				//for(int i=0;i<Bbuffer.nrows;i++){
				//	send(new_socket,&(Bbuffer.rowoffset[i]),sizeof(Bbuffer.rowoffset[i]),0);
				//}
				send(new_socket,&(Bbuffer.length),sizeof(Bbuffer.length),0);
				// cout << "send to handler : 11" << endl;
				// for (int i =0; i < Bbuffer.length; i++){
				// 	printf("%02X",(u_char)Bbuffer.rowData[i]);
				// }
				// cout <<"-----------------------------------------------------------------------------------------"<< endl;
				send(new_socket,Bbuffer.data,Bbuffer.length,0);
				// 	cout << "send to handler : 22" << endl;
				// for (int i =0; i < Bbuffer.length; i++){
				// 	printf("%02X",(u_char)Bbuffer.rowData[i]);
				// }
				// cout <<"------------------------------------------------------"<< endl;
				// sumt.sum1(Bbuffer);
				break;
			}
			default: {
				break;
			}
		}
		close(new_socket);
		
	}
}

int my_LBA2PBA(std::string &req_json,std::string &res_json){
	int sock = 0, valread;
    struct sockaddr_in serv_addr;
    char buffer[BUFF_SIZE] = {0};
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        printf("\n Socket creation error \n");
        return -1;
    }
   
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(LBA2PBAPORT);
       
    // Convert IPv4 and IPv6 addresses from text to binary form
    if(inet_pton(AF_INET, "10.0.5.119", &serv_addr.sin_addr)<=0) //csd 정보를 통해 ip 입력(std::string 타입)
    {
        printf("\nInvalid address/ Address not supported \n");
        return -1;
    }
   
    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        printf("\nConnection Failed %s\n",strerror(errno));
		//sql_print_information("connect error %s", strerror(errno));
        return -1;
    }
	
	//send json
	size_t len = strlen(req_json.c_str());
	send(sock,&len,sizeof(len),0);
	send(sock,req_json.c_str(),strlen(req_json.c_str()),0);

	//read(sock,res_json,BUFF_SIZE);

	size_t length;
	read( sock , &length, sizeof(length));

	int numread;
	while(1) {
		if ((numread = read( sock , buffer, BUFF_SIZE - 1)) == -1) {
			perror("read");
			exit(1);
		}
		length -= numread;
	    buffer[numread] = '\0';
		res_json += buffer;

	    if (length == 0)
			break;
	}

	::close(sock);

	return 0;
}

