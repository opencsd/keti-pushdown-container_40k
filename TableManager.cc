#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <vector>
#include <unordered_map>
#include <iomanip>
#include "rapidjson/document.h"

#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/prettywriter.h" 

#include "TableManager.h"

using namespace rapidjson;

int TableManager::generate_req_json(std::string tablename,char *dst){
	Document document;
	document.SetObject();	
	
    if (m_TableManager.find(tablename) == m_TableManager.end()){
		std::cout << "Not Present" << std::endl;
        return -1;
	}

	struct Table tbl = m_TableManager[tablename];


	rapidjson::Document::AllocatorType& allocator = document.GetAllocator();

	Value REQ(kObjectType);	

	Value str_val(kObjectType);
	str_val.SetString("Chunk | CSD",static_cast<SizeType>(strlen("Chunk | CSD")),allocator);

	REQ.AddMember("Ordering",str_val,allocator); // set req_json ordering

	Value Chunk_List(kArrayType);

	vector<struct SSTFile>::iterator itor = tbl.SSTList.begin();
	for (; itor != tbl.SSTList.end(); itor++) {
		Value Chunk(kObjectType); // req chunk object
		
		str_val.SetString((*itor).filename.c_str(),static_cast<SizeType>(strlen((*itor).filename.c_str())),allocator);
		Chunk.AddMember("filename",str_val,allocator); // set res_json ordering
		
		Value Offset_List(kArrayType); // req offset list
		vector<struct DataBlockHandle>::iterator itor2 = (*itor).BlockList.begin();
		for (; itor2 != (*itor).BlockList.end(); itor2++) {
			Value Offset(kObjectType); // req offset
			Offset.AddMember("Offset",(*itor2).Offset,allocator);
			Offset.AddMember("Length",(*itor2).Length,allocator);

			Offset_List.PushBack(Offset,allocator);
		}
		Chunk.AddMember("Chunks",Offset_List,allocator);
		Chunk_List.PushBack(Chunk,allocator);
	}
	REQ.AddMember("Chunk List",Chunk_List,allocator); // set req_json ordering
	document.AddMember("REQ",REQ,allocator);
	
	rapidjson::StringBuffer strbuf;
	rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
	document.Accept(writer);

	//std::cout << strbuf.GetString() << std::endl;	
	strcpy(dst,strbuf.GetString());
	return 0;
}

int TableManager::generate_req_json(std::string tablename,std::string &dst){
	Document document;
	document.SetObject();
	
    if (m_TableManager.find(tablename) == m_TableManager.end()){
		std::cout << "Not Present" << std::endl;
        return -1;
	}

	struct Table tbl = m_TableManager[tablename];


	rapidjson::Document::AllocatorType& allocator = document.GetAllocator();

	Value REQ(kObjectType);	

	Value str_val(kObjectType);
	str_val.SetString("Chunk | CSD",static_cast<SizeType>(strlen("Chunk | CSD")),allocator);

	REQ.AddMember("Ordering",str_val,allocator); // set req_json ordering

	Value Chunk_List(kArrayType);

	vector<struct SSTFile>::iterator itor = tbl.SSTList.begin();
	for (; itor != tbl.SSTList.end(); itor++) {
		Value Chunk(kObjectType); // req chunk object
		
		str_val.SetString((*itor).filename.c_str(),static_cast<SizeType>(strlen((*itor).filename.c_str())),allocator);
		Chunk.AddMember("filename",str_val,allocator); // set res_json ordering
		
		Value Offset_List(kArrayType); // req offset list
		vector<struct DataBlockHandle>::iterator itor2 = (*itor).BlockList.begin();
		for (; itor2 != (*itor).BlockList.end(); itor2++) {
			Value Offset(kObjectType); // req offset
			Offset.AddMember("Offset",(*itor2).Offset,allocator);
			Offset.AddMember("Length",(*itor2).Length,allocator);

			Offset_List.PushBack(Offset,allocator);
		}
		Chunk.AddMember("Chunks",Offset_List,allocator);
		Chunk_List.PushBack(Chunk,allocator);
	}
	REQ.AddMember("Chunk List",Chunk_List,allocator); // set req_json ordering
	document.AddMember("REQ",REQ,allocator);
	
	rapidjson::StringBuffer strbuf;
	rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
	document.Accept(writer);

	//std::cout << strbuf.GetString() << std::endl;	
	//strcpy(dst,strbuf.GetString());
	dst = std::string(strbuf.GetString());
	return 0;
}

void TableManager::print_TableManager(){
	unordered_map <string,struct Table> :: iterator it;

	for(it = m_TableManager.begin(); it != m_TableManager.end() ; it++){		
		struct Table tmp = it->second;
		std::cout << "tablename : " << tmp.tablename << std::endl;
		std::cout << "Column name     Type  Length Offset" << std::endl;
		vector<struct ColumnSchema>::iterator itor = tmp.Schema.begin();

		for (; itor != tmp.Schema.end(); itor++) {
			std::cout << left << setw(15) << (*itor).column_name << " " << left<< setw(5) << (*itor).type << " " << left << setw(6) <<(*itor).length << " " << left << setw(5) << (*itor).offset << std::endl;
		}

		vector<struct SSTFile>::iterator itor2 = tmp.SSTList.begin();

		for (; itor2 != tmp.SSTList.end(); itor2++) {
			std::cout << "SST filename : " << (*itor2).filename << std::endl;

			vector<struct DataBlockHandle>::iterator itor3 = (*itor2).BlockList.begin();
			std::cout << " StartOffset-Length" << endl;
			for (; itor3 != (*itor2).BlockList.end(); itor3++) {
				std::cout << "Block Handle : " << (*itor3).Offset << "-" << (*itor3).Length << std::endl;				
			}
		}
	}
}

int TableManager::init_TableManager(){
	//read TableManager.json
	int json_fd;
	string json = "";
	char buf;
		
	//memset(json,0,sizeof(json));
	json_fd = open("TableManager.json",O_RDONLY);
	
	int i=0;
	int res;

	
	while(1){
		res = read(json_fd,&buf,1);
		if(res == 0){
			break;
		}
		json += buf;
	}
	close(json_fd);
	
	// debug code check read json
	std::cout << "json : " << json << std::endl;

	//parse json	
	Document document;
	document.Parse(json.c_str());

	Value &TableList = document["Table List"];
	
	for(i=0;i<TableList.Size();i++){
		Value &TableObject = TableList[i];
		struct Table tbl;

		Value &tablenameObject = TableObject["tablename"];
		tbl.tablename = tablenameObject.GetString();

		// debug code check filename
		//std::cout << "tablename : " << tbl.tablename << std::endl;
		
		Value &SchemaObject = TableObject["Schema"];
		int j;
		for(j=0;j<SchemaObject.Size();j++){
			Value &ColumnObject = SchemaObject[j];
			struct ColumnSchema Column;

			Column.column_name = ColumnObject["column_name"].GetString();
			Column.type = ColumnObject["type"].GetInt();
			Column.length = ColumnObject["length"].GetInt();
			Column.offset = ColumnObject["offset"].GetInt();

			tbl.Schema.push_back(Column);
			//debug code check Schema
			//std::cout << "Schema : " << tbl.Schema[j].column_name << std::endl;
		}

		Value &SSTList = TableObject["SST List"];
		for(j=0;j<SSTList.Size();j++){
			Value &SSTObject = SSTList[j];
			struct SSTFile SSTFile;

			SSTFile.filename = SSTObject["filename"].GetString();

			Value &BlockList = SSTObject["Block List"];
			
			for(int k=0;k<BlockList.Size();k++){
				Value &BlockHandleObject = BlockList[k];
				struct DataBlockHandle DataBlockHandle;

				DataBlockHandle.Offset = BlockHandleObject["Offset"].GetInt();
				DataBlockHandle.Length = BlockHandleObject["Length"].GetInt();

				SSTFile.BlockList.push_back(DataBlockHandle);
			}

			tbl.SSTList.push_back(SSTFile);
			//debug code check sst filename
			//std::cout << "SST : " << SSTObject["filename"].GetString() << std::endl;
		}
		m_TableManager.insert({tbl.tablename,tbl});
	}
	
	return 0;
}

int TableManager::get_table_schema(std::string tablename,vector<struct ColumnSchema> &dst){	
    if (m_TableManager.find(tablename) == m_TableManager.end()){
		std::cout << "Not Present" << std::endl;
        return -1;
	}

	struct Table tbl = m_TableManager[tablename];
	dst = tbl.Schema;
	return 0;
}