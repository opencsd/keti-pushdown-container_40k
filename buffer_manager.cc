#include "buffer_manager.h"

void sumtest::sum1(Block_Buffer bbuf){
    char testbuf[7];
    int brownum = 0;
    for(int i =0; i < bbuf.nrows; i++){
        brownum = bbuf.row_offset[i];
        char *iter = bbuf.data + brownum;
        memcpy(testbuf,iter+25,7);
        float a = decimalch(testbuf);
        memcpy(testbuf,iter+32,7);
        float b = decimalch(testbuf);
        //cout << " a : " << a << " b : " << b << endl;
        data = a * b + data;
        
    }
    // cout << "data is :" << endl;
    // printf("%f",data);mergeResult.merged_block_id_list[i].first
}

float sumtest::decimalch(char b[]){
    char num[4];
    int *tempbuf;
    int ab;
    float cd;
    float ret;
    memset(num,0,4);
    for(int i = 0; i < 4; i++){
        num[i] = b[5-i];
    }
    tempbuf = (int*)num;
    ret = tempbuf[0];
    //cout << ret << endl;
    memset(num,0,4);
    num[0] = b[6];
    tempbuf = (int*)num;
    ab = tempbuf[0];
    //cout << ab << endl;
    cd = (float)ab/100;
    //cout << cd << endl;
    return ret + cd;
}

int GetColumnValue(TableManager &tblManager, string col_name, string table_name, int &col_offset, int &col_length, char* rowdata){

    vector<TableManager::ColumnSchema> schema;
    int resp = tblManager.get_table_schema(table_name, schema);
    string col_name_;
    bool isvarchar = false;
    int coltype;
    for(int j = 0; j < schema.size(); j++){
        if (schema[j].type == 15){
            //varchar일 경우 내 길이 구하기 + 뒤에값에 영향주기
            col_offset = schema[j].offset + 1;
            char varcharlenbuf[4];
            memset(varcharlenbuf,0,4);
            varcharlenbuf[3] = rowdata[col_offset];
            int varcharlen = *((int*)varcharlenbuf);
            col_length = varcharlen;
            isvarchar = true;
        }else if(isvarchar){
            col_offset = col_offset + col_length;
            col_length = schema[j].length;
        }else{
            col_offset = schema[j].offset;
            col_length = schema[j].length;
        }
        col_name_ = table_name + '.' + schema[j].column_name;
        if(col_name_ == col_name){
            // col_offset = schema[j].offset;
            // col_length = schema[j].length;
            //schema[j].type;
            coltype = schema[j].type;
            return coltype;
        }
    }
    // return schema[0].type
    //varchar 고려 전
}

int BufferManager::InitBufferManager(Scheduler &scheduler, TableManager &tblManager){
    BufferManager_Input_Thread = thread([&](){BufferManager::BlockBufferInput();});
    BufferManager_Thread = thread([&](){BufferManager::BufferRunning(scheduler, tblManager);});

    return 0;
}

int BufferManager::Join(){
    BufferManager_Input_Thread.join();
    BufferManager_Thread.join();
    return 0;
}

void BufferManager::BlockBufferInput(){

    int server_fd, client_fd;
	int opt = 1;
	struct sockaddr_in serv_addr, client_addr;
	socklen_t addrlen = sizeof(client_addr);
    static char cMsg[] = "ok";

	if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0){
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

	if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))){
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }
	
	memset(&serv_addr, 0, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_addr.s_addr = INADDR_ANY;
	serv_addr.sin_port = htons(PORT_BUF); // port
 
	if (bind(server_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0){
		perror("bind");
		exit(EXIT_FAILURE);
	} 

	if (listen(server_fd, NCONNECTION) < 0){
		perror("listen");
		exit(EXIT_FAILURE);
	}

	while(1){
		if ((client_fd = accept(server_fd, (struct sockaddr*)&client_addr, (socklen_t*)&addrlen)) < 0){
			perror("accept");
        	exit(EXIT_FAILURE);
		}

		std::string json = "";//크기?
		// char buffer[BUFF_SIZE] = {0};
        int njson;
		size_t ljson;

		recv( client_fd , &ljson, sizeof(ljson), 0);

        char buffer[ljson] = {0};
		
		while(1) {
			if ((njson = recv(client_fd, buffer, BUFF_SIZE-1, 0)) == -1) {
				perror("read");
				exit(1);
			}
			ljson -= njson;
		    buffer[njson] = '\0';
			json += buffer;

		    if (ljson == 0)
				break;
		}
		
        send(client_fd, cMsg, strlen(cMsg), 0);

		char data[BUFF_SIZE];//크기?
        char* dataiter = data;
		memset(data, 0, BUFF_SIZE);
        int ndata = 0;
        int totallen = 0;
        size_t ldata = 0;
        recv(client_fd , &ldata, sizeof(ldata),0);
        totallen = ldata;

        // cout << "totallen: " << totallen << endl;

		while(1) {
			if ((ndata = recv( client_fd , dataiter, ldata,0)) == -1) {
				perror("read");
				exit(1);
			}
            dataiter = dataiter+ndata;
			ldata -= ndata;

		    if (ldata == 0)
				break;
		}

        send(client_fd, cMsg, strlen(cMsg), 0);

        // cout << "## BufferQueue.push_work(BlockResult(json.c_str(), data)) ##" << endl;
		BufferQueue.push_work(BlockResult(json.c_str(), data));		
        
        close(client_fd);		
	}   
	close(server_fd);
}

void BufferManager::BufferRunning(Scheduler &scheduler, TableManager &tblManager){
    while (1){
        BlockResult blockResult = BufferQueue.wait_and_pop();
        // cout << "result csdname: " << blockResult.csd_name << endl;
        // cout << "result rows: " << blockResult.rows << endl;
        // cout << "result length: " << blockResult.length << endl;

        if(m_WorkIDManager.find(blockResult.work_id)==m_WorkIDManager.end()){
            cout << "There isn't WORKID [" << blockResult.work_id << "]" << endl;
            break;
        }

        MergeBlock(blockResult, scheduler, tblManager);
    }
}

void BufferManager::MergeBlock(BlockResult result, Scheduler &scheduler, TableManager &tblManager){
    string q = m_WorkIDManager[result.work_id];
    int w = result.work_id;

    Work_Buffer* myWorkBuffer = m_BufferManager[q]->work_buffer_list[w];

    //작업 종료된 id의 데이터인지 확인
    if(myWorkBuffer->is_done){
        return;
    }

    //병합 완료된 블록 확인
    vector<pair<int,list<int>>>::iterator iter1;
    for (iter1 = result.block_id_list.begin(); iter1 != result.block_id_list.end(); iter1++) {
        int is_full = (*iter1).first;
        if(is_full){
            list<int>::iterator iter2;
            for(iter2 = (*iter1).second.begin(); iter2 != (*iter1).second.end(); iter2++){
                if(*iter2 < result.last_valid_block_id){
                    myWorkBuffer->need_block_list.erase(*iter2);
                }else{
                    cout << "-------*iter2 > result.last_valid_block_id-----------" << endl;
                }
            }
        }
        // cout << endl;
        // cout << "size: " << myWorkBuffer->need_block_list.size() << endl;
    }

    cout << "(total/result/total-result)" << "(" << myWorkBuffer->left_block_count <<"/" << result.result_block_count << "/" << myWorkBuffer->left_block_count-result.result_block_count << ")" << endl;
    scheduler.csdworkdec(result.csd_name, result.result_block_count);
    myWorkBuffer->left_block_count -= result.result_block_count;

    //wotk_type에 따라 작업 수행
    switch (myWorkBuffer->work_type){
        case Buffer_Work_Type::JoinX:
        {    
            // cout << "#work_type : JoinX" << endl;

            // cout << "+++++ block save +++++" << endl;
            myWorkBuffer->merging_block_buffer.nrows = result.rows;
            myWorkBuffer->merging_block_buffer.length = result.length;
            memcpy(myWorkBuffer->merging_block_buffer.data,result.data,result.length);
            myWorkBuffer->merging_block_buffer.row_offset.assign(result.row_offset.begin(), result.row_offset.end()); 
            myWorkBuffer->row_all += result.rows;
            // cout << "row_all: " << myWorkBuffer->row_all << endl;
            // cout << "A" << endl;
            myWorkBuffer->PushWork();

            break;
        }

        case Buffer_Work_Type::JoinO_HasMapX_MakeMapO :
        {
            // cout << "#work_type : JoinO_HasMapX_MakeMapO" << endl;

            // cout << "+++++ row make map +++++" << endl;
            //type int
            for(int i=0; i<result.rows; i++){
                int row_offset = result.row_offset[i];
                // make map
                for (int m = 0; m < myWorkBuffer->make_map_col.size(); m++) {
                    string my_col = myWorkBuffer->make_map_col[m];
                    int col_offset, col_length; // 받아올 변수
                    int coltype = GetColumnValue(tblManager, my_col, myWorkBuffer->table_name, col_offset, col_length, result.data+row_offset);
                    //cout << "#col_offset = " << col_offset << " | col_length = " << col_length << endl;

                    char tempbuf[col_length];
                    memcpy(tempbuf, result.data+row_offset+col_offset, col_length);
                    int my_value = 0;
                    if (coltype == 8){
                        my_value = *((int64_t *)tempbuf);
                    }else{
                        my_value = *((int *)tempbuf);
                    }
                    // int my_value = *((int *)tempbuf);
                    // cout << "-col_name: " << my_col << " | my_value : " << my_value << endl;

                    m_BufferManager[q]->col_map[my_col].insert(my_value);
                }   
            }

            // cout << "+++++ block save +++++" << endl;
            myWorkBuffer->merging_block_buffer.nrows = result.rows;
            myWorkBuffer->merging_block_buffer.length = result.length;
            memcpy(myWorkBuffer->merging_block_buffer.data,result.data,result.length);
            myWorkBuffer->merging_block_buffer.row_offset.assign(result.row_offset.begin(), result.row_offset.end());
                
            //map 출력
            //    for (int m = 0; m < myWorkBuffer->make_map_col.size(); m++) {
            //         string my_col = myWorkBuffer->make_map_col[m];
            //         unordered_map<int,int> map;
            //         map = m_BufferManager[q]->col_map[my_col];
            //         cout<< my_col << " map : [ ";
            //         for(auto const&i:map){
            //             cout << i.first << " "; 
            //         }
            //         cout << " ]" << endl;
            //    }
            myWorkBuffer->PushWork();

            break;
        }

        case Buffer_Work_Type::JoinO_HasMapO_MakeMapX :
        {
            // cout << "#work_type : JoinO_HasMapO_MakeMapX" << endl;

            int row_len = 0;
            vector<int> temp_offset;
            temp_offset.assign(result.row_offset.begin(), result.row_offset.end());
            temp_offset.push_back(result.length);

            // cout << "+++++ row join column +++++" << endl;
            for(int i=0; i<result.rows; i++){
                int row_offset = result.row_offset[i];
                bool passed = true;
                // column join
                for (int j = 0; j < myWorkBuffer->join_col.size(); j++) {
                    string my_col = myWorkBuffer->join_col[j].first;
                    string opp_col = myWorkBuffer->join_col[j].second;
                    int col_offset, col_length; // 받아올 변수
                    int coltype = GetColumnValue(tblManager, my_col, myWorkBuffer->table_name, col_offset, col_length, result.data+row_offset);
                    // cout << "#col_offset = " << col_offset << " | col_length = " << col_length << endl;
                    
                    char tempbuf[col_length];
                    memcpy(tempbuf,result.data+row_offset+col_offset,col_length);
                    int my_value = 0;
                    if (coltype == 8){
                        my_value = *((int64_t *)tempbuf);
                    }else{
                        my_value = *((int *)tempbuf);
                    }
                    // int my_value = *((int *)tempbuf);
                    // cout << "-col_name: " << my_col << " | my_value : " << my_value << endl;

                    if(m_BufferManager[q]->col_map[opp_col].find(my_value) == m_BufferManager[q]->col_map[opp_col].end()){
                        passed = false;
                        break;
                    }
                }   

                if(passed){
                    // cout << "+++++ row save +++++" << endl;
                    row_len = temp_offset[i+1] - temp_offset[i];
                    if(myWorkBuffer->merging_block_buffer.length + row_len > BUFF_SIZE){ //row추가시 데이터 크기 넘으면
                        myWorkBuffer->PushWork();
                    }
                    myWorkBuffer->merging_block_buffer.row_offset.push_back(myWorkBuffer->merging_block_buffer.length);
                    myWorkBuffer->merging_block_buffer.nrows += 1;
                    int data_offset = myWorkBuffer->merging_block_buffer.length;
                    memcpy(myWorkBuffer->merging_block_buffer.data + data_offset, result.data + temp_offset[i], row_len);
                    myWorkBuffer->merging_block_buffer.length += row_len;
                }

            }

            break;    
        }

        case Buffer_Work_Type::JoinO_HasMapO_MakeMapO :
        {
            // cout << "#work_type : JoinO_HasMapO_MakeMapO" << endl;

            int row_len = 0;
            vector<int> temp_offset;
            temp_offset.assign(result.row_offset.begin(), result.row_offset.end());
            temp_offset.push_back(result.length);

            // cout << "+++++ row make map / row join column +++++" << endl;
            for(int i=0; i<result.rows; i++){
                int row_offset = result.row_offset[i];

                // make map
                for (int m = 0; m < myWorkBuffer->make_map_col.size(); m++) {
                    string my_col = myWorkBuffer->make_map_col[m];
                    int col_offset, col_length; // 받아올 변수
                    int coltype = GetColumnValue(tblManager, my_col, myWorkBuffer->table_name, col_offset, col_length, result.data+row_offset);
                    //cout << "#col_offset = " << col_offset << " | col_length = " << col_length << endl;
                    
                    char tempbuf[col_length];
                    memcpy(tempbuf,result.data+row_offset+col_offset,col_length);
                    // int my_value = *((int *)tempbuf);
                    int my_value = 0;
                    if (coltype == 8){
                        my_value = *((int64_t *)tempbuf);
                    }else{
                        my_value = *((int *)tempbuf);
                    }
                    // cout << "-col_name: " << my_col << " | my_value : " << my_value << endl;
                    
                    m_BufferManager[q]->col_map[my_col].insert({my_value,1});
                } 

                bool passed = true;

                // column join
                for (int j = 0; j < myWorkBuffer->join_col.size(); j++) {
                    string my_col = myWorkBuffer->join_col[j].first;
                    string opp_col = myWorkBuffer->join_col[j].second;
                    int col_offset, col_length; // 받아올 변수
                    int coltype = GetColumnValue(tblManager, my_col, myWorkBuffer->table_name, col_offset, col_length, result.data+row_offset);
                    // cout << "#col_offset = " << col_offset << " | col_length = " << col_length << endl;
                    
                    char tempbuf[col_length];
                    memcpy(tempbuf,result.data+row_offset+col_offset,col_length);
                    // int my_value = *((int *)tempbuf);
                    int my_value = 0;
                    if (coltype == 8){
                        my_value = *((int64_t *)tempbuf);
                    }else{
                        my_value = *((int *)tempbuf);
                    }
                    // cout << "-col_name: " << my_col << " | my_value : " << my_value << endl;

                    if(m_BufferManager[q]->col_map[opp_col].find(my_value) == m_BufferManager[q]->col_map[opp_col].end()){
                        passed = false;
                        break;
                    }
                }   
                
                if(passed){
                    // cout << "+++++ save row +++++" << endl;
                    row_len = temp_offset[i+1] - temp_offset[i];
                    if(myWorkBuffer->merging_block_buffer.length + row_len > BUFF_SIZE){ //row추가시 데이터 크기 넘으면
                        myWorkBuffer->PushWork();
                    }
                    myWorkBuffer->merging_block_buffer.row_offset.push_back(myWorkBuffer->merging_block_buffer.length);
                    myWorkBuffer->merging_block_buffer.nrows += 1; 
                    int data_offset = myWorkBuffer->merging_block_buffer.length;
                    memcpy(myWorkBuffer->merging_block_buffer.data + data_offset, result.data + temp_offset[i], row_len);
                    myWorkBuffer->merging_block_buffer.length += row_len;
                }

            }
            break;
        }

    }

    // cout << "size: " << myWorkBuffer->need_block_list.size() << endl;

    //필요한 블록이 다 모였는지 확인
    if((myWorkBuffer->need_block_list.size() == 0) || (myWorkBuffer->left_block_count == 0)){
        cout << "FINISHED " << (myWorkBuffer->need_block_list.size() == 0) << "/" << (myWorkBuffer->left_block_count == 0) << endl;
        
        myWorkBuffer->merging_block_buffer.last_merging_buffer = true;
        myWorkBuffer->is_done = true;  
        
        if(myWorkBuffer->work_type == 2 || myWorkBuffer->work_type == 3){
            myWorkBuffer->PushWork();
        }
            
        cout << "Work [" << myWorkBuffer->work_id << "] Done!!" << endl;               
    }

}

void Work_Buffer::PushWork(){
    // if(work_type == 1 || work_type == 3){
    //     make_map_queue.push_work(merging_block_buffer);
    // }
    return_block_queue.push_work(merging_block_buffer);
    merging_block_buffer.InitBlockBuffer();
}

int BufferManager::GetData(Block_Buffer &dest){
    string q_id = m_WorkIDManager[dest.work_id];
    if(m_WorkIDManager.find(dest.work_id)==m_WorkIDManager.end()){
        cout << "no work_id" << endl;
        return -1;
    }
    else if(m_BufferManager[q_id]->work_buffer_list[dest.work_id]->is_done &&
        m_BufferManager[q_id]->work_buffer_list[dest.work_id]->return_block_queue.is_empty()){
        cout << "done" << endl;
        return -2;
    }
    else{
        Block_Buffer BBuf = m_BufferManager[q_id]->work_buffer_list[dest.work_id]->return_block_queue.wait_and_pop();
        dest.length = BBuf.length;
        dest.nrows = BBuf.nrows;
        memcpy(dest.data, BBuf.data, BBuf.length);
        dest.row_offset.assign(BBuf.row_offset.begin(),BBuf.row_offset.end());

        // cout << "send to handler : " << endl;
        // for (int i =0; i < BBuf.length; i++){
        //     printf("%02X ",(u_char)BBuf.data[i]);
        // }
        // cout << "-------------------------------------------------------"<< endl;

    }
    return 1;
}

// void Work_Buffer::WorkBufferMakeMap(unordered_map<string, unordered_map<int,int>> col_map_, TableManager tblManager){

//     while(1){
//         Block_Buffer blockBuffer = make_map_queue.wait_and_pop();
                
//         int row_len = 0;
//         vector<int> temp_offset;
//         temp_offset.assign( blockBuffer.row_offset.begin(), blockBuffer.row_offset.end() );
//         temp_offset.push_back(blockBuffer.length);

//         for(int i=0; i<blockBuffer.nrows; i++){
//             for (int m = 0; m < make_map_col.size(); m++) {
//                 string my_col = make_map_col[m];
//                 int col_offset, col_length; // 받아올 변수
//                 GetColumnValue(tblManager, my_col, table_name, col_offset, col_length);
//                 int my_value = *((int*)blockBuffer.data[i]+col_offset);
//                 cout << "my_value : " << my_value << endl;
//                 col_map_[my_col].insert({my_value,1});
//             }
//         } 

//         if(blockBuffer.last_merging_buffer){
//             return;
//         }
//     }    
// }

int BufferManager::SetWork(string query_, int work_id_, string table_name_,
            vector<tuple<string,string,string>> join_, vector<int> block_list_){
    cout << "#Set Work Called!" << endl;            
    // cout << "#work_id: " << work_id_ << "| table_name: " << table_name_ 
    // << "need_block size: " << block_list_.size() << endl;
    if(m_BufferManager.find(query_)==m_BufferManager.end()){
        cout << "Initialize first " << endl;
        return -1;
    }

    vector<string> make_map_col;
    vector<pair<string,string>> join_col;
    int work_type = Buffer_Work_Type::JoinX;

    //CHECK WORK_TYPE
    if(join_.size() != 0){
        vector<tuple<string,string,string>>::iterator join_iter;
        for (join_iter = join_.begin(); join_iter != join_.end(); join_iter++){
            string my_col = get<0>(*join_iter);
            string oper = get<1>(*join_iter);
            string opp_col = get<2>(*join_iter);

            cout << "{ " << my_col<< " " << oper << " " << opp_col  << "}" << endl;

            if(m_BufferManager[query_]->col_map.find(opp_col) 
                        == m_BufferManager[query_]->col_map.end()){ //Map X
                cout << "#Map X : " << opp_col << endl;
                if(work_type == Buffer_Work_Type::JoinO_HasMapO_MakeMapX 
                    || work_type == Buffer_Work_Type::JoinO_HasMapO_MakeMapO){
                    work_type = Buffer_Work_Type::JoinO_HasMapO_MakeMapO;
                }else{
                    work_type = Buffer_Work_Type::JoinO_HasMapX_MakeMapO;
                }
                make_map_col.push_back(my_col);
            }else{ //Map O
                cout << "#Map O : " << opp_col << endl;
                if(work_type == Buffer_Work_Type::JoinX 
                    || work_type == Buffer_Work_Type::JoinO_HasMapO_MakeMapX){
                    work_type = Buffer_Work_Type::JoinO_HasMapO_MakeMapX;
                }else{
                    work_type = Buffer_Work_Type::JoinO_HasMapO_MakeMapO;
                }            
                join_col.push_back({my_col,opp_col});
            }
        }
    }

    Work_Buffer* myWorkBuffer = new Work_Buffer(work_id_, table_name_, make_map_col, 
                                                join_col, block_list_,work_type);

    m_BufferManager[query_]->work_buffer_list.insert(pair<int,Work_Buffer*>(work_id_,myWorkBuffer));
    m_WorkIDManager.insert(pair<int,string>(work_id_ ,query_));

    // //MAKE_MAP THREAD  
    // if(myWorkBuffer->work_type == 1 || myWorkBuffer->work_type == 3){
    //     thread Work_Buffer_Get_Col_Thread = thread([&](){myWorkBuffer->WorkBufferMakeMap(m_BufferManager[query_]->col_map,tblManager);});
    //     Work_Buffer_Get_Col_Thread.join();
    // }

    cout << "#my work type : " << work_type << endl;
    
    return 1;
}

int BufferManager::InitQuery(string query){
    cout << "#Init Query Called!" << endl;
    if(!(m_BufferManager.find(query)==m_BufferManager.end())){
        cout << "query_id Duplicate Error" << endl;
        
        return -1;
    }
    else{
        Query_Buffer* queryBuffer = new Query_Buffer(query);
        
        m_BufferManager.insert(pair<string,Query_Buffer*>(query,queryBuffer));

        return 1;
    }
}

