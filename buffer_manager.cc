#include "buffer_manager.h"


void sumtest::sum1(Block_Buffer bbuf){
    char testbuf[7];
    int brownum = 0;
    for(int i =0; i < bbuf.nrows; i++){
        brownum = bbuf.rowoffset[i];
        char *iter = bbuf.rowData + brownum;
        memcpy(testbuf,iter+25,7);
        float a = decimalch(testbuf);
        memcpy(testbuf,iter+32,7);
        float b = decimalch(testbuf);
        cout << " a : " << a << " b : " << b << endl;
        data = a * b + data;
        
    }
    cout << "data is :" << endl;
    printf("%f",data);
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
    cout << ret << endl;
    memset(num,0,4);
    num[0] = b[6];
    tempbuf = (int*)num;
    ab = tempbuf[0];
    cout << ab << endl;
    cd = (float)ab/100;
    cout << cd << endl;
    return ret + cd;
}


int BufferManager::InitBufferManager(Scheduler &scheduler){
 
    // BufferManager_Input_Thread = BlockBufferInputThread();
    // BufferManager_Thread = BufferRunningThread(scheduler);

    BufferManager_Input_Thread = thread([&](){BufferManager::BlockBufferInput();});
    BufferManager_Thread = thread([&](){BufferManager::BufferRunning(scheduler);});

    return 0;
}

int BufferManager::join(){
    BufferManager_Input_Thread.join();
    BufferManager_Thread.join();
    return 0;
}

void BufferManager::BlockBufferInput(){
    cout << "[Call Buffer Running]\n";
    cout << "<-----------  Buffer Manager Input Running...  ----------->\n";

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

	if (listen(server_fd, 3) < 0){
		perror("listen");
		exit(EXIT_FAILURE);
	}

	while(1){
		if ((client_fd = accept(server_fd, (struct sockaddr*)&client_addr, (socklen_t*)&addrlen)) < 0){
			perror("accept");
        	exit(EXIT_FAILURE);
		}
        cout << "[Buffer Manager Recieved Merged Data]\n" << endl;

		printf("***json***\n");

		std::string json = "";//크기?
		char buffer[BUFF_SIZE] = {0};
        int njson;
		size_t ljson;
        //read( client_fd , &ljson, sizeof(ljson));
		recv( client_fd , &ljson, sizeof(ljson), 0);
		
		while(1) {
			if ((njson = /*read( client_fd , buffer, BUFF_SIZE - 1)*/recv(client_fd, buffer, BUFF_SIZE-1, 0)) == -1) {
				perror("read");
				exit(1);
			}
			ljson -= njson;
		    buffer[njson] = '\0';
			json += buffer;

		    if (ljson == 0)
				break;
		}
		
		cout << json << endl;
		// cout << "#json len: " << strlen(json) << " | read_size: " << read_size << endl;

        send(client_fd, cMsg, strlen(cMsg), 0);
        cout << "server ok" << endl;

		printf("***data***\n");

		char data[BUFF_SIZE];//크기?
        char* dataiter = data;
		memset(data, 0, BUFF_SIZE);
        int ndata = 0;
        int totallen = 0;
        size_t ldata = 0;
        recv(client_fd , &ldata, sizeof(ldata),0);
        totallen = ldata;
        // cout << "ldata : " << ldata << " ndata : " << ndata << endl;
		while(1) {
            // memset(buffer, 0, BUFF_SIZE);
			if ((ndata = recv( client_fd , dataiter, ldata,0)) == -1) {
				perror("read");
				exit(1);
			}
            dataiter = dataiter+ndata;
			ldata -= ndata;
            // cout << "ldata : " << ldata << " ndata : " << ndata << endl;
		    //buffer[ndata] = '\0';
            // strcat(data, buffer);

		    if (ldata == 0)
				break;
		}
        // for(int i = 0; i < totallen; i ++){
        //     cout << data[i];
        // }
        // cout << endl;

        send(client_fd, cMsg, strlen(cMsg), 0);
        cout << "server ok" << endl;

		//int read_size = read(client_fd , data, BUFF_SIZE);

        // cout << data << endl;

        // string a(data);
        // cout << strlen(a.c_str()) << endl;
		//cout << "#data len: " << strlen(a.c_str()) << " | read_size: " << read_size << endl;

        //  for(int i=0; i<BUFF_SIZE; i++){
        //      cout<<data[i];
        //  }
        //  cout << "\n--" << endl;

		BufferQueue.push_work(BlockResult(json.c_str(), data));		
        
        close(client_fd);		
	}   
	close(server_fd);
}

void BufferManager::BufferRunning(Scheduler &scheduler){
    cout << "<-----------  Buffer Manager Running...  ----------->\n";

    while (1){
        BlockResult blockResult = BufferQueue.wait_and_pop();

		for(int n=0; n < blockResult.length; n++){
			printf("%02X",(u_char)blockResult.data[n]);
		}
        cout << " <------------Buffer Merging Block------------>" << endl;
                
        MergeBlock(blockResult, scheduler);
    }
}

void BufferManager::MergeBlock(BlockResult result,Scheduler &scheduler){
    //printf("~BufferManager::MergeBlock~ # workid: %d, blockid_size: %ld, rows: %d, length: %d, offset_len: %ld\n",result.work_id, result.block_id_list.size(), result.rows, result.length, result.row_offset.size());

    // cout << "-------------------------------------- Block Info----------------------------------"<<endl;
    // cout << "| work id: " << result.work_id << " | length: " << result.length << " | rows: " << result.rows << endl;
    // cout << "| block id list: { block id : block start offset : block size : is full block }" << endl;
    // for(int i=0; i<result.block_id_list.size(); i++){
    //     cout << " -block list[" << i << "] : { " << get<0>(result.block_id_list[i]) << " : " 
    //     <<  get<1>(result.block_id_list[i]) << " : " <<  get<2>(result.block_id_list[i]) 
    //     << " : " <<  get<3>(result.block_id_list[i]) << " }" << endl;
    // }
    // cout << "------------------------------------------------------------------------------------"<<endl;
    // printf("Recieved Data : \n[");
    // for(int n=0; n < result.length; n++){
    //     printf("%02X",(u_char)result.data[n]);
    // }
    // cout << "]" <<endl;
    // cout << "------------------------------------------------------------------------------------"<<endl;
    // cout << "Work ID ["<<result.work_id<<"] merge start"<<endl;
	
    int row_len = 0;
    int key = result.work_id;

    if(m_BufferManager[key]->is_done){
        // cout << "* WorkID [" << result.work_id << "] is done! *" << endl;
        return;
    }

    vector<tuple<int, int, int, bool>>::iterator iter;// block_id : block start offset : block size : is full block
    for (iter = result.block_id_list.begin(); iter != result.block_id_list.end(); iter++) {
		int block_id_ = get<0>(*iter);
        int block_offset_ = get<1>(*iter);
        int block_size_ = get<2>(*iter);
        bool is_full_block_ = get<3>(*iter);
        bool is_need_block = (find(m_BufferManager[key]->need_block_list.begin(), m_BufferManager[key]->need_block_list.end(),
                                 block_id_) != m_BufferManager[key]->need_block_list.end());
        bool is_last_block = (iter == (result.block_id_list.end()-1));
        
        // cout << "Merging Block ID [" << block_id_ << "]..." << endl;
        // cout << "@is_last_block:  " << is_last_block << " @is_need_block_:  " << is_need_block << endl;

        //필요하지 않은 블록일때
        if(!is_need_block){
            // cout << "* Not need Block ID [" << block_id_ << "] *" << endl;
            continue;
        }       
      
        // 필요한 블록이고 블록 사이즈가 0이 아닐때
        if(block_size_ != 0){          
            /*중요*/ vector<int> temp_offset; // 현재 블록에 해당하는 임시 row_offset
            temp_offset.clear();

            int data_start_offset = block_offset_; // data index
            int data_end_offset = block_offset_ + block_size_ - 1;
            int row_start_index = find(result.row_offset.begin(), result.row_offset.end(), data_start_offset) 
                                        - result.row_offset.begin(); // row_offset index
            int next_block_start_index, row_end_index;

            // cout << "# block_size: " << block_size_ << " block_offset: " << block_offset_ << " plus:" << block_size_ + block_offset_ << "length" << result.length << endl;
            if(is_last_block || (block_size_ + block_offset_ == result.length)){
                // cout << "here1" << endl;
                next_block_start_index = result.length;
                row_end_index = result.row_offset.size();

                for(int q = row_start_index; q < result.row_offset.size(); q++){
                    temp_offset.push_back(result.row_offset[q]);
                } 
                temp_offset.push_back(next_block_start_index);
            }
            else{
                // cout << "here2" << endl;
                next_block_start_index = find(result.row_offset.begin(), result.row_offset.end(), data_end_offset+1)
                                        - result.row_offset.begin();
                row_end_index =  next_block_start_index - 1;

                for(int q = row_start_index; q < next_block_start_index; q++){
                    temp_offset.push_back(result.row_offset[q]);
                } 
                temp_offset.push_back(result.row_offset[next_block_start_index]);  
            }
            
            // for(int q = 0; q<temp_offset.size(); q++){
            //     printf("(t1:%d)", temp_offset[q]);
            // } 
            // cout << "--" << endl;                 

            //row 넣기 전 확인
            // row_len = temp_offset[1] - temp_offset[0];
            // if(m_BufferManager[key]->merging_block_buffer.length + row_len > BUFFER_SIZE){
            //     cout << "# Now Merging Block Size : " << m_BufferManager[key]->merging_block_buffer.length << endl;
            //     cout << "** Enqueue Merging Block to Merged Block **" << endl;
            //     m_BufferManager[key]->merged_block_buffer.push_work(m_BufferManager[key]->merging_block_buffer);
            //     m_BufferManager[key]->merging_block_buffer.length = 0;
            //     m_BufferManager[key]->merging_block_buffer.nrows = 0;
            //     m_BufferManager[key]->merging_block_buffer.rowoffset.clear();
            // }

            for(int i=0; i<temp_offset.size()-1; i++){
                row_len = temp_offset[i+1] - temp_offset[i];
                if(m_BufferManager[key]->merging_block_buffer.length + row_len > BUFF_SIZE){ //row추가시 데이터 크기 넘으면
                    // cout << "** Now Merging Block Size : " << m_BufferManager[key]->merging_block_buffer.length
                    // << "-> Enqueue **" << endl;
                    m_BufferManager[key]->merged_block_buffer.push_work(m_BufferManager[key]->merging_block_buffer);
                    m_BufferManager[key]->merging_block_buffer.length = 0;
                    m_BufferManager[key]->merging_block_buffer.nrows = 0;
                    m_BufferManager[key]->merging_block_buffer.rowoffset.clear();
                }

                m_BufferManager[key]->merging_block_buffer.rowoffset.push_back(
                                            m_BufferManager[key]->merging_block_buffer.length); //현재 row 시작 offset
                m_BufferManager[key]->merging_block_buffer.nrows += 1;
                int data_offset = m_BufferManager[key]->merging_block_buffer.length;
                for(int j = temp_offset[i]; j<temp_offset[i+1]; j++){
                    m_BufferManager[key]->merging_block_buffer.rowData[data_offset] =  result.data[j]; // 현재 row데이터 복사
                    data_offset += 1;
                }
                m_BufferManager[key]->merging_block_buffer.length += row_len;// 데이터 길이 = row 전체 길이
            }
        }
        
        //필요한 블록이고 사이즈가 0일때
        else{ 
            // cout << "* block id [" << block_id_ << "] size 0* " << endl;
        }

        //필요한 블록이고 전체 블록일때
        if(is_full_block_){
            // cout << "call scheduler: " << result.csd_name << endl;
            scheduler.csdworkdec(result.csd_name);
            m_BufferManager[key]->merged_block_list.push_back(block_id_);
        }

        // cout << "Need Block List : [" ;
        // for(int i=0; i<m_BufferManager[key]->need_block_list.size(); i++){
        //     cout << m_BufferManager[key]->need_block_list[i] << " ";
        // }
        // cout << "]" << " Length : " << m_BufferManager[key]->need_block_list.size() << endl;

        // cout << "Merged Block ID : [" ;
        // for(int i=0; i<m_BufferManager[key]->merged_block_list.size(); i++){
        //     cout << m_BufferManager[key]->merged_block_list[i] << " ";
        // }
        // cout << "]" << " Length : " << m_BufferManager[key]->merged_block_list.size() << endl;

        // cout << "Merged Block Length : " << m_BufferManager[key]->merged_block_list.size() << endl;
        // cout << "Merging Buffer Info { length: " << m_BufferManager[key]->merging_block_buffer.length 
        // << " rows: " << m_BufferManager[key]->merging_block_buffer.nrows << " }" << endl;

        // cout << "--------------------------------- Merging Buffer Info--------------------------------"<<endl;
        // cout << " | length: " << m_BufferManager[key]->merging_block_buffer.length 
        //      << " | rows: " << m_BufferManager[key]->merging_block_buffer.nrows << " | " << endl;
        // cout << "------------------------------------------------------------------------------------"<<endl;
        // printf("Merging Data : \n[");
        // for(int n=0; n < m_BufferManager[key]->merging_block_buffer.length; n++){
        //     printf("%02X",(u_char)m_BufferManager[key]->merging_block_buffer.rowData[n]);
        // }
        // cout << "]" <<endl;
        // cout << "------------------------------------------------------------------------------------"<<endl;

        //필요한 블록이 다 모였는지 확인
        if(m_BufferManager[key]->need_block_list.size() 
            == m_BufferManager[key]->merged_block_list.size() ){
            // cout << "** Merge Last Block Completed -> Enqueue **" << endl;
            m_BufferManager[key]->merged_block_buffer.push_work(m_BufferManager[key]->merging_block_buffer);
            m_BufferManager[key]->merging_block_buffer.length = 0;
            m_BufferManager[key]->merging_block_buffer.nrows = 0;
            m_BufferManager[key]->merging_block_buffer.rowoffset.clear();
            m_BufferManager[key]->is_done = true;
            // cout << "* work id[" << m_BufferManager[key]->WorkID << "] is done!! *" << endl;            
        }

	}
    
}

int BufferManager::GetData(Block_Buffer &dest){
    if(m_BufferManager.find(dest.work_id)==m_BufferManager.end()){
        cout << "no work_id" << endl;
        return -1;
    }
    else if(m_BufferManager[dest.work_id]->is_done &&
         m_BufferManager[dest.work_id]->merged_block_buffer.is_empty()){
        cout << "done" << endl;
        return -2;
    }
    else{
        Block_Buffer BBuf = m_BufferManager[dest.work_id]->merged_block_buffer.wait_and_pop();
        dest.length = BBuf.length;
        dest.nrows = BBuf.nrows;

        cout << "send to handler : " << endl;
        for (int i =0; i < BBuf.length; i++){
            printf("%02X",(u_char)BBuf.rowData[i]);
        }
        cout << "-------------------------------------------------------"<< endl;
        
        // for(int i=0; i<BUFF_SIZE; i++){
        //     dest.rowData[i] = BBuf.rowData[i];
        // }
        // cout << endl;
        memcpy(dest.rowData,BBuf.rowData,BBuf.length);
        for(int i=0; i < BBuf.rowoffset.size(); i++){
            dest.rowoffset.push_back(BBuf.rowoffset[i]);
        }
    }
    return 1;
}

int BufferManager::SetWork(int work_id, vector<int> block_list){
    if(!(m_BufferManager.find(work_id)==m_BufferManager.end())){
        cout << "work_id Duplicate Error" << endl;
        return -1;
    }

    Block_Buffer blockBuffer;
    Work_Buffer* workBuffer = new Work_Buffer(work_id, block_list, blockBuffer);

    m_BufferManager.insert(pair<int,Work_Buffer*>(work_id,workBuffer));

    return 1;
}

