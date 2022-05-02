#include <iostream>
#include "buffer_manager.h"

class sumtest {
    sumtest(){}
    void sum(Block_Buffer bbuf);
    float decimalch(char b[]);
    int data;
    int offset[16] = {0,4,8,12,16,23,30,37,44,45,46,49,52,55,82,90};
    int offlen[16] = {4,4,4,4,7,7,7,7,1,1,3,3,3,25,10,45};
    //
    // 6번 7번의 데이터를 변환하면댐 인덱스로는 5,6
};

void sumtest::sum(Block_Buffer bbuf){
    char testbuf[7];
    int brownum = 0;
    for(int i =0; i < bbuf.nrows; i++){
        brownum = bbuf.rowoffset[i];
        char *iter = bbuf.rowData + brownum;
        memcpy(testbuf,iter+25,7);
        float a = decimalch(testbuf);
        memcpy(testbuf,iter+32,7);
        float b = decimalch(testbuf);

        data = a * b + data;
        
    }
    cout << data << endl;
}

float decimalch(char b[]){
    char num[8];
    int *tempbuf;
    int ab;
    float cd;
    float ret;
    memset(num,0,8);
    for(int i = 0; i < 5; i++){
        num[i] = b[6-i];
    }
    tempbuf = (int*)num;
    ret = tempbuf[0];

    memset(num,0,8);
    num[0] = b[7];
    tempbuf = (int*)num;
    ab = tempbuf[0];
    cd = ab/100;
    return ret + cd;
}