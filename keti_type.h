#ifndef _KETI_TYPE_H_
#define _KETI_TYPE_H_

typedef enum work_type{
	SCAN = 4,
	SCAN_N_FILTER = 5,
	REQ_SCANED_BLOCK = 6,
	WORK_END = 9
}KETI_WORK_TYPE;

typedef enum opertype
{
    KETI_GE = 0,  // >=
    KETI_LE,      // <=
    KETI_GT,      // >
    KETI_LT,      // <
    KETI_ET,      // ==
    KETI_NE,      // !=
    KETI_LIKE,    // RV로 스트링
    KETI_BETWEEN, // extra로 배열형식 [10,20] OR [COL1,20]
    KETI_IN,      // extra로 배열형식 [10,20,30,40]
    KETI_IS,      // IS 와 IS NOT을 구분 RV는 무조건 NULL
    KETI_ISNOT,   // IS와 구분 필요 RV는 무조건 NULL
    KETI_NOT,     // ISNOT과 관련 없음 OPERATOR 앞에 붙는 형식 --> 혼자 들어오는 oper
    KETI_AND,     // AND --> 혼자 들어오는 oper
    KETI_OR,      // OR --> 혼자 들어오는 oper
	KETI_DEFAULT = 15
}KETI_OPER_TYPE;

#endif
