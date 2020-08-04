#ifndef __BUFFER_H_
#define __BUFFER_H_

#include "index.h"
#include <fcntl.h>

typedef struct bufferPage_t {
    page_t page;
    uint64_t tableId;
    pagenum_t pageNum;
    int isDirty;
    int isPinned;
    bufferPage_t* next;
} bufferPage_t;

/* 
Allocate the buffer pool (array) with the given number of entries. 
• Initialize other fields (state info, LRU info..) with your own design.
• If success, return 0. Otherwise, return non-zero value.
*/
int init_db (int buf_num);

#endif /* __BUFFER_H_*/