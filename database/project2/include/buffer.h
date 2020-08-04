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


#endif /* __BUFFER_H_*/