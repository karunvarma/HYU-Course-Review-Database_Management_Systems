#ifndef __BUFFER_H_
#define __BUFFER_H_

#include <fcntl.h>
#include "index.h"
#include "file.h"


typedef struct table {
    int tableId;
    int fd;
} table;

typedef struct bufferPage_t {
    page_t page;
    int tableId;
    pagenum_t pageNum;
    // dirty 1 , not dirty 0
    int isDirty;
    // pinned 1, not pinned 0
    int isPinned;
    struct bufferPage_t* prev;
    struct bufferPage_t* next;
} bufferPage_t;


int bufferOpenTable(char* pathname);

int bufferWritePage(int tableid, pagenum_t pageNum);
page_t* bufferRequestPage(int tableId, pagenum_t pageNum);


int getFdOfTable(int tableId);
// get index of buffer page that is victim of replace
int getIndexOfVictim();
void updateToMostRecent(bufferPage_t* bufferPage);
#endif /* __BUFFER_H_*/