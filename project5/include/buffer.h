#ifndef __BUFFER_H_
#define __BUFFER_H_

#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <string>
#include "file.h"

#define SUCCESS 0
#define FAIL -1

//global
extern pthread_mutex_t bufferPoolMutex;

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
    pthread_mutex_t bufferPageMutex;
} bufferPage_t;


int bufferOpenTable(char* pathname);

// find page in buffer pool
// if found, return page and increase pincount
// not found, replace least recently used buffer and increase pincount
page_t* bufferRequestPage(int tableId, pagenum_t pageNum);

int bufferGetFdOfTable(int tableId);
void updateToMostRecent(bufferPage_t* bufferPage);

int bufferInitDb(int bufNum);
int bufferCloseTable(int table_id);

//flush all data from buffer and destroy allocated buffer.
//If success, return 0. Otherwise, return -1
int bufferShutDownDb();

//find bufferpage with tableId, pageNum in the bufferPool
//if success return pointer, else NULL
bufferPage_t* bufferFindBufferPage(int tableId, pagenum_t pageNum);

//if success return 0, else -1
int bufferMakeDirty(int tableId, pagenum_t pageNum);

//if success return 0, else -1
int bufferPinPage(int tableId, pagenum_t pageNum);

//if success return 0, else -1
int bufferUnpinPage(int tableId, pagenum_t pageNum);

// pinMax: 1
void bufferFreePage(int tableId, pagenum_t pageNum);

//pinMax: 2 (can be 1)
//alloc pagenum and write {0} 4kb into physical file
pagenum_t bufferAllocPage(int tableId);

int bufferLockBufferPage(int tableId, pagenum_t pageNum);
int bufferUnlockBufferPage(int tableId, pagenum_t pageNum);


page_t* bufferRequestAndLockPage(int tableId, pagenum_t pageNum);

#endif /* __BUFFER_H_*/