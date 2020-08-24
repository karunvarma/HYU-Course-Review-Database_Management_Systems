#include "../include/buffer.h"

bufferPage_t* bufferPool = NULL;
pthread_mutex_t bufferPoolMutex = PTHREAD_MUTEX_INITIALIZER;
int numOfBuffer = 0;

table* tables = NULL;
pthread_mutex_t fdTableMutex = PTHREAD_MUTEX_INITIALIZER;
int numOfTables = 0;

// open table and store table's information
// return tableId(= numOfTables)
int bufferOpenTable(char* pathname) {
    if (file_open_table(pathname) == 0) { //not exist
        //init table
        page_t* header = (page_t*)malloc(sizeof(struct page_t));
        ((headerPage_t*)header) -> rootPageNum = 0;
        ((headerPage_t*)header) -> numOfPages = 1;
        ((headerPage_t*)header) -> freePageNum = 0;
        file_write_page(HEADERPAGENUM, header);
        free(header);
    }
    numOfTables++;
    tables = (table*)realloc(tables, sizeof(struct table) * numOfTables);
    tables[numOfTables - 1].fd = fd;
    tables[numOfTables - 1].tableId = numOfTables;
    return numOfTables;
}


int bufferInitDb(int bufNum) {
    numOfBuffer = bufNum;
    if (numOfBuffer == 0) {
        return FAIL;
    }
    bufferPool = (bufferPage_t*)malloc(sizeof(bufferPage_t) * bufNum);
    if (bufferPool == NULL) {
        return FAIL;
    }
    for (int i = 0; i < numOfBuffer; i++) {
        bufferPool[i].pageNum = 0;
        bufferPool[i].tableId = 0;
        bufferPool[i].isDirty = 0;
        bufferPool[i].isPinned = 0;
        if (i == 0) {
            bufferPool[i].prev = NULL;
            bufferPool[i].next = &bufferPool[i + 1];
        } else if (i == numOfBuffer - 1) {
            bufferPool[i].prev = &bufferPool[i - 1];
            bufferPool[i].next = NULL;
        } else {
            bufferPool[i].prev = &bufferPool[i - 1];
            bufferPool[i].next = &bufferPool[i + 1];
        }
        bufferPool[i].bufferPageMutex = PTHREAD_MUTEX_INITIALIZER;
    }
    return SUCCESS;
}

int bufferCloseTable(int table_id) {
    if (numOfBuffer == 0 || bufferPool == NULL) { 
        printf("error\n");
        return FAIL;
    }

    fd = bufferGetFdOfTable(table_id);

    for (int i = 0; i < numOfBuffer; i++) {
        if (bufferPool[i].tableId == table_id && bufferPool[i].isDirty) {
            file_write_page(bufferPool[i].pageNum, &bufferPool[i].page);
            bufferPool[i].isDirty = 0;
        }
    }

    return SUCCESS;

}

int bufferShutDownDb() {
    for (int i = 0; i < numOfTables; i++) {
        if (bufferCloseTable(tables[i].tableId) == FAIL) {
            return FAIL;
        }
    }
    free(bufferPool);
    bufferPool = NULL;
    return SUCCESS;
}

page_t* bufferRequestPage(int tableId, pagenum_t pageNum) {
    bufferPage_t* bufferPage = NULL;
    page_t* retPage = NULL;
    int i;
    
    bufferPage = bufferFindBufferPage(tableId, pageNum);
    // if there's finding page in the bufferpool
    // pin the buffer and return page
    if (bufferPage != NULL) {
        while (bufferPage -> isPinned != 0) {
            // wait until isPinned to be 0
            // printf("waiting ispinned to be 0\n");
        }
        retPage = &(bufferPage -> page);
        (bufferPage -> isPinned)++;

        updateToMostRecent(bufferPage);
        return retPage;
    }

    // no page in the bufferpool
    // replace (LRU policy)
    // victim is least recently used bufferpage among not pinned

    // find the first one in the LRU list
    bufferPage  = &bufferPool[0];
    while( bufferPage -> prev != NULL) {
        bufferPage = bufferPage -> prev;
    }

    // find the victim following the LRU list
    while (retPage == NULL) {
        if (bufferPage -> isPinned == 0) {
            //found victim
            if (bufferPage -> isDirty) {
                fd = bufferGetFdOfTable(bufferPage -> tableId);
                file_write_page(bufferPage -> pageNum, &bufferPage -> page);
            }
            fd = bufferGetFdOfTable(tableId);
            file_read_page(pageNum, &(bufferPage -> page));
            retPage = &bufferPage -> page;
            bufferPage -> isDirty = 0;
            bufferPage -> isPinned = 0;
            (bufferPage -> isPinned)++;
            bufferPage -> pageNum = pageNum;
            bufferPage -> tableId = tableId;

            updateToMostRecent(bufferPage);
            return retPage;
        } else if (bufferPage -> next == NULL){
            // move to next one
            printf("[ERROR]: All buffer page is pinned!\n");
            printf("Fail to replace page\n");
            exit(EXIT_FAILURE);
        }
        bufferPage = bufferPage -> next;
    }

    return retPage;

}

void updateToMostRecent(bufferPage_t* bufferPage) {
    bufferPage_t* mostRecent = bufferPage;
    if (bufferPage -> next == NULL) {
        // bufferPage is already most recent
        // nothing to do
        return;
    }
    while(mostRecent -> next != NULL) {
        mostRecent = mostRecent -> next;
    }
    // no need , because bufferpage -> prev is null
    if (bufferPage -> prev != NULL) {
        bufferPage -> prev -> next = bufferPage -> next; 
    }
    bufferPage -> next -> prev = bufferPage -> prev;

    mostRecent -> next = bufferPage;
    bufferPage -> prev = mostRecent;
    bufferPage -> next = NULL;
}


int bufferGetFdOfTable(int tableId) {
    for(int i = 0; i < numOfTables; i++) {
        if (tables[i].tableId == tableId) {
            return tables[i].fd;
        }
    }
    printf("something's wrong\n");
    return FAIL;
}


bufferPage_t* bufferFindBufferPage(int tableId, pagenum_t pageNum) {
    for (int i = 0; i < numOfBuffer; i++) {
        if (bufferPool[i].tableId == tableId && bufferPool[i].pageNum == pageNum) {
            return &bufferPool[i];
        }
    }
    return NULL;
}

int bufferMakeDirty(int tableId, pagenum_t pageNum) {
    bufferPage_t* bufferPage;
    
    bufferPage = bufferFindBufferPage(tableId, pageNum);

    if (bufferPage == NULL) {
        //for debug
        printf("should not execute this line\n");
        return FAIL;
    } else {
        bufferPage -> isDirty = 1;
        return SUCCESS;
    }
}

int bufferPinPage(int tableId, pagenum_t pageNum) {
    bufferPage_t* bufferPage;

    bufferPage = bufferFindBufferPage(tableId, pageNum);

    if (bufferPage == NULL) {
        //for debug
        printf("should not execute this line\n");
        return FAIL;
    } else {
        (bufferPage -> isPinned)++;
        return SUCCESS;
    }
}

int bufferUnpinPage(int tableId, pagenum_t pageNum) {
    bufferPage_t* bufferPage;

    bufferPage = bufferFindBufferPage(tableId, pageNum);
    if (bufferPage == NULL) {
        //for debug
        printf("should not execute this line\n");
        return FAIL;
    } else {
        (bufferPage -> isPinned)--;
        return SUCCESS;
    }
}

void bufferFreePage(int tableId, pagenum_t pageNum) {
    page_t* page, * header;
    pagenum_t nextFreePageNum;

    header = bufferRequestPage(tableId, HEADERPAGENUM);
    nextFreePageNum = ((headerPage_t*)header) -> freePageNum;
    ((headerPage_t*)header) -> freePageNum = pageNum;
    bufferMakeDirty(tableId, HEADERPAGENUM);
    bufferUnpinPage(tableId, HEADERPAGENUM);

    page = bufferRequestPage(tableId, pageNum);
    ((freePage_t*)page) -> nextFreePageNum = nextFreePageNum;
    bufferMakeDirty(tableId, pageNum);
    bufferUnpinPage(tableId, pageNum);
    
}

pagenum_t bufferAllocPage(int tableId) {
    page_t* header, * freePage;
    pagenum_t freePageNum, nextFreePageNum;
    

    header = bufferRequestPage(tableId, HEADERPAGENUM);
    freePageNum = ((headerPage_t*)header) -> freePageNum;

    if (freePageNum == 0) {
        freePageNum = ((headerPage_t*)header) -> numOfPages;
        (((headerPage_t*)header) -> numOfPages)++;

        bufferMakeDirty(tableId, HEADERPAGENUM);
        bufferUnpinPage(tableId, HEADERPAGENUM);

        //make physical db size increase
        page_t page = {0, };
        fd = bufferGetFdOfTable(tableId);
        file_write_page(freePageNum, &page);
        return freePageNum;
    } else {
        freePage = bufferRequestPage(tableId, freePageNum);
        nextFreePageNum = ((freePage_t*)freePage) -> nextFreePageNum;
        bufferUnpinPage(tableId, freePageNum);

        ((headerPage_t*)header) -> freePageNum = nextFreePageNum;
        bufferMakeDirty(tableId, HEADERPAGENUM);
        bufferUnpinPage(tableId, HEADERPAGENUM);
        return freePageNum;

    }
}

int bufferLockBufferPage(int tableId, pagenum_t pageNum) {
    bufferPage_t* bufferPage;

    bufferPage = bufferFindBufferPage(tableId, pageNum);

    if (bufferPage == NULL) {
        //for debug
        printf("should not execute this line\n");
        return FAIL;
    } else {
        if (pthread_mutex_trylock(&bufferPage -> bufferPageMutex) == 0) {
            // lock success
            return SUCCESS;
        } else {
            // lock fail
            return FAIL;
        }
        
    }
}

int bufferUnlockBufferPage(int tableId, pagenum_t pageNum) {
    bufferPage_t* bufferPage;

    bufferPage = bufferFindBufferPage(tableId, pageNum);

    if (bufferPage == NULL) {
        //for debug
        printf("should not execute this line\n");
        return FAIL;
    } else {
        pthread_mutex_unlock(&bufferPage -> bufferPageMutex);
        return SUCCESS;
    }
}