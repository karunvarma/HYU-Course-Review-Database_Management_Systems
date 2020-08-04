#include "buffer.h"
#include "file.h"

bufferPage_t* bufferPool = NULL;
int numOfBuffer = 0;

table* tables = NULL;
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
        return -1;
    }
    bufferPool = (bufferPage_t*)malloc(sizeof(bufferPage_t) * bufNum);
    if (bufferPool == NULL) {
        return -1;
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
    }
    return 0;
}

int bufferCloseTable(int table_id) {
    if (numOfBuffer == 0 || bufferPool == NULL) { 
        printf("error\n");
        return -1;
    }

    for (int i = 0; i < numOfTables; i++) {
        if (tables[i].tableId == table_id) {
            fd = tables[i].fd;
            tables[i].fd = 0;
            tables[i].tableId = 0;
            break;
        }
    }

    for (int i = 0; i < numOfBuffer; i++) {
        if (bufferPool[i].tableId == table_id && bufferPool[i].isDirty) {
            file_write_page(bufferPool[i].pageNum, &bufferPool[i].page);
        }
    }

    return 0;

}

page_t* bufferRequestPage(int tableId, pagenum_t pageNum) {
    
    int i;
    // if there's page in the buffer
    for (i = 0; i < numOfBuffer; i++) {
        if (bufferPool[i].pageNum == pageNum && bufferPool[i].tableId == tableId) {
            return &(bufferPool[i].page);
        }
    }

    // no page in the buffer
    // replace (LRU policy)
    page_t* retPage = NULL;
    bufferPage_t* bufferPage  = &bufferPool[0];

    while(retPage == NULL) {
        if (bufferPage -> prev == NULL && bufferPage -> isPinned == 0) {
            if (bufferPage -> isDirty) {
                fd = getFdOfTable(bufferPage -> tableId);
                file_write_page(bufferPage -> pageNum, &bufferPage -> page);
            }
            fd = getFdOfTable(tableId);
            file_read_page(pageNum, &bufferPage -> page);
            retPage = &bufferPage -> page;
            bufferPage -> isDirty = 0;
            bufferPage -> isPinned = 0;
            bufferPage -> pageNum = pageNum;
            bufferPage -> tableId = tableId;

            updateToMostRecent(bufferPage);

        }

        bufferPage = bufferPage -> prev;
    }

    return retPage;

}

void updateToMostRecent(bufferPage_t* bufferPage) {
    bufferPage_t* mostRecent = bufferPage;

    while(mostRecent -> next != NULL) {
        mostRecent = bufferPage -> next;
    }
    // no need , because bufferpage -> prev is null
    // bufferPage -> prev -> next = bufferPage -> next; 
    bufferPage -> next -> prev = NULL;

    mostRecent -> next = bufferPage;
    bufferPage -> prev = mostRecent;
    bufferPage -> next = NULL;
}


int getFdOfTable(int tableId) {
    for(int i = 0; i < numOfTables; i++) {
        if (tables[i].tableId == tableId) {
            return tables[i].fd;
        }
    }
    printf("something's wrong\n");
    return -1;
}

 void bufferWritePage(int tableid, pagenum_t pageNum) {
     //find page 


     // page exist

     //page not exist , 
 }