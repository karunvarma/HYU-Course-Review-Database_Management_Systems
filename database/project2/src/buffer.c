#include "buffer.h"

bufferPage_t* bufferPool = NULL;
int bufferSize = 0;
// int tableId
int init_db(int bufNum) {
    bufferSize = bufNum;
    if (bufferSize == 0) {
        return -1;
    }
    bufferPool = (bufferPage_t*)malloc(sizeof(bufferPage_t) * bufNum);
    if (bufferPool == NULL) {
        return -1;
    }
    return 0;
}

// int close_table(int table_id) {
//     if (bufferSize == NULL || bufferPool == NULL) {
//         return -1;
//     }

//     for (int i = 0; i < bufferSize; i++) {
//         if (bufferPool[i].tableId == table_id) {
//             bufferWritePage(pagenum)
//         }
//     }
// }