#include "buffer.h"

bufferPage_t* bufferPool = NULL;
int init_db(int bufNum) {
    bufferPool = (bufferPage_t*)malloc(sizeof(bufferPage_t) * bufNum);
    if (bufferPool == NULL) {
        return -1;
    }
    return 0;
}
