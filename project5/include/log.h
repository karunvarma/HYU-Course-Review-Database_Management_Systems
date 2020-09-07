#ifndef __LOG_H_
#define __LOG_H_
#include <stdint.h>

enum logType {
    BEGIN,
    UPDATE,
    COMMIT,
    ABORT
};

typedef struct log {
    uint64_t LSN;
    uint64_t prevLSN;
    int transactionId;
    enum logType type;
    int tableId;
    uint32_t pageNum;
    uint32_t offset;
    uint32_t dataLen;
    char oldImage[120];
    char newImage[120];
};

#endif /* __LOG_H_*/