#ifndef __LOG_H_
#define __LOG_H_
#include <stdint.h>
#include <list>
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>

#pragma pack(push, 1)
#define LOG_FILE_NAME "LOGFILE"

enum logType {
    BEGIN,
    UPDATE,
    COMMIT,
    ABORT
};

typedef struct log_t {
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

int initLog();
int openLogFile();
int addLog(uint64_t LSN, int transactionId, logType type);
int addLog(uint64_t LSN, int transactionId, logType type, int tableId, uint32_t pageNum, uint32_t offset, uint32_t dataLen, char* oldImage, char* newImage);

int readLog(uint64_t prevLSN, log_t* dest);
int flushLogBuffer();

#pragma pack(pop)
#endif /* __LOG_H_*/