#include "../include/log.h"

std::list<log_t*> logBuffer;

int logFd = 0;
uint64_t lastLSN = 0;

int initLog() {
    if (openLogFile() == 0) {
        lastLSN = 0;
        return 0;
    } else {
        lastLSN = lseek(logFd, 0, SEEK_END);
        return 1;
    }

}

int openLogFile() {
    if ((logFd = open(LOG_FILE_NAME, O_RDWR | O_CREAT | O_EXCL , 0644)) > 0) {
        return 0;
    } else {
        logFd = open(LOG_FILE_NAME, O_RDWR);
        return 1;
    }
}

// for BEGIN, COMMIT, ABORT

// TODO: remove LSN parameter, think it's not necessary.
int addLog(uint64_t LSN, int transactionId, logType type) {
    log_t* newLog;
    newLog = (log_t*)malloc(sizeof(struct log_t));
    memset(newLog, 0, sizeof(log_t));
    if (logFd == 0) {
        initLog();
    }
    newLog -> LSN = LSN;
    if (LSN == 40) {
        //first log
        newLog -> prevLSN = UINT64_MAX;
    } else {
        newLog -> prevLSN = lastLSN;
    }
    newLog -> transactionId = transactionId;
    newLog -> type = type;
    logBuffer.push_back(newLog);
    return 0;
}

// for UPDATE
int addLog(uint64_t LSN, int transactionId, logType type, int tableId, uint32_t pageNum, uint32_t offset, uint32_t dataLen, char* oldImage, char* newImage) {
    log_t* newLog;
    newLog = (log_t*)malloc(sizeof(struct log_t));
    memset(newLog, 0, sizeof(log_t));
    if (logFd == 0) {
        initLog();
    }
    newLog -> LSN = LSN;
    if (LSN == 280) {
        //first log
        newLog -> prevLSN = UINT64_MAX;
    } else {
        newLog -> prevLSN = lastLSN;
    }
    newLog -> transactionId = transactionId;
    newLog -> type = type;
    newLog -> tableId = tableId;
    newLog -> pageNum = pageNum;
    newLog -> offset = offset;
    newLog -> dataLen = dataLen;
    strcpy(newLog -> oldImage, oldImage);
    strcpy(newLog -> newImage, newImage);
    logBuffer.push_back(newLog);
    return 0;

}

int readLog(uint64_t prevLSN, log_t* dest) {
    lseek(logFd, prevLSN, SEEK_SET);
    read(logFd, dest, 40);

    if (dest -> type == UPDATE) {
        lseek(logFd, prevLSN, SEEK_SET);
        read(logFd, dest, 280);
    }
    return 0;
}

int flushLogBuffer() {
    if (logFd == 0) {
        // try to flush when log buffer is empty
        return -1;
    }
    log_t* log;

    while (!logBuffer.empty()) {
        log = logBuffer.front();
        if (log -> prevLSN == UINT64_MAX) {
            lseek(logFd, 0, SEEK_SET);
        } else {
            lseek(logFd, log -> prevLSN, SEEK_SET);
        }

        if (log -> type == UPDATE) {
            write(logFd, log, sizeof(struct log_t));
        } else {
            write(logFd, log, 40);
        }
        logBuffer.pop_front();
        free(log);
    }

    // TODO: should close?
    close(logFd);
    return 0;
}