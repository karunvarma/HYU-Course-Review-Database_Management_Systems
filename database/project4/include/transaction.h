#ifndef __TRANSACTION_H_
#define __TRANSACTION_H_

#include <stdint.h>
#include <pthread.h>
#include <iostream>
#include <list>
#include <unordered_map>
#include <utility>

volatile int transactionId = 0;

enum lockMode {
    SHARED,
    EXCLUSIVE
};

enum transactionState {
    IDLE,
    RUNNING,
    WAITING
};


typedef struct transactionManager_t {

    // TODO: choose template for transaction table
    // std::list<transaction_t> transactionTable;
    // std::unordered_map<int, transaction_t> transactionTable;
    pthread_mutex_t transactionManagerMutex;
} transactionManager_t;

typedef struct lockManager_t {

    // TODO: choose template for lock table
    // should lock table have head and tail both? why not only head?
    // std::unordered_map<uint64_t, std::pair<lock_t*, lock_t*> > lockTable;

    pthread_mutex_t lockManagerMutex;
} lockManager_t;

typedef struct undoLog_t {
    int tableId;
    int64_t key;
    char old_value[120];
} undoLog_t;

typedef struct transaction_t {
    int id;
    enum transactionState state;
    std::list <lock_t*> acquiredLocks;

    pthread_mutex_t transactionMutex;
    pthread_cond_t transactionCond;

    lock_t* waitLock;

    // TODO: undologlist
    std::list<undoLog_t> undoLogList;

} transaction_t;

typedef struct lock_t {

    // backpointer to transaction 
    transaction_t* transaction;

    int tableId;
    uint64_t pageNum;
    int64_t key;
    
    bool acquired;

    enum lockMode mode;
    lock_t* prev;
    lock_t* next;
} lock_t;

#endif /* __TRANSACTION_H_*/