#ifndef __TRANSACTION_H_
#define __TRANSACTION_H_

#include <stdint.h>
#include <pthread.h>
#include <iostream>
#include <list>
#include <unordered_map>
#include <map>
#include <utility>
#include <stdlib.h>

extern struct transactionManager_t transactionManager;
extern struct lockManager_t lockManager;

enum lockMode {
    SHARED,
    EXCLUSIVE
};

enum transactionState {
    IDLE,
    RUNNING,
    WAITING
};

struct lock_t;
struct transaction_t;


typedef struct transactionManager_t {

    // TODO: choose template for transaction table
    // std::list<transaction_t> transactionTable;
    std::map<int, transaction_t> transactionTable;

    volatile int nextTransactionId;
    pthread_mutex_t transactionManagerMutex;
} transactionManager_t;

typedef struct lockManager_t {

    // TODO: choose template for lock table
    // should lock table have head and tail both? why not only head?
    std::unordered_map<uint64_t, std::pair<lock_t*, lock_t*> > lockTable;

    pthread_mutex_t lockManagerMutex;
} lockManager_t;

typedef struct undoLog_t {
    int tableId;
    int64_t key;
    char oldValue[120];
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

// • Allocate transaction structure and initialize it.
// • Return the unique transaction id if success, otherwise return 0.
// • Note that transaction id should be unique for each transaction, 
// that is you need to allocate transaction id using mutex or atomic instruction, 
// such as __sync_fetch_and_add().
int begin_trx();

// • Clean up the transaction with given tid (transaction id) and its related information
// that has been used in your lock manager. (Shrinking phase of strict 2PL)
// • Return the completed transaction id if success, otherwise return 0.
int end_trx(int transactionId);


int acquireRecordLock(int tableId, uint64_t pageNum, int64_t key, lockMode mode, int transactionId);


#endif /* __TRANSACTION_H_*/