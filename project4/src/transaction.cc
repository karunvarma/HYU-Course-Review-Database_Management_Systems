
#include "../include/transaction.h"

transactionManager_t transactionManager {
    {},
    0,
    PTHREAD_MUTEX_INITIALIZER
};

lockManager_t lockManager = {
    {},
    PTHREAD_MUTEX_INITIALIZER
};


int begin_trx() {
    int newTransactionId;
    transaction_t newTransaction {
        0,
        IDLE,
        {},
        PTHREAD_MUTEX_INITIALIZER,
        PTHREAD_COND_INITIALIZER,
        NULL,
        {}
    };

    // 1 Acquire the transaction system latch.
    pthread_mutex_lock(&transactionManager.transactionManagerMutex);

    // 2 Get a new transaction id.
    newTransactionId = transactionManager.nextTransactionId;
    newTransaction.id = newTransactionId;

    // 3 Insert a new transaction structure in the transaction table. 
    transactionManager.transactionTable[newTransactionId] = newTransaction;

    __sync_fetch_and_add(&transactionManager.nextTransactionId, 1);

    // 4 Release the transaction system latch.
    pthread_mutex_unlock(&transactionManager.transactionManagerMutex);

    // 5 Return the new transaction id.
    return newTransactionId;
}


int end_trx(int transactionId) {
    transaction_t* transaction;
    pthread_mutex_lock(&transactionManager.transactionManagerMutex);
    transaction = &transactionManager.transactionTable[transactionId];
    // 1 Acquire the lock table latch.
    pthread_mutex_lock(&lockManager.lockManagerMutex);
    // 2 Release all acquired locks and wake up threads who wait on this
    // transaction(lock node).
    // 3 Release the lock table latch.
    // 4 Acquire the transaction system latch.
    // 5 Delete the transaction from the transaction table.
    // 6 Release the transaction system latch.
    // 7 Return the transaction id.
}

int acquireRecordLock(int tableId, uint64_t pageNum, int64_t key, int transactionId) {


    // Acquire the lock table latch.
    pthread_mutex_lock(&lockManager.lockManagerMutex);
    // Find the linked list with identical page id in lock table.
    // Find lock nodes of given key in the list. If there is not a lock node of given key, insert a new lock node.
    // 1 If no conflict, return SUCCESS.
    // 2 If conflict, return CONFLICT.
    // 3 If deadlock is detected, return DEADLOCK.
    lock_t* lock;



    // lock = lockManager.lockTable[pageNum].second;

    if (lock == NULL) {
        // no lock in the bucket
        // make lock node
        // insert to the list
    }

    // there's lock list
    // check conflict
    while(lock )
    pthread_mutex_unlock(&lockManager.lockManagerMutex);

    // acquire lock table mutex
    // pthread_mutex_lock( &lockManager.lockmanager )
}