
#include "../include/transaction.h"

transactionManager_t transactionManager {
    {},
    1,
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

    if (transactionManager.transactionTable.find(transactionId) == transactionManager.transactionTable.end()) {
        // transaction doesn't exist in the table,
        // maybe aborted in previous job.
        
        // fail return 0
        return 0;
    }
    transaction = &transactionManager.transactionTable[transactionId];
    
    // 1 Acquire the lock table latch.
    pthread_mutex_lock(&lockManager.lockManagerMutex);
    // 2 Release all acquired locks and wake up threads who wait on this
    // transaction(lock node).
    lock_t* acquiredLock, * lock;

    while (!(transaction -> acquiredLocks.empty())) {

        acquiredLock = transaction -> acquiredLocks.front();
        transaction -> acquiredLocks.pop_front();


        lock = acquiredLock -> next;
        // update lock list
        if (acquiredLock -> prev == NULL && acquiredLock -> next == NULL) {
            lockManager.lockTable[acquiredLock -> pageNum].first = NULL;
            lockManager.lockTable[acquiredLock -> pageNum].second = NULL;
        } else if (acquiredLock -> prev == NULL && acquiredLock -> next != NULL) {
            lockManager.lockTable[acquiredLock -> pageNum].first = acquiredLock -> next;
            acquiredLock -> next -> prev = acquiredLock -> prev;

        } else if (acquiredLock -> prev != NULL && acquiredLock -> next == NULL) {
            lockManager.lockTable[acquiredLock -> pageNum].second = acquiredLock -> prev;
            acquiredLock -> prev -> next = acquiredLock -> next;

        } else if (acquiredLock -> prev != NULL && acquiredLock -> next != NULL) {
            acquiredLock -> next -> prev = acquiredLock -> prev;
            acquiredLock -> prev -> next = acquiredLock -> next;
        }


        while (lock != NULL) {

            // acquired, 
            if (lock -> acquired == true) {
                lock = lock -> next;
                continue;
            }

            // not acquired && on the same object.
            if (lock -> tableId == acquiredLock -> tableId &&
                lock -> pageNum == acquiredLock -> pageNum &&
                lock -> key == acquiredLock -> key
                ) 
            { 
                // lock is waiting acquired lock to release
                if (lock -> transaction -> waitLock == acquiredLock) {
                    // wake up
                    pthread_mutex_lock(&lock -> transaction -> transactionMutex);
                    pthread_cond_signal(&lock -> transaction -> transactionCond);
                    pthread_mutex_unlock(&(lock -> transaction -> transactionMutex));
                }
            }


            lock = lock -> next;
        }
        free(acquiredLock);
    }

    // 3 Release the lock table latch.
    pthread_mutex_unlock(&lockManager.lockManagerMutex);
    // 4 Acquire the transaction system latch.
    pthread_mutex_lock(&transactionManager.transactionManagerMutex);
    // 5 Delete the transaction from the transaction table.
    transactionManager.transactionTable.erase(transactionId);
    // 6 Release the transaction system latch.
    pthread_mutex_unlock(&transactionManager.transactionManagerMutex);
    
    // 7 Return the transaction id.
    return transactionId;
}

int acquireRecordLock(int tableId, uint64_t pageNum, int64_t key, lockMode mode, int transactionId) {
    lock_t* conflictLock, * lock, * tail; 
    transaction_t* transaction;
    std::list<lock_t*>::iterator it;
    bool isWokeUp;
    pthread_mutex_lock(&lockManager.lockManagerMutex);
    isWokeUp = false;
    tail = lockManager.lockTable[pageNum].second;
    lock = tail;

    if (lock == NULL) {
        lock = (lock_t *)malloc(sizeof(struct lock_t));
        lock -> transaction = &transactionManager.transactionTable[transactionId];
        transactionManager.transactionTable[transactionId].acquiredLocks.push_back(lock);
        lock -> tableId = tableId;
        lock -> pageNum = pageNum;
        lock -> key = key;
        lock -> acquired = true;
        lock -> mode = mode;

        lock -> prev = NULL;
        lock -> next = NULL;
        lockManager.lockTable[pageNum].first = lock;
        lockManager.lockTable[pageNum].second = lock;

        pthread_mutex_unlock(&lockManager.lockManagerMutex);
        return LOCKSUCCESS;
    }

    transaction = &transactionManager.transactionTable[transactionId];
    if (!(transaction -> acquiredLocks.empty())) {

        for(it = transaction -> acquiredLocks.begin(); it != transaction -> acquiredLocks.end(); it++){ 
            if ((*it) -> tableId == tableId && (*it) -> key == key) {
                if ((*it) -> mode == SHARED && mode == EXCLUSIVE) {
                    // should push lock? think not
                    pthread_mutex_unlock(&lockManager.lockManagerMutex);
                    return DEADLOCK;
                } else {
                    pthread_mutex_unlock(&lockManager.lockManagerMutex);
                    return LOCKSUCCESS;
                }
            }
        }
    }

    lock = tail;

    // check this transaction woke up by other transaction
    while (lock != NULL) {
        if (lock -> transaction -> id == transactionId && lock -> tableId == tableId && lock -> key == key && lock -> mode == mode && lock -> acquired == false){
            isWokeUp = true;
            tail = lock -> prev;
            break;
        }
        lock = lock -> prev;
    }

    // lock is NULL (not woke up) or (woke up)

    // check conflict
    conflictLock = tail;

    while (conflictLock != NULL) {
        if (conflictLock -> tableId == tableId && conflictLock -> key == key) {
            if (conflictLock -> mode == SHARED && mode == SHARED) {
                conflictLock = conflictLock -> prev;
                continue;
            }
            // check deadlock
            transaction = conflictLock -> transaction;
            while (transaction -> state == WAITING) {
                if (transaction -> waitLock -> transaction -> id == transactionId) {
                    if (isWokeUp) {
                        lock -> transaction -> acquiredLocks.push_back(lock);
                    }
                    pthread_mutex_unlock(&lockManager.lockManagerMutex);
                    return DEADLOCK;
                }
                transaction = transaction -> waitLock -> transaction;
            }
            if (!isWokeUp) {
                lock = (lock_t*)malloc(sizeof(struct lock_t));
                lock -> transaction = &transactionManager.transactionTable[transactionId];

                lock -> tableId = tableId;
                lock -> pageNum = pageNum;
                lock -> key = key;
                lock -> mode = mode;

                lock -> acquired = false;
                
                // push back into lock list
                lock -> prev = lockManager.lockTable[pageNum].second;
                lockManager.lockTable[pageNum].second -> next = lock;
                lockManager.lockTable[pageNum].second = lock;
                lock -> next = NULL;
            }
            lock -> transaction -> waitLock = conflictLock;
            lock -> transaction -> state = WAITING;
            pthread_mutex_unlock(&lockManager.lockManagerMutex);
            return CONFLICT;
        }
        conflictLock = conflictLock -> prev;
    }

    // no conflict lock , lock success
    if (!isWokeUp) {
        lock = (lock_t*)malloc(sizeof(struct lock_t));
        lock -> transaction = &transactionManager.transactionTable[transactionId];


        lock -> tableId = tableId;
        lock -> pageNum = pageNum;
        lock -> key = key;
        lock -> mode = mode;


        // push back into lock list
        lock -> prev = lockManager.lockTable[pageNum].second;
        lockManager.lockTable[pageNum].second -> next = lock;
        lockManager.lockTable[pageNum].second = lock;
        lock -> next = NULL;

    }
    lock -> acquired = true;
    lock -> transaction -> acquiredLocks.push_back(lock);
    lock -> transaction -> state = RUNNING;
    lock -> transaction -> waitLock = NULL;
    pthread_mutex_unlock(&lockManager.lockManagerMutex);
    return LOCKSUCCESS;
}

int abortTransaction(int transactionId) {
    transaction_t* transaction;
    pagenum_t pageNum;
    page_t* page;
    int i;

    transaction = &transactionManager.transactionTable[transactionId];

    std::list<undoLog_t>::reverse_iterator rit;

    // end points to the next of the last element
    // check if ++rit -> rit++ makes error
    for (rit = transaction -> undoLogList.rbegin(); rit != transaction -> undoLogList.rend(); ++rit) {
        while (true) {
            pthread_mutex_lock(&bufferPoolMutex);
            pageNum = findLeaf(rit -> tableId, rit -> key);

            if (pageNum == 0) {
                printf("[ERROR]: pageNum == 0\n");
                pthread_mutex_unlock(&bufferPoolMutex);
                return FAIL;
            }
            page = bufferRequestPage(rit -> tableId, pageNum);

            if (bufferLockBufferPage(rit -> tableId, pageNum) == FAIL) {
                bufferUnpinPage(rit -> tableId, pageNum);
                pthread_mutex_unlock(&bufferPoolMutex);
                continue;
            }

            pthread_mutex_unlock(&bufferPoolMutex);

            //rollback
            
            // find index
            for (i = 0; i < ((leafPage_t*)page) -> numOfKeys; i++) {
                if (((leafPage_t*)page) -> record[i].key == rit -> key) {
                    break;
                }
            }
            if (((leafPage_t*)page) -> numOfKeys == i) {
                // no key in the page ,, should not execute this block
                bufferUnpinPage(rit -> tableId, pageNum);
                bufferUnlockBufferPage(rit -> tableId, pageNum);
                return FAIL;
            }

            strcpy(((leafPage_t*)page) -> record[i].value, rit -> oldValue);
            bufferMakeDirty(rit -> tableId, pageNum);
            bufferUnpinPage(rit -> tableId, pageNum);
            //release
            bufferUnlockBufferPage(rit -> tableId, pageNum);
            break;
        }
    }
    end_trx(transactionId);
    return SUCCESS;
}