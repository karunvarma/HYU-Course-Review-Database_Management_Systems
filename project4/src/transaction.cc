
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
    lock_t* acquiredLock, * lock;

    while (!(transaction -> acquiredLocks.empty())) {

        acquiredLock = transaction -> acquiredLocks.front();
        transaction -> acquiredLocks.pop_front();


        lock = acquiredLock -> next;
        if (acquiredLock -> prev != NULL) {
            acquiredLock -> prev -> next = acquiredLock -> next;
        }
        if (acquiredLock -> next != NULL) {
            acquiredLock -> next -> prev = acquiredLock -> prev;
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
    // 5 Delete the transaction from the transaction table.
    transactionManager.transactionTable.erase(transactionId);
    // 6 Release the transaction system latch.
    pthread_mutex_unlock(&transactionManager.transactionManagerMutex);
    
    // 7 Return the transaction id.
    return transactionId;
}

int acquireRecordLock(int tableId, uint64_t pageNum, int64_t key, lockMode mode, int transactionId) {

    // Acquire the lock table latch.
    pthread_mutex_lock(&lockManager.lockManagerMutex);
    // Find the linked list with identical page id in lock table.
    // Find lock nodes of given key in the list. If there is not a lock node of given key, insert a new lock node.
    // 1 If no conflict, return LOCKSUCCESS.
    // 2 If conflict, return CONFLICT.
    // 3 If deadlock is detected, return DEADLOCK.
    lock_t* lock, * tail, * newLock;
    //check from tail of lockTable.
    tail = lockManager.lockTable[pageNum].second;
    lock = tail;

    if (lock == NULL) {

        // no lock in the bucket
        // make lock node
        // insert to the list
        newLock = (lock_t *)malloc(sizeof(struct lock_t));
        pthread_mutex_lock(&transactionManager.transactionManagerMutex);
        newLock -> transaction = &transactionManager.transactionTable[transactionId];
        pthread_mutex_unlock(&transactionManager.transactionManagerMutex);
        newLock -> tableId = tableId;
        newLock -> pageNum = pageNum;
        newLock -> key = key;
        newLock -> acquired = true;
        newLock -> mode = mode;

        newLock -> prev = NULL;
        newLock -> next = NULL;
        lockManager.lockTable[pageNum].first = newLock;
        lockManager.lockTable[pageNum].second = newLock;

        pthread_mutex_unlock(&lockManager.lockManagerMutex);
        return LOCKSUCCESS;
    }

    // check if transaction already acquired the lock
    
    // where should check this? in lock list or transaction's lock list,
    // should lock the transaction manager?

    while (lock != NULL) {
        if (lock -> transaction -> id == transactionId &&
            lock -> tableId == tableId &&
            lock -> key == key) {
            if (lock -> acquired == true) {
                if (lock -> mode == EXCLUSIVE) {
                    pthread_mutex_unlock(&lockManager.lockManagerMutex);
                    return LOCKSUCCESS;
                } else {
                    // mode == SHARED
                    if (mode == EXCLUSIVE) {
                        pthread_mutex_unlock(&lockManager.lockManagerMutex);
                        return DEADLOCK;
                    } else {
                        pthread_mutex_unlock(&lockManager.lockManagerMutex);
                        return LOCKSUCCESS;
                    }
                }
            } else {
                // have been slept because of CONFLICT.
                // some thread woke me up.
                lock -> transaction -> acquiredLocks.push_back(lock);
                lock -> transaction -> state = RUNNING;
                lock -> transaction -> waitLock = NULL;

                lock -> acquired = true;

                pthread_mutex_unlock(&lockManager.lockManagerMutex);
                return LOCKSUCCESS;

            }
        }
        lock = lock -> prev;
    }
    lock = tail;
    
    lock_t* lastLock;
    transaction_t* transaction;
    // check conflict
    while (lock != NULL) {
        if (lock -> tableId == tableId && lock -> key == key){
            lastLock = lock;

            // check DEADLOCK, following wait-for graph
            transaction = lastLock -> transaction;
            while (transaction -> state == WAITING) {
                if (transaction -> id == transactionId) {
                    // cycle will be created if we insert this lock
                    pthread_mutex_unlock(&lockManager.lockManagerMutex);
                    return DEADLOCK;
                }
                transaction = transaction -> waitLock -> transaction;
            }

            // not DEADLOCK , just CONFLICT
            newLock = (lock_t*)malloc(sizeof(struct lock_t));

            pthread_mutex_lock(&transactionManager.transactionManagerMutex);
            newLock -> transaction = &transactionManager.transactionTable[transactionId];
            pthread_mutex_unlock(&transactionManager.transactionManagerMutex);

            newLock -> transaction -> state = WAITING;
            newLock -> transaction -> waitLock = lock;

            newLock -> tableId = tableId;
            newLock -> pageNum = pageNum;
            newLock -> key = key;
            newLock -> acquired = true;
            newLock -> mode = mode;
            
            // insert into lock list
            newLock -> prev = lockManager.lockTable[pageNum].second;
            lockManager.lockTable[pageNum].second -> next = newLock;
            lockManager.lockTable[pageNum].second = newLock;
            newLock -> next = NULL;

        }
        lock = lock -> prev;
    }
    pthread_mutex_unlock(&lockManager.lockManagerMutex);
    
    return LOCKSUCCESS;
}