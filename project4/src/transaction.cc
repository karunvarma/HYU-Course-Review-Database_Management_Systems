
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
    // TODO: should delete inserted lock of this transaction. (not acquired , waiting lock)

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

    // Acquire the lock table latch.
    pthread_mutex_lock(&lockManager.lockManagerMutex);
    // Find the linked list with identical page id in lock table.
    // Find lock nodes of given key in the list. If there is not a lock node of given key, insert a new lock node.
    // 1 If no conflict, return LOCKSUCCESS.
    // 2 If conflict, return CONFLICT.
    // 3 If deadlock is detected, return DEADLOCK.
    lock_t* lock, * tail, * newLock, * tmpLock;
    //check from tail of lockTable.
    tail = lockManager.lockTable[pageNum].second;
    lock = tail;

    // TODO: not use wait lock, 
    // don't access to freed memory,  
    // check conflict by following list prev, not waitlock.

    if (lock == NULL) {

        // no lock in the bucket
        // make lock node
        // insert to the list
        newLock = (lock_t *)malloc(sizeof(struct lock_t));
        newLock -> transaction = &transactionManager.transactionTable[transactionId];
        transactionManager.transactionTable[transactionId].acquiredLocks.push_back(newLock);
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
    transaction_t* transaction;

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

                // TODO: check waitLock is NULL
                // TODO: 이미 해제된 메모리에 접근하고 있음.( 해제 하고 깨움)
                if (lock -> transaction -> waitLock -> mode == EXCLUSIVE) {

                    tmpLock = lock -> prev;

                    // check CONFLICT
                    while (tmpLock != NULL) {
                        if (tmpLock -> tableId == tableId && tmpLock -> key == key) {
                            if (tmpLock -> acquired == true) {
                                if (tmpLock -> mode == SHARED && mode == SHARED) {
                                    lock -> acquired = true;
                                    lock -> transaction -> acquiredLocks.push_back(lock);
                                    pthread_mutex_unlock(&lockManager.lockManagerMutex);
                                    return LOCKSUCCESS;
                                }
                                //check deadlock
                                transaction = tmpLock -> transaction;
                                while (transaction -> state == WAITING) {
                                    if (transaction -> waitLock -> transaction -> id == transactionId) {
                                        lock -> transaction -> acquiredLocks.push_back(lock);
                                        pthread_mutex_unlock(&lockManager.lockManagerMutex);
                                        return DEADLOCK;
                                    }
                                    transaction = transaction -> waitLock -> transaction;
                                }
                                lock -> transaction -> state = WAITING;
                                lock -> transaction -> waitLock = tmpLock;

                                pthread_mutex_unlock(&lockManager.lockManagerMutex);
                                return CONFLICT;
                            } else {
                                if (mode == EXCLUSIVE) {
                                    //check deadlock
                                    transaction = tmpLock -> transaction;
                                    while (transaction -> state == WAITING) {
                                        if (transaction -> waitLock -> transaction -> id == transactionId) {
                                            lock -> transaction -> acquiredLocks.push_back(lock);
                                            pthread_mutex_unlock(&lockManager.lockManagerMutex);
                                            return DEADLOCK;
                                        }
                                        transaction = transaction -> waitLock -> transaction;
                                    }
                                    lock -> transaction -> state = WAITING;
                                    lock -> transaction -> waitLock = tmpLock;

                                    pthread_mutex_unlock(&lockManager.lockManagerMutex);
                                    return CONFLICT;
                                } else {
                                    if (tmpLock -> mode == SHARED) {
                                        // check deadlock and wait
                                        transaction = tmpLock -> transaction -> waitLock -> transaction;
                                        while (transaction -> state == WAITING) {
                                            if (transaction -> waitLock -> transaction -> id == transactionId) {
                                                lock -> transaction -> acquiredLocks.push_back(lock);
                                                pthread_mutex_unlock(&lockManager.lockManagerMutex);
                                                return DEADLOCK;
                                            }
                                            transaction = transaction -> waitLock -> transaction;
                                        }
                                        lock -> transaction -> state = WAITING;
                                        lock -> transaction -> waitLock = tmpLock -> transaction -> waitLock;

                                        pthread_mutex_unlock(&lockManager.lockManagerMutex);
                                        return CONFLICT;
                                    } else {
                                        //check deadlock
                                        transaction = tmpLock -> transaction;
                                        while (transaction -> state == WAITING) {
                                            if (transaction -> waitLock -> transaction -> id == transactionId) {
                                                lock -> transaction -> acquiredLocks.push_back(lock);
                                                pthread_mutex_unlock(&lockManager.lockManagerMutex);
                                                return DEADLOCK;
                                            }
                                            transaction = transaction -> waitLock -> transaction;
                                        }
                                        lock -> transaction -> state = WAITING;
                                        lock -> transaction -> waitLock = tmpLock;

                                        pthread_mutex_unlock(&lockManager.lockManagerMutex);
                                        return CONFLICT;
                                    }

                                }

                            }
                        }
                        tmpLock = tmpLock -> prev;
                    }

                    // means waiting only this lock
                    // can acquire lock
                    lock -> transaction -> acquiredLocks.push_back(lock);
                    lock -> transaction -> state = RUNNING;
                    lock -> transaction -> waitLock = NULL;

                    lock -> acquired = true;
  
                    pthread_mutex_unlock(&lockManager.lockManagerMutex);
                    return LOCKSUCCESS;
                } else {
                    // waitlock's mode is S mode (this lock mode is EXCLUSIVE)
                    

                    tmpLock = lock -> prev;

                    while (tmpLock != NULL) {
                        if (tmpLock -> tableId == tableId && tmpLock -> key == key) {
                            // case 1: waitlock was sharing SHARED lock with other trx's SHARED lock, can't acquire lock

                            // check deadlock
                            transaction = tmpLock -> transaction ;
                            while (transaction -> state == WAITING) {
                                if (transaction -> waitLock -> transaction -> id == transactionId) {
                                    // not acquired, but push to acquiredlock list
                                    // to signal waiting this lock when aborting this transaction.
                                    lock -> transaction -> acquiredLocks.push_back(lock);
                                    
                                    pthread_mutex_unlock(&lockManager.lockManagerMutex);
                                    return DEADLOCK;
                                }
                                transaction = transaction -> waitLock -> transaction;
                            }

                            lock -> transaction -> state = WAITING;
                            lock -> transaction -> waitLock = tmpLock;

                            pthread_mutex_unlock(&lockManager.lockManagerMutex);
                            return CONFLICT;
                        }

                        tmpLock = tmpLock -> prev;
                    }

                    // case 2: waitlock was only one , can acquire lock
                    
                    lock -> transaction -> acquiredLocks.push_back(lock);
                    lock -> transaction -> state = RUNNING;
                    lock -> transaction -> waitLock = NULL;
                    lock -> acquired = true;

                    pthread_mutex_unlock(&lockManager.lockManagerMutex);
                    return LOCKSUCCESS;
                }
            }
        }
        lock = lock -> prev;
    }
    lock = tail;
    
    
    // check conflict
    while (lock != NULL) {
        if (lock -> tableId == tableId && lock -> key == key){

            if (lock -> mode == SHARED && mode == SHARED) {
                if (lock -> acquired == true) {

                    // shared lock is acquired by othre transaction,
                    // can acquire record lock of this transaction.
                    newLock = (lock_t*)malloc(sizeof(struct lock_t));
                    newLock -> transaction = &transactionManager.transactionTable[transactionId];
                    newLock -> transaction -> acquiredLocks.push_back(newLock);

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

                    pthread_mutex_unlock(&lockManager.lockManagerMutex);
                    return LOCKSUCCESS;
                } else {
                    // lock is waiting(means lock's waitlock is exclusive), can share waiting lock.

                    // check deadlock 
                    transaction = lock -> transaction -> waitLock -> transaction;
                    while (transaction -> state == WAITING) {
                        if (transaction -> waitLock -> transaction -> id == transactionId) {
                            // cycle will be created if we insert this lock
                            pthread_mutex_unlock(&lockManager.lockManagerMutex);
                            return DEADLOCK;
                        }
                        transaction = transaction -> waitLock -> transaction;
                    }

                    // not DEADLOCK , just CONFLICT
                    newLock = (lock_t*)malloc(sizeof(struct lock_t));

                    newLock -> transaction = &transactionManager.transactionTable[transactionId];

                    newLock -> transaction -> state = WAITING;

                    if (lock -> transaction -> waitLock == NULL) {
                        // error
                        printf("ERROR: lock -> transaction -> waitLock == NULL\n");
                        exit(0);

                    }
                    newLock -> transaction -> waitLock = lock -> transaction -> waitLock;

                    newLock -> tableId = tableId;
                    newLock -> pageNum = pageNum;
                    newLock -> key = key;
                    newLock -> acquired = false;
                    newLock -> mode = mode;
                    
                    // push back into lock list
                    newLock -> prev = lockManager.lockTable[pageNum].second;
                    lockManager.lockTable[pageNum].second -> next = newLock;
                    lockManager.lockTable[pageNum].second = newLock;
                    newLock -> next = NULL;

                    pthread_mutex_unlock(&lockManager.lockManagerMutex);
                    return CONFLICT;

                }
            }

            // check DEADLOCK, following wait-for graph
            transaction = lock -> transaction;
            while (transaction -> state == WAITING) {
                if (transaction -> waitLock -> transaction -> id == transactionId) {
                    // cycle will be created if we insert this lock
                    pthread_mutex_unlock(&lockManager.lockManagerMutex);
                    return DEADLOCK;
                }
                transaction = transaction -> waitLock -> transaction;
            }

            // not DEADLOCK , just CONFLICT
            newLock = (lock_t*)malloc(sizeof(struct lock_t));

            newLock -> transaction = &transactionManager.transactionTable[transactionId];

            newLock -> transaction -> state = WAITING;
            newLock -> transaction -> waitLock = lock;

            newLock -> tableId = tableId;
            newLock -> pageNum = pageNum;
            newLock -> key = key;
            newLock -> acquired = false;
            newLock -> mode = mode;
            
            // push back into lock list
            newLock -> prev = lockManager.lockTable[pageNum].second;
            lockManager.lockTable[pageNum].second -> next = newLock;
            lockManager.lockTable[pageNum].second = newLock;
            newLock -> next = NULL;

            pthread_mutex_unlock(&lockManager.lockManagerMutex);
            return CONFLICT;
        }
        lock = lock -> prev;
    }
    
    // lock list exist && not in trx's acquired list && lock on the record not exist in the list
    // can lock the record

    newLock = (lock_t *)malloc(sizeof(struct lock_t));
    newLock -> transaction = &transactionManager.transactionTable[transactionId];
    transactionManager.transactionTable[transactionId].acquiredLocks.push_back(newLock);
    newLock -> tableId = tableId;
    newLock -> pageNum = pageNum;
    newLock -> key = key;
    newLock -> acquired = true;
    newLock -> mode = mode;

    // push back into lock list
    newLock -> prev = lockManager.lockTable[pageNum].second;
    lockManager.lockTable[pageNum].second -> next = newLock;
    lockManager.lockTable[pageNum].second = newLock;
    newLock -> next = NULL;

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

            if (bufferLockBufferPage(rit -> tableId, pageNum) == FAIL) {
                pthread_mutex_unlock(&bufferPoolMutex);
                continue;
            }

            pthread_mutex_unlock(&bufferPoolMutex);

            //rollback
            page = bufferRequestPage(rit -> tableId, pageNum);
            
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