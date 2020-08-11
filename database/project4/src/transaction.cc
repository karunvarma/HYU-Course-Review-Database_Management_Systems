// __sync_fetch_and_add(&transactionId, 1);

#include <stdlib.h>
#include "../include/transaction.h"
#include <pthread.h>

volatile int transactionId = 0;

transactionManager_t transactionManager = {
    transactionId,
    PTHREAD_MUTEX_INITIALIZER
};

lockManager_t lockManager = {
    std::unordered_map<int, transaction_t> transactionTable,
    PTHREAD_MUTEX_INITIALIZER
};


int begin_trx() {
// 1 Acquire the transaction system latch.
// 2 Get a new transaction id.
// 3 Insert a new transaction structure in the transaction table. 
// 4 Release the transaction system latch.
// 5 Return the new transaction id.

}


int end_trx(int tid);