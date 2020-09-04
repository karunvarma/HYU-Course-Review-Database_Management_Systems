#include "../include/index.h"


// Insert input ‘key/value’ (record) to data file at the right place.
// If success, return 0. Otherwise, return non-zero value.
int db_insert(int tableId, int64_t key, char * value) {
    page_t* page;
    pagenum_t leafPageNum;
    char* tmp = (char*)malloc(sizeof(char) * 120);

    //set fd
    fd = bufferGetFdOfTable(tableId);

    //check if key is already in the tree
    if (db_find(tableId, key, tmp) == SUCCESS) {
        printf("[ERROR]: key is already in the tree\n");
        printf("[ERROR]: db_insert fail\n");
        free(tmp);
        return FAIL;
    }
    free(tmp);
    
    page = bufferRequestPage(tableId, HEADERPAGENUM);

    //there's no rootpage
    if (((headerPage_t*)page) -> rootPageNum == 0) {
        bufferUnpinPage(tableId, HEADERPAGENUM);
        return startNewTree(tableId, key, value);
    }

    bufferUnpinPage(tableId, HEADERPAGENUM);

    leafPageNum = findLeaf(tableId, key);

    page = bufferRequestPage(tableId, leafPageNum);


    if (((leafPage_t*)page) -> numOfKeys < LEAF_ORDER - 1) {
        bufferUnpinPage(tableId, leafPageNum);
        return insertIntoLeaf(tableId, leafPageNum, key, value);
    }



    bufferUnpinPage(tableId,leafPageNum);
    return insertIntoLeafAfterSplitting(tableId, leafPageNum, key, value);
}

int startNewTree(int tableId, int64_t key, char* value) {
    page_t* page, * header;
    pagenum_t pageNum;

    pageNum = bufferAllocPage(tableId);
    page = bufferRequestPage(tableId, pageNum);
    ((leafPage_t*)page) -> parentPageNum = 0;
    ((leafPage_t*)page) -> isLeaf = 1;
    ((leafPage_t*)page) -> numOfKeys = 1;
    ((leafPage_t*)page) -> rightSiblingPageNum = 0;
    ((leafPage_t*)page) -> record[0].key = key;
    strcpy(((leafPage_t*)page) -> record[0].value, value);
    bufferMakeDirty(tableId, pageNum);
    bufferUnpinPage(tableId, pageNum);
    
    header = bufferRequestPage(tableId, HEADERPAGENUM);
    ((headerPage_t*)header) -> rootPageNum = pageNum;
    bufferMakeDirty(tableId, HEADERPAGENUM);
    bufferUnpinPage(tableId, HEADERPAGENUM);

    return SUCCESS;
}

int insertIntoLeaf(int tableId, pagenum_t pageNum, int64_t key, char* value) {
    int i, insertionPoint;
    page_t* page;
    page = bufferRequestPage(tableId, pageNum);

    insertionPoint = 0;
    while (insertionPoint < ((leafPage_t*)page) -> numOfKeys 
                         && ((leafPage_t*)page) -> record[insertionPoint].key < key) {
        insertionPoint++;
    }

    for (i = ((leafPage_t*)page) -> numOfKeys; i > insertionPoint; i--) {
        ((leafPage_t*)page) -> record[i].key = ((leafPage_t*)page) -> record[i - 1].key;
        strcpy(((leafPage_t*)page) -> record[i].value, ((leafPage_t*)page) -> record[i - 1].value);
    }
    ((leafPage_t*)page) -> record[insertionPoint].key = key;
    strcpy(((leafPage_t*)page) -> record[insertionPoint].value, value);
    (((leafPage_t*)page) -> numOfKeys)++;

    bufferMakeDirty(tableId, pageNum);
    bufferUnpinPage(tableId, pageNum);
    return SUCCESS;
}

int insertIntoLeafAfterSplitting(int tableId, pagenum_t oldLeafPageNum, int64_t key, char* value) {

    page_t* newLeaf, * oldLeaf;
    pagenum_t newLeafPageNum;
    leafRecord_t* temporaryRecord;
    int insertionIndex, split, i, j;
    int64_t newKey;

    temporaryRecord = (leafRecord_t*)malloc(sizeof(struct leafRecord_t) * LEAF_ORDER);


    oldLeaf = bufferRequestPage(tableId, oldLeafPageNum);

    insertionIndex = 0;
    while (insertionIndex < LEAF_ORDER - 1 && ((leafPage_t*)oldLeaf) -> record[insertionIndex].key < key) {
        insertionIndex++;
    }
    for (i = 0, j = 0; i < ((leafPage_t*)oldLeaf) -> numOfKeys; i++, j++) {
        if (j == insertionIndex) j++;
        temporaryRecord[j].key = ((leafPage_t*)oldLeaf) ->record[i].key;
        strcpy(temporaryRecord[j].value, ((leafPage_t*)oldLeaf) ->record[i].value);
    }

    temporaryRecord[insertionIndex].key = key;
    strcpy(temporaryRecord[insertionIndex].value, value);

    ((leafPage_t*)oldLeaf) -> numOfKeys = 0;
    split = cut(LEAF_ORDER - 1);

    for (i = 0; i < split; i++) {
        ((leafPage_t*)oldLeaf) -> record[i].key = temporaryRecord[i].key;
        strcpy(((leafPage_t*)oldLeaf) -> record[i].value, temporaryRecord[i].value);
        (((leafPage_t*)oldLeaf) -> numOfKeys)++;
    }

    newLeafPageNum = bufferAllocPage(tableId);
    newLeaf = bufferRequestPage(tableId, newLeafPageNum);
    ((leafPage_t*)newLeaf) -> parentPageNum = 0;
    ((leafPage_t*)newLeaf) -> isLeaf = 1;
    ((leafPage_t*)newLeaf) -> numOfKeys = 0;
    ((leafPage_t*)newLeaf) -> rightSiblingPageNum = 0;

    for (i = split, j = 0; i < LEAF_ORDER; i++, j++) {
        ((leafPage_t*)newLeaf) -> record[j].key = temporaryRecord[i].key;
        strcpy(((leafPage_t*)newLeaf) -> record[j].value, temporaryRecord[i].value);
        (((leafPage_t*)newLeaf) -> numOfKeys)++;
    }

    free(temporaryRecord);

    ((leafPage_t*)newLeaf) -> rightSiblingPageNum = ((leafPage_t*)oldLeaf) -> rightSiblingPageNum;
    ((leafPage_t*)oldLeaf) -> rightSiblingPageNum = newLeafPageNum;

    ((leafPage_t*)newLeaf) -> parentPageNum = ((leafPage_t*)oldLeaf) -> parentPageNum;
    newKey = ((leafPage_t*)newLeaf) -> record[0].key;

    bufferMakeDirty(tableId, oldLeafPageNum);
    bufferUnpinPage(tableId, oldLeafPageNum);
    bufferMakeDirty(tableId, newLeafPageNum);
    bufferUnpinPage(tableId, newLeafPageNum);
    return insertIntoParent(tableId, oldLeafPageNum, newKey, newLeafPageNum);
}

int cut( int length ) {
    if (length % 2 == 0)
        return length/2;
    else
        return length/2 + 1;
}

int insertIntoParent(int tableId, pagenum_t leftChildPageNum, int64_t newKey, pagenum_t rightChildPageNum) {

    int leftIndex;
    page_t* leftChild, * parent;
    pagenum_t parentPageNum;

    leftChild = bufferRequestPage(tableId, leftChildPageNum);
    parentPageNum = ((leafPage_t*)leftChild) -> parentPageNum;
    bufferUnpinPage(tableId, leftChildPageNum);
    //leftChild was rootPage
    if (parentPageNum == HEADERPAGENUM) {
        return insertIntoNewRoot(tableId, leftChildPageNum, newKey, rightChildPageNum);
    }
    leftIndex = getIndexOfLeft(tableId, parentPageNum, leftChildPageNum);

    parent = bufferRequestPage(tableId, parentPageNum);
    if (((internalPage_t*)parent) -> numOfKeys < INTERNAL_ORDER - 1) {
        bufferUnpinPage(tableId, parentPageNum);
        return insertIntoInternal(tableId, parentPageNum, leftIndex, newKey, rightChildPageNum);
    }
    
    bufferUnpinPage(tableId, parentPageNum);
    return insertIntoInternalAfterSplitting(tableId, parentPageNum, leftIndex, newKey, rightChildPageNum);

    return SUCCESS;

}

int insertIntoNewRoot(int tableId, pagenum_t leftLeafPageNum, int64_t newKey, pagenum_t rightLeafPageNum) {
    page_t* root, * leftLeaf, * rightLeaf, * header;
    pagenum_t rootPageNum;

    rootPageNum = bufferAllocPage(tableId);
    root = bufferRequestPage(tableId, rootPageNum);
    ((internalPage_t*)root) -> parentPageNum = HEADERPAGENUM;
    ((internalPage_t*)root) -> isLeaf = 0;
    ((internalPage_t*)root) -> numOfKeys = 0;
    (((internalPage_t*)root) -> numOfKeys)++;
    ((internalPage_t*)root) -> leftMostPageNum = leftLeafPageNum;
    ((internalPage_t*)root) -> record[0].key = newKey;
    ((internalPage_t*)root) -> record[0].pageNum = rightLeafPageNum;
    bufferMakeDirty(tableId, rootPageNum);
    bufferUnpinPage(tableId, rootPageNum);
    

    leftLeaf = bufferRequestPage(tableId, leftLeafPageNum);
    ((leafPage_t*)leftLeaf) -> parentPageNum = rootPageNum;
    bufferMakeDirty(tableId, leftLeafPageNum);
    bufferUnpinPage(tableId, leftLeafPageNum);

    rightLeaf = bufferRequestPage(tableId, rightLeafPageNum);
    ((leafPage_t*)rightLeaf) -> parentPageNum = rootPageNum;
    bufferMakeDirty(tableId, rightLeafPageNum);
    bufferUnpinPage(tableId, rightLeafPageNum);
    

    header = bufferRequestPage(tableId, HEADERPAGENUM);
    ((headerPage_t*)header) -> rootPageNum = rootPageNum;
    bufferMakeDirty(tableId, HEADERPAGENUM);
    bufferUnpinPage(tableId, HEADERPAGENUM);
    return SUCCESS;
}

// find index of child
// if child is left most page , return - 1
int getIndexOfLeft(int tableId, pagenum_t parentPageNum, pagenum_t childPageNum) {
    int indexOfLeftChild = 0;
    page_t* parent;

    parent = bufferRequestPage(tableId, parentPageNum);

    if (((internalPage_t*)parent) -> leftMostPageNum == childPageNum) {
        bufferUnpinPage(tableId, parentPageNum);
        return -1;
    }
    while (indexOfLeftChild <= ((internalPage_t*)parent) -> numOfKeys 
           && ((internalPage_t*)parent) -> record[indexOfLeftChild].pageNum != childPageNum) {
               indexOfLeftChild++;
           }
    
    bufferUnpinPage(tableId, parentPageNum);
    return indexOfLeftChild;
}
int insertIntoInternal(int tableId, pagenum_t parentPageNum, int leftChildIndex, int64_t newKey, pagenum_t rightChildPageNum) {
    int i;
    page_t* parent;
    parent = bufferRequestPage(tableId,parentPageNum);
    
    for (i = (((internalPage_t*)parent) -> numOfKeys) - 1; i > leftChildIndex; i--) {
        ((internalPage_t*)parent) -> record[i + 1].key = ((internalPage_t*)parent) -> record[i].key;
        ((internalPage_t*)parent) -> record[i + 1].pageNum = ((internalPage_t*)parent) -> record[i].pageNum; 
    }
    ((internalPage_t*)parent) -> record[leftChildIndex + 1].key = newKey;
    ((internalPage_t*)parent) -> record[leftChildIndex + 1].pageNum = rightChildPageNum;
    (((internalPage_t*)parent) -> numOfKeys)++;

    bufferMakeDirty(tableId, parentPageNum);
    bufferUnpinPage(tableId, parentPageNum);
    return SUCCESS;
}

// insert new key in the parent,
int insertIntoInternalAfterSplitting(int tableId, pagenum_t parentPageNum, int leftChildIndex, int64_t newKey, pagenum_t rightChildPageNum) {
    internalRecord_t* temporaryRecord;
    page_t* parent, * rightParent, * child;
    pagenum_t rightParentPageNum, grandParentPageNum, childPageNum;

    int i, j, split;
    int64_t kPrime;

    temporaryRecord = (internalRecord_t*)malloc(sizeof(struct internalRecord_t) * INTERNAL_ORDER);

    parent = bufferRequestPage(tableId, parentPageNum);
    grandParentPageNum = ((internalPage_t*)parent) -> parentPageNum;
    for(i = 0, j = 0; i < ((internalPage_t*)parent) -> numOfKeys; i++, j++) {
        if (j == leftChildIndex + 1) j++;
        temporaryRecord[j].key = ((internalPage_t*)parent) -> record[i].key;
        temporaryRecord[j].pageNum = ((internalPage_t*)parent) -> record[i].pageNum;
    }
    temporaryRecord[leftChildIndex + 1].key = newKey;
    temporaryRecord[leftChildIndex + 1].pageNum =  rightChildPageNum;

    split = cut(INTERNAL_ORDER);
    ((internalPage_t*)parent) -> numOfKeys = 0;
    for (i = 0; i < split - 1; i++){
        ((internalPage_t*)parent) -> record[i].key = temporaryRecord[i].key;
        ((internalPage_t*)parent) -> record[i].pageNum = temporaryRecord[i].pageNum;
        (((internalPage_t*)parent) -> numOfKeys)++;
    }
    bufferMakeDirty(tableId, parentPageNum);
    bufferUnpinPage(tableId, parentPageNum);

    rightParentPageNum = bufferAllocPage(tableId);
    rightParent = bufferRequestPage(tableId, rightParentPageNum);
    ((internalPage_t*)rightParent) -> parentPageNum = 0;
    ((internalPage_t*)rightParent) -> isLeaf = 0;
    ((internalPage_t*)rightParent) -> numOfKeys = 0;
    ((internalPage_t*)rightParent) -> leftMostPageNum = temporaryRecord[i].pageNum;
    kPrime = temporaryRecord[i].key;
    for(++i, j = 0; i < INTERNAL_ORDER; i++, j++) {
        ((internalPage_t*)rightParent) -> record[j].key = temporaryRecord[i].key;
        ((internalPage_t*)rightParent) -> record[j].pageNum = temporaryRecord[i].pageNum;
        (((internalPage_t*)rightParent) -> numOfKeys)++;
    }
    free(temporaryRecord);
    ((internalPage_t*)rightParent) -> parentPageNum = grandParentPageNum;

    childPageNum = ((internalPage_t*)rightParent) -> leftMostPageNum;
    child = bufferRequestPage(tableId, childPageNum);
    ((internalPage_t*)child) -> parentPageNum = rightParentPageNum;
    bufferMakeDirty(tableId, childPageNum);
    bufferUnpinPage(tableId, childPageNum);
    for (i = 0; i < ((internalPage_t*)rightParent) -> numOfKeys; i++) {
        childPageNum = ((internalPage_t*)rightParent) -> record[i].pageNum;
        child = bufferRequestPage(tableId, childPageNum);
        ((internalPage_t*)child) -> parentPageNum = rightParentPageNum;
        bufferMakeDirty(tableId, childPageNum);
        bufferUnpinPage(tableId, childPageNum);
    }

    bufferMakeDirty(tableId, rightParentPageNum);
    bufferUnpinPage(tableId, rightParentPageNum);
    return insertIntoParent(tableId, parentPageNum, kPrime, rightParentPageNum);


}

// Find the record containing input ‘key’.
// • If found matching ‘key’, store matched ‘value’ string in ret_val and return 0. Otherwise, return
// non-zero value.
int db_find(int tableId, int64_t key, char * ret_val) {
    pagenum_t pageNum;
    int i = 0;
    fd = bufferGetFdOfTable(tableId);
    pageNum = findLeaf(tableId, key);
    if (pageNum == 0) {
        return FAIL;
    }
    page_t* page = bufferRequestPage(tableId, pageNum);

    while (i < ((leafPage_t*)page) -> numOfKeys) {
        if (((leafPage_t*)page) -> record[i].key == key) {
            break;
        }
        i++;
    }

    if (i == ((leafPage_t*)page) -> numOfKeys) {
        bufferUnpinPage(tableId, pageNum);
        return FAIL; // fail
    } else {
        strcpy(ret_val, ((leafPage_t*)page) -> record[i].value);
        bufferUnpinPage(tableId, pageNum);
        return SUCCESS; // success
    }
}


pagenum_t findLeaf(int tableId, int64_t key) {
    page_t* page, * header;
    pagenum_t leafPageNum, rootPageNum, prevLeafPageNum;
    int i = 0;

    header = bufferRequestPage(tableId, HEADERPAGENUM);
    rootPageNum = ((headerPage_t*)header) -> rootPageNum;
    bufferUnpinPage(tableId, HEADERPAGENUM);
    if (rootPageNum == 0) {
        return 0;
    }

    page = bufferRequestPage(tableId, rootPageNum);
    leafPageNum = rootPageNum;
    while (!((internalPage_t*)page) -> isLeaf) {
        i = 0;
        while (i < ((internalPage_t*)page) -> numOfKeys) {
            if (key < ((internalPage_t*)page) -> record[i].key) {
                break;
            } else {
                i++;
            }
        }
        prevLeafPageNum = leafPageNum;
        if (i == 0) {
            leafPageNum = ((internalPage_t*)page) -> leftMostPageNum;
        } else {
            leafPageNum = ((internalPage_t*)page) -> record[i - 1].pageNum;
        }
        bufferUnpinPage(tableId, prevLeafPageNum);
        page = bufferRequestPage(tableId, leafPageNum);
    }
    bufferUnpinPage(tableId, leafPageNum);
    return leafPageNum;
}

// delete
// Find the matching record and delete it if found.
// If success, return 0. Otherwise, return non-zero value.
int db_delete(int tableId, int64_t key) {
    pagenum_t leafPageNum;
    char tmp[120];
    //set fd
    fd = bufferGetFdOfTable(tableId);
    if (db_find(tableId, key, tmp) == FAIL) {
        printf("[ERROR]: no %lld in the tree\n", key);
        printf("         db_delete(%lld) failed\n", key);
        return FAIL;
    } else {
        leafPageNum = findLeaf(tableId, key);
        return deleteEntry(tableId, leafPageNum, key);
    }
}

int deleteEntry(int tableId, pagenum_t pageNum, int64_t key) {
    page_t* parent, * page, * neighbor;
    int neighborIndex, kPrimeIndex, kPrime, capacity;
    pagenum_t neighborPageNum, parentPageNum;
    int pageIsLeaf;
    removeEntryFromPage(tableId, pageNum, key);

    page = bufferRequestPage(tableId, pageNum);
    parentPageNum = ((internalPage_t*)page) -> parentPageNum;
    pageIsLeaf = ((internalPage_t*)page) -> isLeaf;
    if (parentPageNum == 0) {
        bufferUnpinPage(tableId, pageNum);
        return adjustRoot(tableId, pageNum);
    }

    if (((internalPage_t*)page) -> numOfKeys > 0) {
        bufferUnpinPage(tableId, pageNum);
        return SUCCESS;
    }
    
    bufferUnpinPage(tableId, pageNum);
    neighborIndex = getNeighborIndex(tableId, pageNum);
    kPrimeIndex = neighborIndex == -2 ? 0 : neighborIndex + 1;

    parent = bufferRequestPage(tableId, parentPageNum);  
    kPrime = ((internalPage_t*)parent) -> record[kPrimeIndex].key;
    if (neighborIndex == - 2) {
        neighborPageNum = ((internalPage_t*)parent) -> record[kPrimeIndex].pageNum;
    } else if (kPrimeIndex == 0) {
        neighborPageNum = ((internalPage_t*)parent) -> leftMostPageNum;
    } else {
        neighborPageNum = ((internalPage_t*)parent) -> record[kPrimeIndex - 1].pageNum;
    }
    bufferUnpinPage(tableId, parentPageNum);

    neighbor = bufferRequestPage(tableId, neighborPageNum);
    capacity = pageIsLeaf ? LEAF_ORDER : INTERNAL_ORDER - 1;

    if (((internalPage_t*)neighbor) -> numOfKeys + ((internalPage_t*)page) -> numOfKeys  < capacity) {
        bufferUnpinPage(tableId, neighborPageNum);
        return coalescePages(tableId, neighborPageNum, kPrime, pageNum, neighborIndex);
    } else {
        bufferUnpinPage(tableId, neighborPageNum);
        return redistributePages(tableId, neighborPageNum, kPrime, pageNum, neighborIndex, kPrimeIndex);
    } 

    return SUCCESS; 
}

void removeEntryFromPage(int tableId, pagenum_t pageNum, int64_t key) {
    page_t* page;
    int i;
    i = 0;

    page = bufferRequestPage(tableId, pageNum);
    if (((internalPage_t*)page) -> isLeaf) {
        while (((leafPage_t*)page) -> record[i].key != key) {
            i++;
        }
        for (++i; i < ((leafPage_t*)page) -> numOfKeys; i++) {
            ((leafPage_t*)page) -> record[i - 1].key = ((leafPage_t*)page) -> record[i].key;
            strcpy(((leafPage_t*)page) -> record[i - 1].value, ((leafPage_t*)page) -> record[i].value);
        }
    } else {
        while (((internalPage_t*)page) -> record[i].key != key) {
            i++;
        }
        for (++i; i < ((internalPage_t*)page) -> numOfKeys; i++) {
            ((internalPage_t*)page) -> record[i - 1].key = ((internalPage_t*)page) -> record[i].key;
            ((internalPage_t*)page) -> record[i - 1].pageNum = ((internalPage_t*)page) -> record[i].pageNum;
        }
    }
    (((internalPage_t*)page) -> numOfKeys)--;
    bufferMakeDirty(tableId, pageNum);
    bufferUnpinPage(tableId, pageNum);
}

int adjustRoot(int tableId, pagenum_t pageNum) {
    page_t* page, * header, * child;
    pagenum_t childPageNum;

    page = bufferRequestPage(tableId, pageNum);

    if (((internalPage_t*)page) -> numOfKeys > 0) {
        bufferUnpinPage(tableId,pageNum);
        return SUCCESS;
    }

    if (((internalPage_t*)page) -> isLeaf) {
        bufferUnpinPage(tableId,pageNum);
        header = bufferRequestPage(tableId, HEADERPAGENUM);
        ((headerPage_t*)header) -> rootPageNum = 0;
        bufferMakeDirty(tableId, HEADERPAGENUM);
        bufferUnpinPage(tableId, HEADERPAGENUM);
        bufferFreePage(tableId,pageNum);
        return SUCCESS;
    } else {
        childPageNum = ((internalPage_t*)page) -> leftMostPageNum;
        bufferUnpinPage(tableId,pageNum);
        bufferFreePage(tableId, pageNum);

        header = bufferRequestPage(tableId, HEADERPAGENUM);
        ((headerPage_t*)header) -> rootPageNum = ((internalPage_t*)page) -> leftMostPageNum;
        bufferMakeDirty(tableId, HEADERPAGENUM);
        bufferUnpinPage(tableId, HEADERPAGENUM);

        child = bufferRequestPage(tableId, childPageNum);
        ((internalPage_t*)child) -> parentPageNum = 0;
        bufferMakeDirty(tableId, childPageNum);
        bufferUnpinPage(tableId, childPageNum);
        return SUCCESS;
    }
}

// return pageNum's left one's index at parent,  if page is leftmost return - 2
int getNeighborIndex(int tableId, pagenum_t pageNum) {
    pagenum_t parentPageNum;
    page_t* parent, * page;
    int i;


    page = bufferRequestPage(tableId, pageNum);
    parentPageNum = ((internalPage_t*)page) -> parentPageNum;
    bufferUnpinPage(tableId, pageNum);

    parent = bufferRequestPage(tableId, parentPageNum);
    if (((internalPage_t*)parent) -> leftMostPageNum == pageNum) {
        bufferUnpinPage(tableId, parentPageNum);
        return -2;
    }

    for (i = 0; i < ((internalPage_t*)parent) -> numOfKeys; i++) {
        if (((internalPage_t*)parent) -> record[i].pageNum == pageNum) {
            bufferUnpinPage(tableId, parentPageNum);
            return i - 1;
        }
    }



    printf("[ERROR]: getNeighborIndex fail");
    bufferUnpinPage(tableId, parentPageNum);
    return -3;

}

//move right keys to left
int coalescePages(int tableId, pagenum_t neighborPageNum , int64_t kPrime, pagenum_t pageNum, int neighborIndex) {
    pagenum_t leftPageNum, rightPageNum, parentPageNum, childPageNum;
    page_t* left, * right, * child;
    int leftInsertionIndex, rightNumOfKeys;
    int i,j;


    if (neighborIndex == -2) {
        leftPageNum = pageNum;
        rightPageNum = neighborPageNum; 
    } else {
        leftPageNum = neighborPageNum;
        rightPageNum = pageNum;
    }

    left = bufferRequestPage(tableId, leftPageNum);
    right = bufferRequestPage(tableId, rightPageNum);

    parentPageNum = ((internalPage_t*)left) -> parentPageNum;
    leftInsertionIndex = ((internalPage_t*)left) -> numOfKeys;
    rightNumOfKeys = ((internalPage_t*)right) -> numOfKeys;
    if(((internalPage_t*)left) -> isLeaf) {
        for (i = leftInsertionIndex, j = 0; j < rightNumOfKeys; i++, j++) {
            ((leafPage_t*)left) ->  record[i].key = ((leafPage_t*)right) -> record[j].key;
            strcpy(((leafPage_t*)left) -> record[i].value, ((leafPage_t*)right) -> record[j].value);
            (((leafPage_t*)left) -> numOfKeys)++;
            (((leafPage_t*)right) -> numOfKeys)--;
        }
        ((leafPage_t*)left) -> rightSiblingPageNum = ((leafPage_t*)right) -> rightSiblingPageNum;
        bufferMakeDirty(tableId, rightPageNum);
        bufferUnpinPage(tableId, rightPageNum);
    } else {
        ((internalPage_t*)left) -> record[leftInsertionIndex].key = kPrime;
        ((internalPage_t*)left) -> record[leftInsertionIndex].pageNum = ((internalPage_t*)right) -> leftMostPageNum;
        (((internalPage_t*)left) -> numOfKeys)++;


        for (i = leftInsertionIndex + 1, j = 0; j < rightNumOfKeys; i++, j++) {
            ((internalPage_t*)left) -> record[i].key = ((internalPage_t*)right) -> record[j].key;
            ((internalPage_t*)left) -> record[i].pageNum = ((internalPage_t*)right) -> record[j].pageNum;
            (((internalPage_t*)left) -> numOfKeys)++;
            (((internalPage_t*)right) -> numOfKeys)--;
        }
        bufferMakeDirty(tableId, rightPageNum);
        bufferUnpinPage(tableId, rightPageNum);

        for (i = 0; i < ((internalPage_t*)left) -> numOfKeys; i++) {
            childPageNum = ((internalPage_t*)left) -> record[i].pageNum;
            child = bufferRequestPage(tableId, childPageNum);
            ((internalPage_t*)child) -> parentPageNum = leftPageNum;
            bufferMakeDirty(tableId, childPageNum);
            bufferUnpinPage(tableId, childPageNum);
        }
    }


    bufferMakeDirty(tableId, leftPageNum);
    bufferUnpinPage(tableId, leftPageNum);
    
    bufferFreePage(tableId, rightPageNum);

    if (parentPageNum == 0) {
        //should not come in here
        return SUCCESS;
    }
    return deleteEntry(tableId, parentPageNum, kPrime);
}



int redistributePages(int tableId, pagenum_t neighborPageNum, int64_t kPrime, pagenum_t pageNum, int neighborIndex, int kPrimeIndex) {
    // execute only when page is internal

    int i;
    int64_t newKPrime;
    page_t* child, * neighbor, * page, * parent;
    pagenum_t childPageNum, parentPageNum;

    // TODO: check  page's numkey == 0
    // if (((internalPage_t*)page) -> numOfKeys != 0) {
    //     printf("something's wrong \n");
    //     return FAIL;
    // }
    page = bufferRequestPage(tableId, pageNum);
    neighbor = bufferRequestPage(tableId, neighborPageNum);

    // move one from neighbor to page 
    if (neighborIndex == -2) {
        // neighbor is right child, page is leftmostchild

        ((internalPage_t*)page) -> record[((internalPage_t*)page) -> numOfKeys].key = kPrime;
        ((internalPage_t*)page) -> record[((internalPage_t*)page) -> numOfKeys].pageNum = ((internalPage_t*)neighbor) -> leftMostPageNum;
        ((internalPage_t*)neighbor) -> leftMostPageNum = ((internalPage_t*)neighbor) -> record[0].pageNum;
        childPageNum = ((internalPage_t*)page) -> record[((internalPage_t*)page) -> numOfKeys].pageNum;
        parentPageNum = ((internalPage_t*)page) -> parentPageNum;
        newKPrime = ((internalPage_t*)neighbor) -> record[0].key;

        for (i = 0; i < ((internalPage_t*)neighbor) -> numOfKeys; i++) {
            ((internalPage_t*)neighbor) -> record[i].key = ((internalPage_t*)neighbor) -> record[i + 1].key;
            ((internalPage_t*)neighbor) -> record[i].pageNum = ((internalPage_t*)neighbor) -> record[i + 1].pageNum;
        }

    } else {
        // neighbor is leftChild, page is rightChild
        
        for(i = 0; i  < ((internalPage_t*)page) -> numOfKeys; i++) {
            ((internalPage_t*)page) -> record[i + 1].key = ((internalPage_t*)page) -> record[i].key;
            ((internalPage_t*)page) -> record[i + 1].pageNum = ((internalPage_t*)page) -> record[i].pageNum;
        }
        ((internalPage_t*)page) -> record[0].key = kPrime;
        ((internalPage_t*)page) -> record[0].pageNum = ((internalPage_t*)page) -> leftMostPageNum;
        ((internalPage_t*)page) -> leftMostPageNum = ((internalPage_t*)neighbor) -> record[((internalPage_t*)neighbor) -> numOfKeys - 1].pageNum;
        childPageNum = ((internalPage_t*)page) -> leftMostPageNum;
        parentPageNum = ((internalPage_t*)page) -> parentPageNum;
        newKPrime = ((internalPage_t*)neighbor) -> record[((internalPage_t*)neighbor) -> numOfKeys - 1].key;
        
    } 
    (((internalPage_t*)page) -> numOfKeys)++;
    (((internalPage_t*)neighbor) -> numOfKeys)--;
    bufferMakeDirty(tableId, neighborPageNum);
    bufferUnpinPage(tableId, neighborPageNum);
    bufferMakeDirty(tableId, pageNum);
    bufferUnpinPage(tableId, pageNum);

    child = bufferRequestPage(tableId, childPageNum);
    ((internalPage_t*)child) -> parentPageNum = pageNum;
    bufferMakeDirty(tableId, childPageNum);
    bufferUnpinPage(tableId, childPageNum);

    parent = bufferRequestPage(tableId, parentPageNum);
    ((internalPage_t*)parent) -> record[kPrimeIndex].key = newKPrime;
    bufferMakeDirty(tableId, parentPageNum);
    bufferUnpinPage(tableId, parentPageNum);
    return SUCCESS;
}

// return execution number of open_table
// open first table: return 1 
// Open existing data file or create one if not existed. 
// You can give same table id when db opens same table more than once after init_db.
// • If success, return the unique table id, which represents 
// the own table in this database. (Return negative value if error occurs)
// • You have to maintain a table id once open_table() is called, 
// which is matching file descriptor or file pointer depending on your 
// previous implementation. (table id ≥ 1 and maximum
// allocated id is set to 10)
int open_table(char *pathname) {
    return bufferOpenTable(pathname);
} 

// Allocate the buffer pool (array) with the given number of entries.
// • Initialize other fields (state info, LRU info..) with your own design.
// • If success, return 0. Otherwise, return non-zero value.
int init_db(int buf_num) {
    return bufferInitDb(buf_num);
}

// Write all pages of this table from buffer to disk and discard the table id.
// • If success, return 0. Otherwise, return non-zero value.
int close_table(int table_id) {
    return bufferCloseTable(table_id);
}

// Flush all data from buffer and destroy allocated buffer.
// • If success, return 0. Otherwise, return non-zero value.
int shutdown_db() {
    return bufferShutDownDb();
}

// Because tree is already sorted by key, do sort-merge join
int join_table(int table_id_1, int table_id_2, char * pathname) {
    page_t* pageOfTable1, * pageOfTable2;
    pagenum_t prevPageNumOfTable1, pageNumOfTable1, prevPageNumOfTable2, pageNumOfTable2, markPageNum;

    int joinResultFd;
    if ((joinResultFd = open(pathname, O_RDWR | O_CREAT | O_EXCL, 0644)) > 0) {
    } else {
        joinResultFd = open(pathname, O_RDWR | O_TRUNC);
    }
    int i, j, iMark, jMark;
    pageNumOfTable1 = findLeaf(table_id_1, INT64_MIN);
    if (pageNumOfTable1 == 0) {
        //no leaf page at all
        close(joinResultFd);
        return FAIL;
    }
    pageOfTable1 = bufferRequestPage(table_id_1, pageNumOfTable1);
    pageNumOfTable2 = findLeaf(table_id_2, ((leafPage_t*)pageOfTable1) -> record[0].key);
    if (pageNumOfTable2 == 0) {
        bufferUnpinPage(table_id_1, pageNumOfTable1);
        close(joinResultFd);
        return FAIL;
    }
    pageOfTable2 = bufferRequestPage(table_id_2, pageNumOfTable2);
    int done = 0;
    i = 0;
    j = 0;

    //TODO: check tree is empty

    while (!done) {
        while (((leafPage_t*)pageOfTable1) -> record[i].key > ((leafPage_t*)pageOfTable2) -> record[j].key) {
            if (j == ((leafPage_t*)pageOfTable2) -> numOfKeys - 1) {
                j = 0;
                prevPageNumOfTable2 = pageNumOfTable2;
                pageNumOfTable2 = ((leafPage_t*)pageOfTable2) -> rightSiblingPageNum;
                bufferUnpinPage(table_id_2, prevPageNumOfTable2);
                if (pageNumOfTable2 == 0) {
                    bufferUnpinPage(table_id_1, pageNumOfTable1);
                    close(joinResultFd);
                    return SUCCESS;
                }
                pageOfTable2 = bufferRequestPage(table_id_2, pageNumOfTable2);
            } else {
                j++;
            }
        }
        while (((leafPage_t*)pageOfTable1) -> record[i].key < ((leafPage_t*)pageOfTable2) -> record[j].key) {
            if (i == ((leafPage_t*)pageOfTable1) -> numOfKeys - 1) {
                i = 0;
                prevPageNumOfTable1 = pageNumOfTable1;
                pageNumOfTable1 = ((leafPage_t*)pageOfTable1) -> rightSiblingPageNum;
                bufferUnpinPage(table_id_1, prevPageNumOfTable1);
                if (pageNumOfTable1 == 0) {
                    bufferUnpinPage(table_id_2, pageNumOfTable2);
                    close(joinResultFd);
                    return SUCCESS;
                }
                pageOfTable1 = bufferRequestPage(table_id_1, pageNumOfTable1);
            } else {
                i++;
            }
        }
        jMark = j;
        markPageNum = pageNumOfTable2;
        // below while loops are examined only once each, 
        // because key is unique in my tree
        while (((leafPage_t*)pageOfTable1) -> record[i].key == ((leafPage_t*)pageOfTable2) -> record[j].key) {
            while(((leafPage_t*)pageOfTable1) -> record[i].key == ((leafPage_t*)pageOfTable2) -> record[j].key) {
                std::string tmp = std::to_string(((leafPage_t*)pageOfTable1) -> record[i].key) + "," 
                + ((leafPage_t*)pageOfTable1) -> record[i].value + ","
                + std::to_string(((leafPage_t*)pageOfTable2) -> record[j].key) + ","
                + ((leafPage_t*)pageOfTable2) -> record[j].value + "\n";
                write(joinResultFd, (char*)tmp.c_str(), tmp.size());
                if (j == ((leafPage_t*)pageOfTable2) -> numOfKeys - 1) {
                    j = 0;
                    prevPageNumOfTable2 = pageNumOfTable2;
                    pageNumOfTable2 = ((leafPage_t*)pageOfTable2) -> rightSiblingPageNum;
                    bufferUnpinPage(table_id_2, prevPageNumOfTable2);
                    if (pageNumOfTable2 == 0) {
                        bufferUnpinPage(table_id_1, pageNumOfTable1);
                        close(joinResultFd);
                        return SUCCESS;
                    }
                    pageOfTable2 = bufferRequestPage(table_id_2, pageNumOfTable2);
                } else {
                    j++;
                }
            }

            j = jMark;
            if (pageNumOfTable2 != markPageNum) {
                bufferUnpinPage(table_id_2, pageNumOfTable2);
                pageNumOfTable2 = markPageNum;
                pageOfTable2 = bufferRequestPage(table_id_2, pageNumOfTable2);
            }

            if (i == ((leafPage_t*)pageOfTable1) -> numOfKeys - 1) {
                i = 0;
                prevPageNumOfTable1 = pageNumOfTable1;
                pageNumOfTable1 = ((leafPage_t*)pageOfTable1) -> rightSiblingPageNum;
                bufferUnpinPage(table_id_1, prevPageNumOfTable1);
                if (pageNumOfTable1 == 0) {
                    bufferUnpinPage(table_id_2, pageNumOfTable2);
                    close(joinResultFd);
                    return SUCCESS;
                }
                pageOfTable1 = bufferRequestPage(table_id_1, pageNumOfTable1);
            } else {
                i++;
            }

        }

    }
}

//overload
int db_find(int tableId, int64_t key, char* retVal, int transactionId) {
    // ① Acquire the buffer pool latch.
    // ② Find a leaf page containing the given record(key).
    // ③ Try to acquire the buffer page latch.
    // ① If fail to acquire, release the buffer pool latch and go to (1).
    // ④ Release the buffer pool latch.
    // ⑤ Try to acquire record lock.
    // ① If fail due to deadlock, abort transaction and release buffer page latch. Return FAIL.
    // ② If fail due to lock conflict, release the buffer page latch and wait(sleep) until another
    // thread wake me up. After waken up, go to (1).
    // ⑥ Do update / find.
    // ⑦ Release the buffer page latch.
    // ⑧ Return SUCCESS.
    if (transactionManager.transactionTable.find(transactionId) == transactionManager.transactionTable.end()) {
        // transaction doesn't exist in the table,
        // maybe aborted in previous job.
        return FAIL;
    }

    page_t* page;
    pagenum_t leafPageNum;
    bool done = false;
    int indexOfKey = 0;
    int ret;
    while (!done) {
        pthread_mutex_lock(&bufferPoolMutex);
        leafPageNum = findLeaf(tableId, key);

        // if no root page.
        if (leafPageNum == 0) {
            // not abort, just inform to client
            pthread_mutex_unlock(&bufferPoolMutex);
            printf("[ERROR]: findleaf = 0\n");
            return FAIL;
        }

        page = bufferRequestPage(tableId, leafPageNum);

        if (bufferLockBufferPage(tableId, leafPageNum) == FAIL) {
            // bufferUnpinPage(tableId, leafPageNum);
            pthread_mutex_unlock(&bufferPoolMutex);
            continue;
        }
        pthread_mutex_unlock(&bufferPoolMutex);

        indexOfKey = 0;
        while (indexOfKey < ((leafPage_t*)page) -> numOfKeys) {
            if (((leafPage_t*)page) -> record[indexOfKey].key == key) {
                break;
            }
            indexOfKey++;
        }

        // no key.
        if (indexOfKey == ((leafPage_t*)page) -> numOfKeys) {
            // inform to client, not abort
            printf("[ERROR]: indexOfKey == ((leafPage_t*)page) -> numOfKeys\n");
            bufferUnpinPage(tableId, leafPageNum);
            bufferUnlockBufferPage(tableId, leafPageNum);
            return FAIL;
        } 

        // acquire record lock
        ret = acquireRecordLock(tableId, leafPageNum, key, SHARED, transactionId);

        if (ret == LOCKSUCCESS) {
            
            strcpy(retVal, ((leafPage_t*)page) -> record[indexOfKey].value);
            bufferUnpinPage(tableId, leafPageNum);

            bufferUnlockBufferPage(tableId, leafPageNum);
            return SUCCESS;
        } else if (ret == CONFLICT) {
            
            bufferUnpinPage(tableId, leafPageNum);
            bufferUnlockBufferPage(tableId, leafPageNum);

            // release lockmanager mutex
            pthread_mutex_unlock(&lockManager.lockManagerMutex);
            // sleep wait
            transaction_t* transaction;
            // TODO: should lock transaction manager?
            transaction =  &transactionManager.transactionTable[transactionId];
            pthread_mutex_lock(&transaction -> transactionMutex);
            transaction -> state = WAITING;
            pthread_cond_wait(&transaction -> transactionCond, &transaction -> transactionMutex);
            transaction -> state = RUNNING;
            pthread_mutex_unlock(&transaction -> transactionMutex);

            // after wake up, return to first.
            continue;
        } else if (ret == DEADLOCK) {
            bufferUnpinPage(tableId, leafPageNum);
            bufferUnlockBufferPage(tableId, leafPageNum);

            // abort transaction,
            abortTransaction(transactionId);
            
            // return FAIL
            return FAIL;
        }

    }

    // should not come here
    return FAIL;
}

int db_update(int tableId, int64_t key, char* values, int transactionId) {

    if (transactionManager.transactionTable.find(transactionId) == transactionManager.transactionTable.end()) {
        // transaction doesn't exist in the table,
        // maybe aborted in previous job.
        return FAIL;
    }

    page_t* page;
    pagenum_t leafPageNum;
    bool done = false;
    int indexOfKey = 0;
    int ret;
    while (!done) {
        pthread_mutex_lock(&bufferPoolMutex);
        leafPageNum = findLeaf(tableId, key);

        // if no root page.
        if (leafPageNum == 0) {
            // not abort, just inform to client
            pthread_mutex_unlock(&bufferPoolMutex);
            printf("[ERROR]: findleaf = 0\n");
            return FAIL;
        }

        page = bufferRequestPage(tableId, leafPageNum);

        if (bufferLockBufferPage(tableId, leafPageNum) == FAIL) {

            bufferUnpinPage(tableId, leafPageNum);
            pthread_mutex_unlock(&bufferPoolMutex);

            continue;
        }
        pthread_mutex_unlock(&bufferPoolMutex);

        indexOfKey = 0;
        while (indexOfKey < ((leafPage_t*)page) -> numOfKeys) {
            if (((leafPage_t*)page) -> record[indexOfKey].key == key) {
                break;
            }
            indexOfKey++;
        }

        // no key.
        if (indexOfKey == ((leafPage_t*)page) -> numOfKeys) {
            // inform to client, not abort
            printf("[ERROR]: indexOfKey == ((leafPage_t*)page) -> numOfKeys\n");
            bufferUnpinPage(tableId, leafPageNum);
            bufferUnlockBufferPage(tableId, leafPageNum);
            return FAIL;
        } 

        
        // acquire record lock
        ret = acquireRecordLock(tableId, leafPageNum, key, EXCLUSIVE, transactionId);

        if (ret == LOCKSUCCESS) {
            // make log before update
            transaction_t* transaction = &transactionManager.transactionTable[transactionId];
            undoLog_t undoLog = {
                tableId,
                key,
            };
            strcpy(undoLog.oldValue, ((leafPage_t*)page) -> record[indexOfKey].value);
            transaction -> undoLogList.push_back(std::move(undoLog));
            // do update
            strcpy(((leafPage_t*)page) -> record[indexOfKey].value, values);
            bufferMakeDirty(tableId, leafPageNum);
            bufferUnpinPage(tableId, leafPageNum);

            bufferUnlockBufferPage(tableId, leafPageNum);
            return SUCCESS;
        } else if (ret == CONFLICT) {

            bufferUnpinPage(tableId, leafPageNum);
            bufferUnlockBufferPage(tableId, leafPageNum);

            // release lockmanager mutex
            pthread_mutex_unlock(&lockManager.lockManagerMutex);
            // sleep wait
            transaction_t* transaction;
            // TODO: should lock transaction manager?
            transaction =  &transactionManager.transactionTable[transactionId];
            pthread_mutex_lock(&transaction -> transactionMutex);
            transaction -> state = WAITING;
            pthread_cond_wait(&transaction -> transactionCond, &transaction -> transactionMutex);
            transaction -> state = RUNNING;
            pthread_mutex_unlock(&transaction -> transactionMutex);

            // after wake up, return to first step.
            continue;
        } else if (ret == DEADLOCK) {
            bufferUnpinPage(tableId, leafPageNum);
            bufferUnlockBufferPage(tableId, leafPageNum);

            // abort transaction,
            abortTransaction(transactionId);

            // return FAIL
            return FAIL;
        }

    }

    // should not come here
    return FAIL;
}
