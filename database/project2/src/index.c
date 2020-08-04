#include "index.h"
// GLOBALS.
int numOfTables = 0;

// return execution number of open_table
// open first table: return 1 
int open_table(char *pathname) {
    if (file_open_table(pathname) == 0) { //not exist
        //init table
        page_t* header = (page_t*)malloc(sizeof(struct page_t));
        ((headerPage_t*)header) -> rootPageNum = 0;
        ((headerPage_t*)header) -> numOfPages = 1;
        ((headerPage_t*)header) -> freePageNum = 0;
        file_write_page(HEADERPAGENUM, header);
        free(header);
    }
    numOfTables++;
    return numOfTables;
} 

int startNewTree(int64_t key, char* value) {
    page_t* page = (page_t*)malloc(sizeof(struct page_t));
    page_t* header = (page_t*)malloc(sizeof(struct page_t));
    pagenum_t pageNum;

    ((leafPage_t*)page) -> parentPageNum = 0;
    ((leafPage_t*)page) -> isLeaf = 1;
    ((leafPage_t*)page) -> numOfKeys = 1;
    ((leafPage_t*)page) -> rightSiblingPageNum = 0;
    ((leafPage_t*)page) -> record[0].key = key;
    strcpy(((leafPage_t*)page) -> record[0].value, value);
    pageNum = file_alloc_page();
    file_read_page(HEADERPAGENUM, header);
    ((headerPage_t*)header) -> rootPageNum = pageNum;
    file_write_page(HEADERPAGENUM, header);
    file_write_page(pageNum, page);

    free(page);

    return SUCCESS;
}

int insertIntoLeaf(pagenum_t leafPageNum, int64_t key, char* value) {
    int i, insertionPoint;
    page_t* page = (page_t*)malloc(sizeof(struct page_t));
    file_read_page(leafPageNum, page);

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
    file_write_page(leafPageNum, page);
    free(page);
    return SUCCESS;
}

int cut( int length ) {
    if (length % 2 == 0)
        return length/2;
    else
        return length/2 + 1;
}

// make root that has left leaf as leftmost child, right leaf as first child
int insertIntoNewRoot(pagenum_t leftLeafPageNum, int64_t newKey, pagenum_t rightLeafPageNum) {
    page_t* root = (page_t*)malloc(sizeof(struct page_t));
    page_t* leftLeaf = (page_t*)malloc(sizeof(struct page_t));
    page_t* rightLeaf = (page_t*)malloc(sizeof(struct page_t));
    page_t* header = (page_t*)malloc(sizeof(struct page_t));
    pagenum_t rootPageNum;

    rootPageNum = file_alloc_page();
    file_read_page(HEADERPAGENUM, header);
    file_read_page(leftLeafPageNum, leftLeaf);
    file_read_page(rightLeafPageNum, rightLeaf);

    ((headerPage_t*)header) -> rootPageNum = rootPageNum;
    ((internalPage_t*)root) -> parentPageNum = HEADERPAGENUM;
    ((internalPage_t*)root) -> isLeaf = 0;
    ((internalPage_t*)root) -> numOfKeys = 0;
    (((internalPage_t*)root) -> numOfKeys)++;
    ((internalPage_t*)root) -> leftMostPageNum = leftLeafPageNum;
    ((internalPage_t*)root) -> record[0].key = newKey;
    ((internalPage_t*)root) -> record[0].pageNum = rightLeafPageNum;
    ((leafPage_t*)leftLeaf) -> parentPageNum = rootPageNum;
    ((leafPage_t*)rightLeaf) -> parentPageNum = rootPageNum;

    file_write_page(HEADERPAGENUM, header);
    file_write_page(leftLeafPageNum, leftLeaf);
    file_write_page(rightLeafPageNum, rightLeaf);
    file_write_page(rootPageNum, root);

    free(header);
    free(root);
    free(leftLeaf);
    free(rightLeaf);
    return SUCCESS;
}

// find index of child
// if child is left most page , return - 1
int getIndexOfLeft(pagenum_t parentPageNum, pagenum_t childPageNum) {
    int indexOfLeftChild = 0;
    page_t* parent = (page_t*)malloc(sizeof(struct page_t));
    file_read_page(parentPageNum, parent);

    if (((internalPage_t*)parent) -> leftMostPageNum == childPageNum) {
        free(parent);
        return -1;
    }
    while (indexOfLeftChild <= ((internalPage_t*)parent) -> numOfKeys 
           && ((internalPage_t*)parent) -> record[indexOfLeftChild].pageNum != childPageNum) {
               indexOfLeftChild++;
           }
    free(parent);
    return indexOfLeftChild;
}
int insertIntoInternal(pagenum_t parentPageNum, int leftChildIndex, int64_t newKey, pagenum_t rightChildPageNum) {
    int i;
    page_t* parent = (page_t*)malloc(sizeof(struct page_t));
    file_read_page(parentPageNum, parent);
    
    for (i = (((internalPage_t*)parent) -> numOfKeys) - 1; i > leftChildIndex; i--) {
        ((internalPage_t*)parent) -> record[i + 1].key = ((internalPage_t*)parent) -> record[i].key;
        ((internalPage_t*)parent) -> record[i + 1].pageNum = ((internalPage_t*)parent) -> record[i].pageNum; 
    }
    ((internalPage_t*)parent) -> record[leftChildIndex + 1].key = newKey;
    ((internalPage_t*)parent) -> record[leftChildIndex + 1].pageNum = rightChildPageNum;
    (((internalPage_t*)parent) -> numOfKeys)++;
    file_write_page(parentPageNum, parent);
    free(parent);
    return SUCCESS;
}

//insert new key to parent, parent need to be splitted to parent and rightparent
int insertIntoInternalAfterSplitting(pagenum_t parentPageNum, int leftChildIndex, int64_t newKey, pagenum_t rightChildPageNum) {
    internalRecord_t* temporaryRecord;
    page_t* parent, * rightParent;
    pagenum_t rightParentPageNum;

    int i, j, split;
    int64_t kPrime;

    temporaryRecord = (internalRecord_t*)malloc(sizeof(struct internalRecord_t) * INTERNAL_ORDER);
    parent = (page_t*)malloc(sizeof(struct page_t));
    rightParent = (page_t*)malloc(sizeof(struct page_t));
    rightParentPageNum = file_alloc_page();
    file_read_page(parentPageNum, parent);
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

    ((internalPage_t*)rightParent) -> parentPageNum = ((internalPage_t*)parent) -> parentPageNum;

    page_t* child = (page_t*)malloc(sizeof(struct page_t));
    pagenum_t childPageNum = ((internalPage_t*)rightParent) -> leftMostPageNum;

    file_read_page(childPageNum, child);
    ((internalPage_t*)child) -> parentPageNum = rightParentPageNum;
    file_write_page(childPageNum, child);

    for (i = 0; i < ((internalPage_t*)rightParent) -> numOfKeys; i++) {
        childPageNum = ((internalPage_t*)rightParent) -> record[i].pageNum;
        file_read_page(childPageNum, child);
        ((internalPage_t*)child) -> parentPageNum = rightParentPageNum;
        file_write_page(childPageNum, child);
    }
    file_write_page(parentPageNum, parent);
    file_write_page(rightParentPageNum, rightParent);
    free(child);
    free(parent);
    free(rightParent);
    return insertIntoParent(parentPageNum, kPrime, rightParentPageNum);


}
int insertIntoParent(pagenum_t leftChildPageNum, int64_t newKey, pagenum_t rightChildPageNum) {

    int leftIndex;
    page_t* leftChild, * parent;
    pagenum_t parentPageNum;
    leftChild = (page_t*)malloc(sizeof(struct page_t));
    file_read_page(leftChildPageNum, leftChild);
    parentPageNum = ((leafPage_t*)leftChild) -> parentPageNum;
    free(leftChild);
    //leftChild was rootPage
    if (parentPageNum == HEADERPAGENUM) {
        return insertIntoNewRoot(leftChildPageNum, newKey, rightChildPageNum);
    }
    leftIndex = getIndexOfLeft(parentPageNum, leftChildPageNum);

    parent = (page_t*)malloc(sizeof(struct page_t));
    file_read_page(parentPageNum, parent);
    if (((internalPage_t*)parent) -> numOfKeys < INTERNAL_ORDER - 1) {
        free(parent);
        return insertIntoInternal(parentPageNum, leftIndex, newKey, rightChildPageNum);
    }
    
    free(parent);
    return insertIntoInternalAfterSplitting(parentPageNum, leftIndex, newKey, rightChildPageNum);

    return SUCCESS;

}
int insertIntoLeafAfterSplitting(pagenum_t oldLeafPageNum, int64_t key, char* value) {

    page_t* newLeaf, * oldLeaf;
    pagenum_t newLeafPageNum;
    leafRecord_t* temporaryRecord;
    int insertionIndex, split, i, j;
    int64_t newKey;

    newLeaf = (page_t*)malloc(sizeof(struct page_t));
    oldLeaf = (page_t*)malloc(sizeof(struct page_t));
    temporaryRecord = (leafRecord_t*)malloc(sizeof(struct leafRecord_t) * LEAF_ORDER);
    ((leafPage_t*)newLeaf) -> parentPageNum = 0;
    ((leafPage_t*)newLeaf) -> isLeaf = 1;
    ((leafPage_t*)newLeaf) -> numOfKeys = 0;
    ((leafPage_t*)newLeaf) -> rightSiblingPageNum = 0;

    file_read_page(oldLeafPageNum, oldLeaf);
    newLeafPageNum = file_alloc_page();

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

    file_write_page(newLeafPageNum, newLeaf);
    file_write_page(oldLeafPageNum, oldLeaf);
    free(newLeaf);
    free(oldLeaf);
    return insertIntoParent(oldLeafPageNum, newKey, newLeafPageNum);
}
// Insert input ‘key/value’ (record) to data file at the right place.
// If success, return 0. Otherwise, return non-zero value.
int db_insert(int64_t key, char * value) {
    page_t* page = (page_t*)malloc(sizeof(struct page_t));
    pagenum_t leafPageNum;
    char* tmp = (char*)malloc(sizeof(char) * 120);

    //check if key is already in the tree
    if (db_find(key, tmp) == SUCCESS) {
        printf("[ERROR]: key is already in the tree\n");
        printf("[ERROR]: db_insert fail\n");
        free(tmp);
        free(page);
        return FAIL;
    }
    free(tmp);
    
    file_read_page(HEADERPAGENUM, page);

    //there's no rootpage
    if (((headerPage_t*)page) -> rootPageNum == 0) {
        free(page);
        return startNewTree(key, value);
    }

    leafPageNum = findLeaf(key);

    file_read_page(leafPageNum, page);

    if (((leafPage_t*)page) -> numOfKeys < LEAF_ORDER - 1) {
        free(page);
        return insertIntoLeaf(leafPageNum, key, value);
    }



    free(page);
    return insertIntoLeafAfterSplitting(leafPageNum, key, value);
}

// Find the record containing input ‘key’.
// • If found matching ‘key’, store matched ‘value’ string in ret_val and return 0. Otherwise, return
// non-zero value.
int db_find(int64_t key, char * ret_val) {
    pagenum_t pageNum;
    int i = 0;
    pageNum = findLeaf(key);
    if (pageNum == 0) {
        return FAIL;
    }
    page_t* page = (page_t*)malloc(sizeof(struct page_t));
    file_read_page(pageNum, page);

    while (i < ((leafPage_t*)page) -> numOfKeys) {
        if (((leafPage_t*)page) -> record[i].key == key) {
            break;
        }
        i++;
    }

    if (i == ((leafPage_t*)page) -> numOfKeys) {
        free(page);
        return FAIL; // fail
    } else {
        strcpy(ret_val, ((leafPage_t*)page) -> record[i].value);
        free(page);
        return SUCCESS; // success
    }
}


pagenum_t findLeaf(int64_t key) {
    page_t* page = (page_t*)malloc(sizeof(struct page_t));
    pagenum_t leafPageNum;
    file_read_page(HEADERPAGENUM, page);
    pagenum_t rootPageNum = ((headerPage_t*)page) -> rootPageNum;
    file_read_page(rootPageNum, page);
    int i = 0;

    leafPageNum = rootPageNum;
    if (leafPageNum == 0) {
        return 0;
    }
    while (!((internalPage_t*)page) -> isLeaf) {
        i = 0;
        while (i < ((internalPage_t*)page) -> numOfKeys) {
            if (key < ((internalPage_t*)page) -> record[i].key) {
                break;
            } else {
                i++;
            }
        }
        if (i == 0) {
            leafPageNum = ((internalPage_t*)page) -> leftMostPageNum;
        } else {
            leafPageNum = ((internalPage_t*)page) -> record[i - 1].pageNum;
        }
        file_read_page(leafPageNum, page);
    }
    free(page);
    return leafPageNum;
}

// delete
// Find the matching record and delete it if found.
// If success, return 0. Otherwise, return non-zero value.
int db_delete(int64_t key) {
    pagenum_t leafPageNum;
    char tmp[120];

    if (db_find(key, tmp) == FAIL) {
        printf("[ERROR]: no %lld in the tree\n", key);
        printf("         db_delete(%lld) failed\n", key);
        return FAIL;
    } else {
        leafPageNum = findLeaf(key);
        return deleteEntry(leafPageNum, key);
    }
}

int deleteEntry(pagenum_t pageNum, int64_t key) {

    int neighborIndex, kPrimeIndex, kPrime, capacity;

    pagenum_t neighborPageNum;

    removeEntryFromPage(pageNum, key);

    page_t* page = (page_t*)malloc(sizeof(struct page_t));
    file_read_page(pageNum, page);
    if (((internalPage_t*)page) -> parentPageNum == 0) {
        free(page);
        return adjustRoot(pageNum);
    }

    if (((internalPage_t*)page) -> numOfKeys > 0) {
        free(page);
        return SUCCESS;
    }

    neighborIndex = getNeighborIndex(pageNum);
    kPrimeIndex = neighborIndex == -2 ? 0 : neighborIndex + 1;
    page_t* parent = (page_t*)malloc(sizeof(struct page_t));
    file_read_page(((internalPage_t*)page) -> parentPageNum, parent);
    kPrime = ((internalPage_t*)parent) -> record[kPrimeIndex].key;
    if (neighborIndex == - 2) {
        neighborPageNum = ((internalPage_t*)parent) -> record[kPrimeIndex].pageNum;
    } else if (kPrimeIndex == 0) {
        neighborPageNum = ((internalPage_t*)parent) -> leftMostPageNum;
    } else {
        neighborPageNum = ((internalPage_t*)parent) -> record[kPrimeIndex - 1].pageNum;
    }
    page_t* neighbor = (page_t*)malloc(sizeof(struct page_t));
    file_read_page(neighborPageNum, neighbor);
    capacity = ((internalPage_t*)page) -> isLeaf ? LEAF_ORDER : INTERNAL_ORDER - 1;

    if (((internalPage_t*)neighbor) -> numOfKeys + ((internalPage_t*)page) -> numOfKeys  < capacity) {
        free(page);
        free(parent);
        free(neighbor);
        return coalescePages(neighborPageNum, kPrime, pageNum, neighborIndex);
    } else {
        free(page);
        free(parent);
        free(neighbor);
        return redistributePages(neighborPageNum, kPrime, pageNum, neighborIndex, kPrimeIndex);
    } 

    free(page);
    free(parent);
    return SUCCESS;
}

int redistributePages(pagenum_t neighborPageNum, int64_t kPrime, pagenum_t pageNum, int neighborIndex, int kPrimeIndex) {
    // execute only when page is internal

    int i;
    page_t* child, * neighbor, * page, * parent;
    pagenum_t childPageNum, parentPageNum;

    neighbor = (page_t*)malloc(sizeof(struct page_t));
    page = (page_t*)malloc(sizeof(struct page_t));
    child = (page_t*)malloc(sizeof(struct page_t));
    parent = (page_t*)malloc(sizeof(struct page_t));
 
    file_read_page(neighborPageNum, neighbor);
    file_read_page(pageNum, page);
    // TODO: check  page's numkey == 0
    if (((internalPage_t*)page) -> numOfKeys != 0) {
        printf("something's wrong \n");
        free(page);
        free(neighbor);
        free(child);
        free(parent);
        return FAIL;
    }

    // move one from neighbor to page 
    if (neighborIndex == -2) {
        // neighbor is right child, page is leftmostchild

        ((internalPage_t*)page) -> record[((internalPage_t*)page) -> numOfKeys].key = kPrime;
        ((internalPage_t*)page) -> record[((internalPage_t*)page) -> numOfKeys].pageNum = ((internalPage_t*)neighbor) -> leftMostPageNum;
        ((internalPage_t*)neighbor) -> leftMostPageNum = ((internalPage_t*)neighbor) -> record[0].pageNum;
        childPageNum = ((internalPage_t*)page) -> record[((internalPage_t*)page) -> numOfKeys].pageNum;
        parentPageNum = ((internalPage_t*)page) -> parentPageNum;

        file_read_page(childPageNum, child);
        file_read_page(parentPageNum, parent);
        ((internalPage_t*)child) -> parentPageNum = pageNum;
        ((internalPage_t*)parent) -> record[kPrimeIndex].key = ((internalPage_t*)neighbor) -> record[0].key;
        file_write_page(childPageNum, child);
        file_write_page(parentPageNum, parent);
        free(parent);
        free(child);

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

        file_read_page(childPageNum, child);
        file_read_page(parentPageNum, parent);
        ((internalPage_t*)child) -> parentPageNum = pageNum;
        ((internalPage_t*)parent) -> record[kPrimeIndex].key = ((internalPage_t*)neighbor) -> record[((internalPage_t*)neighbor) -> numOfKeys - 1].key;
        file_write_page(childPageNum, child);
        file_write_page(parentPageNum, parent);
        free(parent);
        free(child);
    } 
    (((internalPage_t*)page) -> numOfKeys)++;
    (((internalPage_t*)neighbor) -> numOfKeys)--;
    file_write_page(pageNum, page);
    file_write_page(neighborPageNum, neighbor);
    free(page);
    free(neighbor);
    return SUCCESS;
}
//move right keys to left
int coalescePages(pagenum_t neighborPageNum , int64_t kPrime, pagenum_t pageNum, int neighborIndex) {
    pagenum_t leftPageNum, rightPageNum, parentPageNum;
    page_t* left = (page_t*)malloc(sizeof(struct page_t));
    page_t* right = (page_t*)malloc(sizeof(struct page_t));
    int leftInsertionIndex, rightNumOfKeys;
    int i,j;


    if (neighborIndex == -2) {
        leftPageNum = pageNum;
        rightPageNum = neighborPageNum; 
    } else {
        leftPageNum = neighborPageNum;
        rightPageNum = pageNum;
    }

    file_read_page(leftPageNum, left);
    file_read_page(rightPageNum, right);

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

        page_t* child = (page_t*)malloc(sizeof(struct page_t));

        for (i = 0; i < ((internalPage_t*)left) -> numOfKeys; i++) {
            file_read_page(((internalPage_t*)left) -> record[i].pageNum, child);
            ((internalPage_t*)child) -> parentPageNum = leftPageNum;
            file_write_page(((internalPage_t*)left) -> record[i].pageNum, child);
        }
        free(child);
    }


    parentPageNum = ((internalPage_t*)left) -> parentPageNum;
    file_write_page(leftPageNum, left);
    file_write_page(rightPageNum, right);
    file_free_page(rightPageNum);
    free(left);
    free(right);
    if (parentPageNum == 0) {
        //should not come in here
        return SUCCESS;
    }
    return deleteEntry(parentPageNum, kPrime);
}

// return pageNum's left one's index at parent,  if page is leftmost return - 2
int getNeighborIndex(pagenum_t pageNum) {
    pagenum_t parentPageNum;
    page_t* parent = (page_t*)malloc(sizeof(struct page_t));
    page_t* page = (page_t*)malloc(sizeof(struct page_t));
    int i;

    file_read_page(pageNum, page);
    parentPageNum = ((internalPage_t*)page) -> parentPageNum;
    file_read_page(parentPageNum, parent);

    if (((internalPage_t*)parent) -> leftMostPageNum == pageNum) {
        free(parent);
        free(page);
        return -2;
    }

    for (i = 0; i < ((internalPage_t*)parent) -> numOfKeys; i++) {
        if (((internalPage_t*)parent) -> record[i].pageNum == pageNum) {
            free(parent);
            free(page);
            return i - 1;
        }
    }



    printf("[ERROR]: getNeighborIndex fail");
    free(parent);
    free(page);
    return -3;

}
void removeEntryFromPage(pagenum_t pageNum, int64_t key) {
    int i;
    i = 0;

    page_t* page = (page_t*)malloc(sizeof(struct page_t));
    file_read_page(pageNum, page);
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
    file_write_page(pageNum, page);
    free(page);
}

int adjustRoot(pagenum_t pageNum) {
    page_t* page = (page_t*)malloc(sizeof(struct page_t));
    file_read_page(pageNum, page);

    if (((internalPage_t*)page) -> numOfKeys > 0) {
        return SUCCESS;
    }
    page_t* header = (page_t*)malloc(sizeof(struct page_t));
    file_read_page(HEADERPAGENUM, header);

    if (((internalPage_t*)page) -> isLeaf) {
        ((headerPage_t*)header) -> rootPageNum = 0;
        file_write_page(HEADERPAGENUM, header);
        file_free_page(pageNum);
        return SUCCESS;
    } else {
        ((headerPage_t*)header) -> rootPageNum = ((internalPage_t*)page) -> leftMostPageNum;
        ((internalPage_t*)page) -> parentPageNum = 0;
        page_t* child = (page_t*)malloc(sizeof(struct page_t));
        pagenum_t childPageNum = ((internalPage_t*)page) -> leftMostPageNum;
        file_read_page(childPageNum, child);
        ((internalPage_t*)child) -> parentPageNum = 0;
        file_write_page(childPageNum, child); 
        file_write_page(HEADERPAGENUM, header);
        file_write_page(pageNum, page);
        file_free_page(pageNum);

        return SUCCESS;
    }
}