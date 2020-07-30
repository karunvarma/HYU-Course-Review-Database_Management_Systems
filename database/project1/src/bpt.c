#include "bpt.h"
// GLOBALS.

int open_table(char *pathname) {
   
}
int startNewTree(int64_t key, char* value) {
    page_t* page = (page_t*)malloc(sizeof(struct page_t));
    page_t* headerPage = (page_t*)malloc(sizeof(struct page_t));

    ((leafPage_t*)page) -> parentPageNum = 0;
    ((leafPage_t*)page) -> isLeaf = 1;
    ((leafPage_t*)page) -> numOfKeys = 1;
    ((leafPage_t*)page) -> rightSiblingPageNum = 0;
    ((leafPage_t*)page) -> record[0].key = key;
    strcpy(((leafPage_t*)page) -> record[0].value, value);
    //update headerpage
    file_read_page(HEADERPAGENUM, headerPage);
    (((headerPage_t*)headerPage) -> numOfPages)++;
    ((headerPage_t*)headerPage) -> rootPageNum = file_alloc_page();

    file_write_page(HEADERPAGENUM, headerPage);
    file_write_page(((headerPage_t*)headerPage) -> rootPageNum, page);

    free(page);
    free(headerPage);

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

int insertIntoNewRoot(pagenum_t oldLeafPageNum, int64_t newKey, pagenum_t newLeafPageNum) {
    page_t* root = (page_t*)malloc(sizeof(struct page_t));
    page_t* oldLeaf = (page_t*)malloc(sizeof(struct page_t));
    page_t* newLeaf = (page_t*)malloc(sizeof(struct page_t));
    page_t* header = (page_t*)malloc(sizeof(struct page_t));
    pagenum_t rootPageNum;

    rootPageNum = file_alloc_page();
    file_read_page(HEADERPAGENUM, header);
    file_read_page(oldLeafPageNum, oldLeaf);
    file_read_page(newLeafPageNum, newLeaf);

    ((headerPage_t*)header) -> rootPageNum = rootPageNum;
    ((internalPage_t*)root) -> parentPageNum = HEADERPAGENUM;
    ((internalPage_t*)root) -> isLeaf = 0;
    ((internalPage_t*)root) -> numOfKeys = 0;
    (((internalPage_t*)root) -> numOfKeys)++;
    ((internalPage_t*)root) -> leftMostPageNum = oldLeafPageNum;
    ((internalPage_t*)root) -> record[0].key = newKey;
    ((internalPage_t*)root) -> record[0].pageNum = newLeafPageNum;
    ((leafPage_t*)oldLeaf) -> parentPageNum = rootPageNum;
    ((leafPage_t*)newLeaf) -> parentPageNum = rootPageNum;

    file_write_page(HEADERPAGENUM, header);
    file_write_page(oldLeafPageNum, oldLeaf);
    file_write_page(newLeafPageNum, newLeaf);
    file_write_page(rootPageNum, root);

    free(header);
    free(root);
    free(oldLeaf);
    free(newLeaf);
    return SUCCESS;
}

//if leftchild is the leftmost child , return -1
int getIndexOfLeft(pagenum_t parentPageNum, pagenum_t leftChildPageNum) {
    int IndexOfLeftChild = 0;
    page_t* parent = (page_t*)malloc(sizeof(struct page_t));
    file_read_page(parentPageNum, parent);

    if (((internalPage_t*)parent) -> leftMostPageNum == leftChildPageNum) {
        free(parent);
        return -1;
    }
    while (IndexOfLeftChild <= ((internalPage_t*)parent) -> numOfKeys 
           && ((internalPage_t*)parent) -> record[IndexOfLeftChild].pageNum != leftChildPageNum) {
               IndexOfLeftChild++;
           }
    free(parent);
    return IndexOfLeftChild;
}
int insertIntoInternal(pagenum_t parentPageNum, int leftIndex, int64_t newKey, pagenum_t newLeafPageNum) {
    int i;
    page_t* parent = (page_t*)malloc(sizeof(struct page_t));
    file_read_page(parentPageNum, parent);
    
    for (i = (((internalPage_t*)parent) -> numOfKeys) - 1; i > leftIndex; i--) {
        ((internalPage_t*)parent) -> record[i + 1].key = ((internalPage_t*)parent) -> record[i].key;
        ((internalPage_t*)parent) -> record[i + 1].pageNum = ((internalPage_t*)parent) -> record[i].pageNum; 
    }
    ((internalPage_t*)parent) -> record[leftIndex + 1].key = newKey;
    ((internalPage_t*)parent) -> record[leftIndex + 1].pageNum = newLeafPageNum;
    (((internalPage_t*)parent) -> numOfKeys)++;
    file_write_page(parentPageNum, parent);
    free(parent);
    return SUCCESS;
}

//insert new key to parent, parent need to be splitted to parent and rightparent
int insertIntoInternalAfterSplitting(pagenum_t parentPageNum, int leftIndex, int64_t newKey, pagenum_t rightChildPageNum) {
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
        if (j == leftIndex + 1) j++;
        temporaryRecord[j].key = ((internalPage_t*)parent) -> record[i].key;
        temporaryRecord[j].pageNum = ((internalPage_t*)parent) -> record[i].pageNum;
    }

    temporaryRecord[leftIndex + 1].key = newKey;
    temporaryRecord[leftIndex + 1].pageNum =  rightChildPageNum;

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
int insertIntoParent(pagenum_t oldLeafPageNum, int64_t newKey, pagenum_t newLeafPageNum) {

    int leftIndex;
    page_t* oldLeaf, * parent;
    pagenum_t parentPageNum;
    oldLeaf = (page_t*)malloc(sizeof(struct page_t));
    file_read_page(oldLeafPageNum, oldLeaf);
    parentPageNum = ((leafPage_t*)oldLeaf) -> parentPageNum;
    free(oldLeaf);
    //oldLeaf was rootPage
    if (parentPageNum == HEADERPAGENUM) {
        return insertIntoNewRoot(oldLeafPageNum, newKey, newLeafPageNum);
    }
    leftIndex = getIndexOfLeft(parentPageNum, oldLeafPageNum);

    parent = (page_t*)malloc(sizeof(struct page_t));
    file_read_page(parentPageNum, parent);
    if (((internalPage_t*)parent) -> numOfKeys < INTERNAL_ORDER - 1) {
        free(parent);
        return insertIntoInternal(parentPageNum, leftIndex, newKey, newLeafPageNum);
    }
    
    free(parent);
    return insertIntoInternalAfterSplitting(parentPageNum, leftIndex, newKey, newLeafPageNum);

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
// ((leafPage_t*)oldLeaf) ->
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
int db_delete(int64_t key) {

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