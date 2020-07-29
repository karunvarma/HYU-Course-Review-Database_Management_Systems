#ifndef __FILE_H_
#define __FILE_H_

#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
// disable byte padding
#pragma pack(1)

#define PAGESIZE 4096
typedef uint64_t pagenum_t;

typedef struct leafRecord_t {
    int64_t key;
    char value[120];
} leafRecord_t;

typedef struct internalRecord_t {
    int64_t key;
    pagenum_t pageNum;
} internalRecord_t;

//header
typedef struct headerPage_t {
            pagenum_t freePageNum;
            pagenum_t rootPageNum;
            uint64_t numOfPages;
            char headerPageReserved[PAGESIZE - 24];
} headerPage_t;

typedef struct freePage_t {
    pagenum_t nextFreePageNum;
    char freePageReserved[PAGESIZE - 8];
} freePage_t;

typedef struct internalPage_t { 
    pagenum_t parentPageNum;
    int isLeaf;
    int numOfKeys;
    char internalPageHeaderReserved[104];
    pagenum_t leftMostPageNum;
    internalRecord_t record[248];
} internalPage_t;

// leaf page
typedef struct leafPage_t {
    pagenum_t parentPageNum;
    int isLeaf;
    int numOfKeys;
    char leafPageHeaderReserved[104];
    pagenum_t rightSiblingPageNum;  //If rightmost leaf page, right sibling page number field is 0.
    leafRecord_t record[31];
} leafPage_t;

typedef struct page_t {
    char reserved[PAGESIZE];
} page_t;
// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page();
// Free an on-disk page to the free page list
void file_free_page(pagenum_t pagenum);
// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(pagenum_t pagenum, page_t *dest);
// Write an in-memory page(src) to the on-disk page
void file_write_page(pagenum_t pagenum, const page_t *src);


#endif /* __FILE_H_*/