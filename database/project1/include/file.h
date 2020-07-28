#ifndef __FILE_H_
#define __FILE_H_

#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
// disable byte padding
#pragma pack(1)

#define PAGESIZE 4096
typedef uint64_t pagenum_t;

typedef struct leafRecord {
    uint64_t key;
    char value[120];
} leafRecord;
typedef struct internalRecord {
    uint64_t key;
    pagenum_t pageNum;
} internalRecord;
//header
typedef struct headerPage {
            pagenum_t freePageNum;
            pagenum_t rootPageNum;
            uint64_t numOfPages;
            char headerPageReserved[PAGESIZE - 24];
} headerPage;
typedef struct freePage {
    pagenum_t nextFreePageNum;
    char freePageReserved[PAGESIZE - 8];
} freePage;
typedef struct internalPage{ 
    pagenum_t parentPageNum;
    int isLeaf;
    int numOfKeys;
    char internalPageHeaderReserved[104];
    pagenum_t leftMostPageNum;
    internalRecord record[248];
} internalPage;
// leaf page
typedef struct leafPage{
    pagenum_t parentPageNum;
    int isLeaf;
    int numOfKeys;
    char leafPageHeaderReserved[104];
    pagenum_t rightSiblingPageNum;  //If rightmost leaf page, right sibling page number field is 0.
    leafRecord record[31];
} leafPage;

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