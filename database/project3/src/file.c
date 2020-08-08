#include "file.h"
#include <stdio.h>
#include <stdlib.h>

int fd;

// if file already exists, open : return 1 
// if file doesn't exist, create: return 0
int file_open_table(char *pathname) {
    if ((fd = open(pathname, O_RDWR | O_CREAT | O_EXCL , 0644)) > 0) {
        return 0;
    } else {
        fd = open(pathname, O_RDWR );
        return 1;
    }

}

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page() {
    pagenum_t freePageNum, prevFreePageNum;
    uint64_t numOfPages;
    char alloc[PAGESIZE] = {0,};

    page_t* header = (page_t*)malloc(sizeof(struct page_t));
    page_t* freePage = (page_t*)malloc(sizeof(struct page_t));
    pagenum_t nextFreePageNum;

    file_read_page(HEADERPAGENUM, header);
    freePageNum = ((headerPage_t*)header) -> freePageNum;

    // freePageList is empty
    if (freePageNum == 0) {

        freePageNum = ((headerPage_t*)header) -> numOfPages;
        (((headerPage_t*)header) -> numOfPages)++;

        file_write_page(HEADERPAGENUM, header);
        free(freePage);
        free(header);
        return freePageNum;
    } else {
        // freePageList is not empty
        // alloc first freePageNum

        file_read_page(freePageNum, freePage);
        nextFreePageNum = ((freePage_t*)freePage) -> nextFreePageNum;

        ((headerPage_t*)header) -> freePageNum = nextFreePageNum;

        file_write_page(HEADERPAGENUM, header);
        free(header);
        free(freePage);
        return freePageNum;
    }
    return prevFreePageNum; //  TODO: check this line never executed

};
// Free an on-disk page to the free page list
void file_free_page(pagenum_t pagenum) {
    page_t* header = (page_t*)malloc(sizeof(struct page_t));
    page_t* page = (page_t*)malloc(sizeof(struct page_t));
    
    file_read_page(HEADERPAGENUM, header);
    file_read_page(pagenum, page);

    ((freePage_t*)page) -> nextFreePageNum = ((headerPage_t*)header) -> freePageNum;
    ((headerPage_t*)header) -> freePageNum = pagenum;

    file_write_page(HEADERPAGENUM, header);
    file_write_page(pagenum, page);
    free(page);
    free(header);
    
}
// Read an on-disk page into the in-memory page structure(dest)

void file_read_page(pagenum_t pagenum, page_t *dest) {
    lseek(fd, pagenum * PAGESIZE, SEEK_SET);
    read(fd, dest, PAGESIZE);
    //TODO: check if filesize is smaller thand pagesize * pagenum
}
// Write an in-memory page(src) to the on-disk page
void file_write_page(pagenum_t pagenum, const page_t *src) {
    lseek(fd, pagenum * PAGESIZE, SEEK_SET);
    write(fd, src, PAGESIZE);
}