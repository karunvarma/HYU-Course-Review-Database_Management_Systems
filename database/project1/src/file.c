#include "file.h"

extern int fd;


// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page() {
    pagenum_t freePageNum, prevFreePageNum;
    uint64_t numOfPages;

    lseek(fd, 0, SEEK_SET);
    read(fd, &freePageNum, sizeof(pagenum_t));

    if (freePageNum == 0) {
        lssek(fd, 16, SEEK_SET);
        read(fd, &numOfPages, sizeof(uint64_t));
        freePageNum = numOfPages;
        return freePageNum;
    }
    while (freePageNum != 0) {
        prevFreePageNum = freePageNum;
        lseek(fd, freePageNum * PAGESIZE, SEEK_SET);
        read(fd, &freePageNum, sizeof(pagenum_t));

    }
    
    return prevFreePageNum; 

};
// Free an on-disk page to the free page list
void file_free_page(pagenum_t pagenum);
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