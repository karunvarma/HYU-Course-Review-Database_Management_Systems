#include "file.h"
#include <stdio.h>

int fd;
// if file already exists, open : return 1 
// if file doesn't exist, create: return 0
int file_open_table(char *pathname) {
    if ((fd = open(pathname,O_RDWR |O_CREAT |O_EXCL,0644)) > 0) {
        return 0;
    } else {
        fd = open(pathname, O_RDWR | O_APPEND);
        return 1;
    }

}

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page() {
    pagenum_t freePageNum, prevFreePageNum;
    uint64_t numOfPages;
    char alloc[PAGESIZE] = {0,};

    lseek(fd, 0, SEEK_SET);
    read(fd, &freePageNum, sizeof(pagenum_t));

    if (freePageNum == 0) {
        lseek(fd, 16, SEEK_SET);
        read(fd, &numOfPages, sizeof(uint64_t));
        freePageNum = numOfPages;
        numOfPages++;

        lseek(fd, 16, SEEK_SET);
        write(fd, &numOfPages, sizeof(pagenum_t));
        lseek(fd, freePageNum * PAGESIZE, SEEK_SET);
        write(fd, alloc, PAGESIZE);
        return freePageNum;
    }
    while (freePageNum != 0) {
        prevFreePageNum = freePageNum;
        lseek(fd, freePageNum * PAGESIZE, SEEK_SET);
        read(fd, &freePageNum, sizeof(pagenum_t));

    }
    //TODO: update free file list
    return prevFreePageNum; 

};
// Free an on-disk page to the free page list
void file_free_page(pagenum_t pagenum);
// Read an on-disk page into the in-memory page structure(dest)

void file_read_page(pagenum_t pagenum, page_t *dest) {
    if (pagenum == 8) {
            printf("asdf");
    }
    lseek(fd, pagenum * PAGESIZE, SEEK_SET);
    read(fd, dest, PAGESIZE);
    //TODO: check if filesize is smaller thand pagesize * pagenum
}
// Write an in-memory page(src) to the on-disk page
void file_write_page(pagenum_t pagenum, const page_t *src) {
    if (pagenum == 8) {
        printf("asdf");
    }
    lseek(fd, pagenum * PAGESIZE, SEEK_SET);
    write(fd, src, PAGESIZE);
}