#include "tests.h"
#include "file.h"
#include "index.h"
extern int fd;

void pageSizeTest() {
    printf("pageSizeTest: %s\n", sizeof(struct page_t) == 4096 ? "passed" : "failed");
    printf("pageSizeTest: %s\n", sizeof(struct leafPage_t) == 4096 ? "passed" : "failed");
    printf("pageSizeTest: %s\n", sizeof(struct headerPage_t) == 4096 ? "passed" : "failed");
    printf("pageSizeTest: %s\n", sizeof(struct internalPage_t) == 4096 ? "passed" : "failed");
}

void headerPageTest() {
    page_t* page = (page_t*) malloc(sizeof(struct page_t));
    lseek(fd,0, SEEK_SET);
    read(fd, page, PAGESIZE);
    printf("headerpage rootpagenum : %llu\n", ((headerPage_t*)page) -> rootPageNum );

    free(page);

}
void fileTest() {
    int testfd = open("testdb",O_RDWR |O_CREAT |O_EXCL,0644);
    
    remove("./testdb");
}
void tests() {
    pageSizeTest();
    headerPageTest();
    printf("-------------------------------------------------------------\n");
}