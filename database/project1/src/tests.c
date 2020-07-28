#include "tests.h"
extern int fd;

void pageSizeTest() {
    printf("pageSizeTest: %s\n", sizeof(struct page_t) == 4096 ? "passed" : "failed");
    printf("pageSizeTest: %s\n", sizeof(struct leafPage) == 4096 ? "passed" : "failed");
    printf("pageSizeTest: %s\n", sizeof(struct headerPage) == 4096 ? "passed" : "failed");
    printf("pageSizeTest: %s\n", sizeof(struct internalPage) == 4096 ? "passed" : "failed");
}

void headerPageTest(){
    page_t* page = (page_t*) malloc(sizeof(struct page_t));
    lseek(fd,0, SEEK_SET);
    read(fd, page, PAGESIZE);
    printf("headerpage rootpagenum : %d\n", ((headerPage*)page) -> rootPageNum );

    free(page);

}
void tests() {
    pageSizeTest();
    headerPageTest();
}