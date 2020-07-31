#include "bpt.h"
#include "file.h"
#include "tests.h"
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>

int fd;
void printHeader(page_t* headerPage) {
    printf("HeaderPage\n");
    printf("freePageNum: %d\nnumOfPages: %d\nrootPageNum: %d\n",
    ((headerPage_t*)headerPage) -> freePageNum, ((headerPage_t*)headerPage) -> numOfPages, ((headerPage_t*)headerPage) -> rootPageNum); 
    printf("-------------------------------------------------------------\n");
}
void printInternal(page_t* internalPage) {
    printf("InternalPage\n");
    printf("internalPage -> parentPageNum: %d\ninternalPage -> isLeaf: %d\ninternalPage -> numOfKeys: %d\ninternalPage ->leftMostPageNum: %d\n", ((internalPage_t*)internalPage) -> parentPageNum, ((internalPage_t*)internalPage) -> isLeaf, ((internalPage_t*)internalPage) -> numOfKeys, ((internalPage_t*)internalPage) ->leftMostPageNum);
    for (int i = 0; i < ((internalPage_t*)internalPage) -> numOfKeys; i++) {
        printf("internalPage -> record[%d].key: %d, internalPage -> record[%d].pageNum: %d\n", i, ((internalPage_t*)internalPage) -> record[i].key, i, ((internalPage_t*)internalPage) -> record[i].pageNum);
    }
    printf("-------------------------------------------------------------\n");
}
void printLeaf(page_t* leafPage) {
    printf("LeafPage\n");
    printf("leafPage -> parentPageNum: %d\nleafPage -> isLeaf: %d\nleafPage -> numOfKeys: %d\nleafPage -> rightSiblingPageNum: %d\n", ((leafPage_t*)leafPage) -> parentPageNum, ((leafPage_t*)leafPage) -> isLeaf, ((leafPage_t*)leafPage) -> numOfKeys, ((leafPage_t*)leafPage) -> rightSiblingPageNum);
    for (int i = 0; i < ((leafPage_t*)leafPage) -> numOfKeys; i++) {
        printf("leafPage -> record[%d].key: %d, leafPage -> record[%d].value: %s\n", i, ((leafPage_t*)leafPage) -> record[i].key, i, ((leafPage_t*)leafPage) -> record[i].value);
    }
    printf("-------------------------------------------------------------\n");
}
void printDb() {
    page_t* page = (page_t*)malloc(sizeof(struct page_t));
    pagenum_t pageNum = 0;
    file_read_page(pageNum, page);
    printf("pageNum: %d\n", pageNum);
    printHeader(page);
    
    pageNum++;
    uint64_t numofpages = ((headerPage_t*)page) -> numOfPages;
    while (pageNum < numofpages) {
        file_read_page(pageNum, page);
        if (((internalPage_t*)page) -> isLeaf) {
            printf("pageNum: %d\n", pageNum);
            printLeaf(page);
        } else {
            printf("pageNum: %d\n", pageNum);
            printInternal(page);
        }
        pageNum++;
    }
    
    

    free(page);

}
// MAIN
int main( int argc, char ** argv ) {

    // init();
    if ((fd = open("db",O_RDWR |O_CREAT |O_EXCL,0644)) > 0) { //not exist
        headerPage_t* header = (headerPage_t*)malloc(sizeof(struct headerPage_t));
        header -> rootPageNum = 0;
        header -> numOfPages = 1;
        header -> freePageNum = 0;
        lseek(fd, 0 , SEEK_SET);
        write(fd, header, PAGESIZE);
        free(header);
    } else { //exist 
        fd = open("db", O_RDWR | O_APPEND);

    }
    tests();
    int input_key;
    char input_value[120];
    char instruction;
    printf("> ");
    while (scanf("%c", &instruction) != EOF) {
        switch (instruction) {
        case 'i':
            scanf("%d",&input_key);
            scanf("%s",input_value);
            db_insert(input_key, input_value);
            break;
        case 'q':
            close(fd);
            remove("./db");
            while (getchar() != (int)'\n');
            return EXIT_SUCCESS;
            break;
        default:
            break;
        }
        while (getchar() != (int)'\n');
        // fflush(stdin);
        printDb();
        printf("======================================================================\n");
        printf("> ");
    }
    printf("\n");
    close(fd);
    remove("./db");
    return EXIT_SUCCESS;
}
