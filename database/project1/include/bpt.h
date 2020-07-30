#ifndef __BPT_H__
#define __BPT_H__

// Uncomment the line below if you are compiling on Windows.
// #define WINDOWS
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include "file.h"
#ifdef WINDOWS
#define bool char
#define false 0
#define true 1
#endif
#define HEADERPAGENUM 0
#define SUCCESS 0
#define FAIL -1
// #define LEAF_ORDER 31
// #define INTERNAL_ORDER 248
#define LEAF_ORDER 3
#define INTERNAL_ORDER 3




int cut(int length);

int open_table(char *pathname);

int startNewTree(int64_t key, char* value);

int insertIntoLeaf(pagenum_t oldLeafPageNum, int64_t key, char* value);
int insertIntoNewRoot(pagenum_t oldLeafPageNum, int64_t newKey, pagenum_t newLeafPageNum);
int insertIntoInternal(pagenum_t parentPageNum, int leftIndex, int64_t newKey, pagenum_t newLeafPageNum);
int insertIntoParent(pagenum_t oldLeafPageNum, int64_t newKey, pagenum_t newLeafPageNum);
int getIndexOfLeft(pagenum_t parentPageNum, pagenum_t leftChildPageNum);
int insertIntoLeafAfterSplitting(pagenum_t leafPageNum, int64_t key, char* value);
int insertIntoInternalAfterSplitting(pagenum_t parentPageNum, int leftIndex, int64_t newKey, pagenum_t rightChildPageNum);
int db_insert(int64_t key, char *value);
int db_find(int64_t key, char *ret_val);
int db_delete(int64_t key);
pagenum_t findLeaf(int64_t key);

#endif /* __BPT_H__*/
