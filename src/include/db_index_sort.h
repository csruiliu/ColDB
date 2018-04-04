//
// Created by rui on 4/3/18.
//

#ifndef COLDB_DB_INDEX_SORT_H
#define COLDB_DB_INDEX_SORT_H

#include "db_fds.h"

typedef struct sort_item {
    int value;
    int rowId;
    struct sort_item* next;
} SortNode;

typedef struct sort_link {
    unsigned int slink_size;
    SortNode* head;
} SortLink;

SortLink* sortlink_init();

void sortlink_insert(int value, int rowId, SortLink* sortLink);

int sortlink_load(SortLink* sortLink, Column* lcol);



#endif //COLDB_DB_INDEX_SORT_H
