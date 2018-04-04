//
// Created by rui on 4/3/18.
//

#include <utils.h>
#include <db_manager.h>
#include "db_index_sort.h"

SortLink* sortlink_init() {
    SortLink* slink = malloc(sizeof(SortLink));
    slink->head = NULL;
    slink->slink_size = 0;
}

int sortlink_load(SortLink* sortLink, Column* lcol) {
    SortNode* cursor = sortLink->head;
    while(cursor != NULL) {
        if(insert_data_col(lcol,cursor->value,cursor->rowId) != 0) {
            log_err("load data failed.\n");
            return 1;
        }
        cursor = cursor->next;
    }
    return 0;
}

void sortlink_insert(int value, int rowId, SortLink* sortLink) {
    SortNode* snode = malloc(sizeof(SortNode));
    snode->value = value;
    snode->rowId = rowId;
    snode->next = NULL;
    if (sortLink->head == NULL) {
        sortLink->head = snode;
    }
    else {
        SortNode* cursor = sortLink->head;
        while(cursor->next != NULL) {
            cursor = cursor->next;
        }
        cursor->next = snode;
    }
    sortLink->slink_size++;
}

SortLink* sortlink_select_sort(SortLink* sortLink) {
    SortNode* p,*q;
    SortNode* pMin;
    int tmpv=0;
    int tmpr=0;

    if (sortLink->head->next == NULL) {
        return sortLink;
    }

    for (p=sortLink->head; p->next != NULL; p=p->next) {
        pMin = p;
        for (q=p->next; q; q=q->next) {
            if (q->value < pMin->value) {
                pMin = q;
            }
        }
        if (pMin != p) {
            tmpv = pMin->value;
            tmpr = pMin->rowId;
            pMin->value = p->value;
            pMin->rowId = p->rowId;
            p->value = tmpv;
            p->rowId = tmpr;
        }
    }
    return sortLink;
}