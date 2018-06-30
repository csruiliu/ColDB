//
// Created by rui on 6/27/18.
//

#include <utils.h>
#include <db_manager.h>

#include "db_cluster.h"

ClusterLink* clusterlink_init() {
    ClusterLink* clink = malloc(sizeof(ClusterLink));
    clink->head = NULL;
    clink->clink_size = 0;
}

void clusterlink_insert(int value, int rowId, ClusterLink* clusterLink) {
    ClusterNode* cnode = malloc(sizeof(ClusterNode));
    cnode->value = value;
    cnode->rowId = rowId;
    cnode->next = NULL;
    if (clusterLink->head == NULL) {
        clusterLink->head = cnode;
    }
    else {
        ClusterNode* cursor = clusterLink->head;
        while(cursor->next != NULL) {
            cursor = cursor->next;
        }
        cursor->next = cnode;
    }
    clusterLink->clink_size++;
}

//we use select sort method
ClusterLink* clusterlink_sort(ClusterLink *clusterLink) {
    ClusterNode* p,*q;
    ClusterNode* pMin;
    int tmpv=0;
    int tmpr=0;

    if (clusterLink->head->next == NULL) {
        return clusterLink;
    }

    for (p = clusterLink->head; p->next != NULL; p=p->next) {
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
    return clusterLink;
}

ClusterLink* clusterlink_btree(ClusterLink* clusterLink, int order) {
    ClusterNode* p;
    ClusterNode* q;

    if (clusterLink->head->next == NULL) {
        return clusterLink;
    }

}

int clusterlink_load(ClusterLink* clusterLink, Column* lcol) {
    ClusterNode* cursor = clusterLink->head;
    while(cursor != NULL) {
        if(insert_data_col(lcol,cursor->value,cursor->rowId) != 0) {
            log_err("load data failed.\n");
            return 1;
        }
        cursor = cursor->next;
    }
    return 0;
}