//
// Created by rui on 6/27/18.
//

#ifndef COLDB_DB_CLUSTER_H
#define COLDB_DB_CLUSTER_H

#include "db_fds.h"

typedef struct cluster_item {
    int value;
    int rowId;
    struct cluster_item* next;
} ClusterNode;

typedef struct cluster_link {
    unsigned int clink_size;
    ClusterNode* head;
} ClusterLink;

ClusterLink* clusterlink_init();

void clusterlink_insert(int value, int rowId, ClusterLink* clusterLink);

ClusterLink* clusterlink_sort_select(ClusterLink* clusterLink);

ClusterLink* clusterlink_btree(ClusterLink* clusterLink);

int clusterlink_load(ClusterLink* clusterLink, Column* lcol);

#endif //COLDB_DB_CLUSTER_H
