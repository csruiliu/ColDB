//
// Created by rui on 4/13/18.
//

#ifndef COLDB_DB_BATCH_QUEUE_H
#define COLDB_DB_BATCH_QUEUE_H

#include "parse.h"

typedef struct batchQueueNode {
    DbOperator* query;
    struct batchQueueNode* next;
    char* share_query_handle;
} bqNode;

typedef struct batchQueue {
    bqNode *head;
    bqNode *tail;
    int length;
} batchQueue;

bqNode* create_node(DbOperator *query);

int create_batch_queue();

int add_batch_queue_sse(bqNode* node);


#endif //COLDB_DB_BATCH_QUEUE_H
