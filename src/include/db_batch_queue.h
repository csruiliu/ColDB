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

int add_batch_queue(bqNode* node);

int is_bq_empty();

void show_batch_query();

int get_queue_length();

bqNode* pop_batch_queue();

#endif //COLDB_DB_BATCH_QUEUE_H
