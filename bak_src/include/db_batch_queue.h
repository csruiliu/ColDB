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

int create_refine_batch_queue();

batchQueue* get_bq();

batchQueue* get_bqr();

int is_bq_empty();

int is_bqr_empty();

int push_node_bq(bqNode *node);

int push_node_bqr(bqNode * node);

int get_bq_length();

int get_bqr_length();

void show_bq();

void show_bqr();

int push_bqr_convoy(bqNode *node);

bqNode* pop_head_bq();

bqNode* pop_head_bqr();

#endif //COLDB_DB_BATCH_QUEUE_H
