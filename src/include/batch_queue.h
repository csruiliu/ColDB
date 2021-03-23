#ifndef BATCH_QUEUE_H
#define BATCH_QUEUE_H

#include "parse.h"

typedef struct batchQueueNode {
    DbOperator* query;
    struct batchQueueNode* next;
    char* share_query_handle;
} bqNode;

typedef struct batchQueue {
    bqNode *head;
    bqNode *tail;
    size_t length;
} batchQueue;

bqNode* create_node(DbOperator *query);

int create_batch_queue();

int create_refine_batch_queue();

batchQueue* get_bq();

batchQueue* get_bqr();

int is_bq_empty();

int is_bqr_empty();

int push_node_bq(bqNode *node);

int push_node_bqrefine(bqNode *node);

size_t get_bq_length();

size_t get_bqr_length();

void show_bq();

void show_bqrefine();

int push_bqr_convoy(bqNode *node);

bqNode* pop_head_bq();

bqNode* pop_head_bqrefine();

#endif //BATCH_QUEUE_H