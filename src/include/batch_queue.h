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

/**
 * create batch queue
 * and the operations
 */
int create_bq();

int push_node_bq(bqNode *node);

bqNode* pop_head_bq();

void show_bq();

batchQueue* get_bq();

int is_empty_bq();

size_t get_length_bq();


/**
 * create batch queue for optimization
 * and the operations
 */
int create_bq_opt();

int push_node_bq_opt(bqNode *node);

bqNode* pop_head_bq_opt();

batchQueue* get_bq_opt();

int is_empty_bq_opt();

size_t get_length_bq_opt();

void show_bq_opt();

int push_node_convoy(bqNode* ref_node);

#endif //BATCH_QUEUE_H