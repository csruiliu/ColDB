#define _DEFAULT_SOURCE

#include <memory.h>
#include "batch_queue.h"
#include "utils_func.h"

batchQueue* bq;

batchQueue* bq_refine;

bqNode* create_node(DbOperator *query) {
    bqNode* node = (bqNode*) malloc(sizeof(bqNode));
    if(node == NULL) {
        log_err("create batch queue node failed.\n");
        return NULL;
    }
    node->query = query;
    node->next = NULL;
    node->share_query_handle = (char *)malloc((sizeof(query->operator_fields.select_operator.handle)+1)* sizeof(char));
    strcpy(node->share_query_handle, query->operator_fields.select_operator.handle);
    return node;
}

int create_batch_queue() {
    bq = (batchQueue*) malloc(sizeof(batchQueue));
    if(bq == NULL) {
        return 1;
    }
    bqNode* node = (bqNode*) malloc(sizeof(bqNode));
    if(node == NULL){
        free(bq);
        return 1;
    }
    bq->head = node;
    bq->tail = node;
    bq->length = 0;
    return 0;
}

/**
 * create a refined batch queue
 */
int create_refine_batch_queue() {
    bq_refine = (batchQueue*) malloc(sizeof(batchQueue));
    if(bq_refine == NULL) {
        return 1;
    }
    bqNode* node_r = (bqNode*) malloc(sizeof(bqNode));
    if(node_r == NULL){
        free(bq_refine);
        return 1;
    }
    bq_refine->head = node_r;
    bq_refine->tail = node_r;
    bq_refine->length = 0;
    return 0;
}

batchQueue* get_bqr() {
    return bq_refine;
}

batchQueue* get_bq() {
    return bq;
}

int is_bq_empty() {
    if(bq == NULL) {
        return 0;
    }
    else if(bq->length == 0) {
        return 0;
    }
    else {
        return 1;
    }
}

int is_bqr_empty() {
    if(bq_refine == NULL) {
        return 0;
    }
    else if(bq_refine->length == 0) {
        return 0;
    }
    else {
        return 1;
    }
}

/**
 * pop the head node of batch queue
 */
bqNode* pop_head_bq() {
    if (is_bq_empty() == 0) {
        return NULL;
    }
    bqNode* pop_node = malloc(sizeof(bqNode));
    bqNode* node = bq->head->next;
    memcpy(pop_node, node, sizeof(bqNode));
    if(bq->length == 1) {
        bq->head->next = NULL;
        bq->head = bq->tail;
        bq->length = 0;
    }
    else {
        bq->head->next = node->next;
        bq->length--;
    }
    free(node);
    return pop_node;
}

/**
 * pop the head node of refined batch queue
 */
bqNode* pop_head_bqrefine() {
    if (is_bqr_empty() == 0) {
        return NULL;
    }
    bqNode* pop_node = malloc(sizeof(bqNode));
    bqNode* node = bq_refine->head->next;
    memcpy(pop_node, node, sizeof(bqNode));
    if(bq_refine->length == 1) {
        bq_refine->head->next = NULL;
        bq_refine->head = bq->tail;
        bq_refine->length = 0;
    }
    else {
        bq_refine->head->next = node->next;
        bq_refine->length--;
    }
    free(node);
    return pop_node;
}

int push_node_bq(bqNode *node) {
    if(bq == NULL || node == NULL) {
        log_err("The queue or the adding node is null.\n");
        return 1;
    }
    bq->tail->next = node;
    bq->tail = node;
    bq->length++;
    return 0;
}

int push_node_bqrefine(bqNode *node) {
    if(bq_refine == NULL || node == NULL) {
        log_err("The refined queue or the adding node is null.\n");
        return 1;
    }
    bq_refine->tail->next = node;
    bq_refine->tail = node;
    bq_refine->length++;
    return 0;
}

size_t get_bq_length() {
    return bq->length;
}

size_t get_bqr_length() {
    return bq_refine->length;
}

void show_bq() {
    if (is_bq_empty() == 0) {
        log_info("The bqr is empty\n");
        return;
    }
    bqNode *node = bq->head->next;
    while (node != NULL) {
        log_info("query: %s, %s, %s.\n", node->query->operator_fields.select_operator.select_col,
                 node->query->operator_fields.select_operator.pre_range,
                 node->query->operator_fields.select_operator.post_range);
        node = node->next;
    }
}

void show_bqrefine() {
    if (is_bqr_empty() == 0) {
        log_info("The bqr is empty\n");
        return;
    }
    bqNode* node = bq_refine->head->next;
    while (node != NULL) {
        log_info("query: %s, %s, %s.\n", node->query->operator_fields.select_operator.select_col,
                 node->query->operator_fields.select_operator.pre_range,
                 node->query->operator_fields.select_operator.post_range);
        node = node->next;
    }
}

/*
 * Convoy method:
 * 1. queries which have overlap will be adjacent
 * 2. the order is from the larger range to the smaller range
 */
int push_bqr_convoy(bqNode* ref_node) {
    if(bq_refine == NULL || ref_node == NULL) {
        log_err("The refined queue or the adding node is null.\n");
        return 1;
    }
    if(is_bqr_empty() == 0) {
        bq_refine->tail->next = ref_node;
        bq_refine->tail = ref_node;
        bq_refine->length++;
        return 0;
    }
    bqNode* cur_node = bq_refine->head->next;
    bqNode* pre_node = bq_refine->head;
    while (cur_node != NULL) {
        char* ref_pre = ref_node->query->operator_fields.select_operator.pre_range;
        char* ref_post = ref_node->query->operator_fields.select_operator.post_range;
        char* cur_pre = cur_node->query->operator_fields.select_operator.pre_range;
        char* cur_post = cur_node->query->operator_fields.select_operator.post_range;
        if (strcmp(ref_post,cur_post) == 0 || strcmp(ref_pre,cur_pre) == 0) {
            ref_node->next = cur_node->next;
            cur_node->next = ref_node;
            bq_refine->length++;
            return 0;
        }
        else if(strcmp(ref_pre,"null") != 0 && strcmp(ref_post,"null") != 0
                && strcmp(cur_pre,"null") != 0 && strcmp(cur_post,"null") != 0) {
            int int_ref_pre = atoi(ref_pre);
            int int_ref_post = atoi(ref_post);
            int int_cur_pre = atoi(cur_pre);
            int int_cur_post = atoi(cur_post);
            if(int_ref_post < int_cur_post && int_ref_pre > int_cur_pre) {
                ref_node->next = cur_node->next;
                cur_node->next = ref_node;
                bq_refine->length++;
                return 0;
            }
            else if (int_ref_post > int_cur_post && int_ref_pre < int_cur_pre) {
                pre_node->next = ref_node;
                ref_node->next = cur_node;
                bq_refine->length++;
                return 0;
            }
        }
        pre_node = cur_node;
        cur_node = cur_node->next;
    }
    bq_refine->tail->next = ref_node;
    bq_refine->tail = ref_node;
    bq_refine->length++;
    return 0;
}