#define _DEFAULT_SOURCE

#include <memory.h>
#include "batch_queue.h"
#include "utils_func.h"

batchQueue* bq;

batchQueue* bq_opt;

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

/**
 * batch queue and operations
 */
int create_bq() {
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

batchQueue* get_bq() {
    return bq;
}

bool is_empty_bq() {
    if(bq == NULL) {
        return true;
    }
    else if(bq->length == 0) {
        return true;
    }
    else {
        return false;
    }
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

bqNode* pop_head_bq() {
    if (is_empty_bq()) {
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

void show_bq() {
    if (is_empty_bq()) {
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

size_t get_length_bq() {
    return bq->length;
}

/**
 * batch queue and operations for optimization
 */
int create_bq_opt() {
    bq_opt = (batchQueue*) malloc(sizeof(batchQueue));
    if(bq_opt == NULL) {
        return 1;
    }
    bqNode* node_r = (bqNode*) malloc(sizeof(bqNode));
    if(node_r == NULL){
        free(bq_opt);
        return 1;
    }
    bq_opt->head = node_r;
    bq_opt->tail = node_r;
    bq_opt->length = 0;
    return 0;
}

int push_node_bq_opt(bqNode *node) {
    if(bq_opt == NULL || node == NULL) {
        log_err("The refined queue or the adding node is null.\n");
        return 1;
    }
    bq_opt->tail->next = node;
    bq_opt->tail = node;
    bq_opt->length++;
    return 0;
}

bqNode* pop_head_bq_opt() {
    if (is_empty_bq_opt()) {
        return NULL;
    }
    bqNode* pop_node = malloc(sizeof(bqNode));
    bqNode* node = bq_opt->head->next;
    memcpy(pop_node, node, sizeof(bqNode));
    if(bq_opt->length == 1) {
        bq_opt->head->next = NULL;
        bq_opt->head = bq->tail;
        bq_opt->length = 0;
    }
    else {
        bq_opt->head->next = node->next;
        bq_opt->length--;
    }
    free(node);
    return pop_node;
}

batchQueue* get_bq_opt() {
    return bq_opt;
}

bool is_empty_bq_opt() {
    if(bq_opt == NULL) {
        return true;
    }
    else if(bq_opt->length == 0) {
        return true;
    }
    else {
        return false;
    }
}

size_t get_length_bq_opt() {
    return bq_opt->length;
}

void show_bq_opt() {
    if (is_empty_bq_opt()) {
        log_info("The bqr is empty\n");
        return;
    }
    bqNode* node = bq_opt->head->next;
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
int push_node_convoy(bqNode* ref_node) {
    if(bq_opt == NULL || ref_node == NULL) {
        log_err("The refined queue or the adding node is null.\n");
        return 1;
    }
    if(is_empty_bq_opt()) {
        bq_opt->tail->next = ref_node;
        bq_opt->tail = ref_node;
        bq_opt->length++;
        return 0;
    }
    bqNode* cur_node = bq_opt->head->next;
    bqNode* pre_node = bq_opt->head;
    while (cur_node != NULL) {
        char* ref_pre = ref_node->query->operator_fields.select_operator.pre_range;
        char* ref_post = ref_node->query->operator_fields.select_operator.post_range;
        char* cur_pre = cur_node->query->operator_fields.select_operator.pre_range;
        char* cur_post = cur_node->query->operator_fields.select_operator.post_range;
        if (strcmp(ref_post,cur_post) == 0 || strcmp(ref_pre,cur_pre) == 0) {
            ref_node->next = cur_node->next;
            cur_node->next = ref_node;
            bq_opt->length++;
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
                bq_opt->length++;
                return 0;
            }
            else if (int_ref_post > int_cur_post && int_ref_pre < int_cur_pre) {
                pre_node->next = ref_node;
                ref_node->next = cur_node;
                bq_opt->length++;
                return 0;
            }
        }
        pre_node = cur_node;
        cur_node = cur_node->next;
    }
    bq_opt->tail->next = ref_node;
    bq_opt->tail = ref_node;
    bq_opt->length++;
    return 0;
}
