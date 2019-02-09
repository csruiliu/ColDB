//
// Created by rui on 4/13/18.
//

#include <memory.h>
#include "utils.h"
#include "db_batch_queue.h"

batchQueue* bq;

batchQueue* bqr;

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

int create_refine_batch_queue() {
    bqr = (batchQueue*) malloc(sizeof(batchQueue));
    if(bqr == NULL) {
        return 1;
    }
    bqNode* node_r = (bqNode*) malloc(sizeof(bqNode));
    if(node_r == NULL){
        free(bqr);
        return 1;
    }
    bqr->head = node_r;
    bqr->tail = node_r;
    bqr->length = 0;
    return 0;
}

batchQueue* get_bqr() {
    return bqr;
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
    if(bqr == NULL) {
        return 0;
    }
    else if(bqr->length == 0) {
        return 0;
    }
    else {
        return 1;
    }
}

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

bqNode* pop_head_bqr() {
    if (is_bqr_empty() == 0) {
        return NULL;
    }
    bqNode* pop_node = malloc(sizeof(bqNode));
    bqNode* node = bqr->head->next;
    memcpy(pop_node, node, sizeof(bqNode));
    if(bqr->length == 1) {
        bqr->head->next = NULL;
        bqr->head = bq->tail;
        bqr->length = 0;
    }
    else {
        bqr->head->next = node->next;
        bqr->length--;
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

int push_node_bqr(bqNode *node) {
    if(bqr == NULL || node == NULL) {
        log_err("The refined queue or the adding node is null.\n");
        return 1;
    }
    bqr->tail->next = node;
    bqr->tail = node;
    bqr->length++;
    return 0;
}

int get_bq_length() {
    return bq->length;
}

int get_bqr_length() {
    return bqr->length;
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

void show_bqr() {
    if (is_bqr_empty() == 0) {
        log_info("The bqr is empty\n");
        return;
    }
    bqNode* node = bqr->head->next;
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
    if(bqr == NULL || ref_node == NULL) {
        log_err("The refined queue or the adding node is null.\n");
        return 1;
    }
    if(is_bqr_empty() == 0) {
        bqr->tail->next = ref_node;
        bqr->tail = ref_node;
        bqr->length++;
        return 0;
    }
    bqNode* cur_node = bqr->head->next;
    bqNode* pre_node = bqr->head;
    while (cur_node != NULL) {
        char* ref_pre = ref_node->query->operator_fields.select_operator.pre_range;
        char* ref_post = ref_node->query->operator_fields.select_operator.post_range;
        char* cur_pre = cur_node->query->operator_fields.select_operator.pre_range;
        char* cur_post = cur_node->query->operator_fields.select_operator.post_range;
        if (strcmp(ref_post,cur_post) == 0 || strcmp(ref_pre,cur_pre) == 0) {
            ref_node->next = cur_node->next;
            cur_node->next = ref_node;
            bqr->length++;
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
                bqr->length++;
                return 0;
            }
            else if (int_ref_post > int_cur_post && int_ref_pre < int_cur_pre) {
                pre_node->next = ref_node;
                ref_node->next = cur_node;
                bqr->length++;
                return 0;
            }
        }
        pre_node = cur_node;
        cur_node = cur_node->next;
    }
    bqr->tail->next = ref_node;
    bqr->tail = ref_node;
    bqr->length++;
    return 0;
}

