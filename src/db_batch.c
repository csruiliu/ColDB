//
// Created by rui on 4/12/18.
//

#include <db_manager.h>
#include "db_batch.h"
#include "db_batch_queue.h"
#include "utils.h"

int useBatch = 0;

pthread_t** pths;
char** pth_messages;

void setUseBatch(int value) {
    useBatch = value;
}

int getUseBatch() {
    return useBatch;
}

int init_batch() {
    return create_batch_queue();
}

int batch_add(DbOperator *query) {
    bqNode* node = create_node(query);
    if(node == NULL) {
        return 1;
    }
    if(push_node_bq(node) != 0) {
        return 1;
    }
    return 0;
}

int batch_schedule_convoy() {
    show_bqr();
    if(create_refine_batch_queue() != 0) {
        log_err("[db_batch.c:batch_schedule_convoy] create refined batch queue failed.\n");
        return 1;
    }
    int bq_len = get_bq_length();
    for(int i = 0; i < bq_len; ++i) {
        bqNode* node = pop_head_bq();
        node->next = NULL;
        if(push_bqr_convoy(node) != 0) {
            log_err("[db_batch.c:batch_schedule_convoy] refine batch queue failed.\n");
            return 1;
        }
    }
    show_bqr();
    return 0;
}

int batch_schedule_partition() {
    /*
    int bq_len = get_bq_length();
    for (int i = 0; i < bq_len; ++i) {
        bqNode* node = get_head_bq();
        printf("query: %s, %s, %s.\n", node->query->operator_fields.select_operator.select_col,
               node->query->operator_fields.select_operator.pre_range,
               node->query->operator_fields.select_operator.post_range);
        pop_node_queue();
        if(is_bq_empty() == 0) {
            log_info("queue is empty\n");
        }
    }
     */
    return 0;
}

void query_execute() {
    DbOperator* query;
    while(1) {
        bqNode* node = pop_head_bqr();
        if (node == NULL) {
            break;
        }
        else {
            query = node->query;
            if (query->type == SELECT){
                if (query->operator_fields.select_operator.selectType == HANDLE_COL) {
                    char* select_col_name = query->operator_fields.select_operator.select_col;
                    char* handle = query->operator_fields.select_operator.handle;
                    char* pre_range = query->operator_fields.select_operator.pre_range;
                    char* post_range = query->operator_fields.select_operator.post_range;
                    Column* scol = get_col(select_col_name);
                    if(scol == NULL) {
                        log_err("[server.c:execute_DbOperator()] column didn't exist in the database.\n");
                    }
                    if (scol->idx_type == UNIDX) {
                        if(select_data_col_unidx(scol, handle, pre_range, post_range) != 0) {
                            free(query);
                            log_err("[server.c:execute_DbOperator()] select data from column in database failed.\n");
                        }
                    }
                    free(query);
                    log_info("select data from column in database successfully.\n");
                }
                else if (query->operator_fields.select_operator.selectType == HANDLE_RSL){
                    char* select_rsl_pos = query->operator_fields.select_operator.select_rsl_pos;
                    char* select_rsl_val = query->operator_fields.select_operator.select_rsl_val;
                    char* handle = query->operator_fields.select_operator.handle;
                    char* pre_range = query->operator_fields.select_operator.pre_range;
                    char* post_range = query->operator_fields.select_operator.post_range;
                    Result* srsl_pos = get_rsl(select_rsl_pos);
                    Result* srsl_val = get_rsl(select_rsl_val);
                    if(select_data_rsl(srsl_pos, srsl_val, handle, pre_range, post_range) != 0) {
                        free(query);
                        log_err("[server.c:execute_DbOperator()] select data from result in database failed.\n");
                    }
                    free(query);
                    log_info("select data from result in database successfully.\n");
                }
                else {
                    log_err("Cannot identify the select type[COL/RSL].\n");
                }
            }
        }
    }
}

int create_thread(int size_p) {
    pths = calloc(size_p, sizeof(pthread_t*));
    pth_messages = calloc(size_p, sizeof(char*));
    for(int i = 0; i < size_p; ++i) {
        pths[i] = malloc(sizeof(pthread_t));
        char* pth_name = "thread" + i;
        pth_messages[i] = malloc((sizeof(pth_name)+1)* sizeof(char));
        strcpy(pth_messages[i],pth_name);
        int ret_thread = pthread_create(pths[i],NULL,(void *) &query_execute,(void *)pth_messages[i]);
        if(ret_thread == 0) {
            log_info("Thread %d create successfully.\n", i);
        }
        else {
            log_info("Thread %d create failed.\n", i);
        }
    }
    return 0;
}


int exec_batch_query() {
    void *status;
    int nprocs = get_nprocs();
    log_info("current available process number: %d\n", nprocs);
    create_thread(nprocs-1);
    for(int i = 0; i < (nprocs-1); ++i) {
        pthread_join(*pths[i],&status);
    }
    return 0;
}

