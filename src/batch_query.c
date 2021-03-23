#define _DEFAULT_SOURCE

#include "utils_func.h"
#include "batch_query.h"
#include "batch_queue.h"
#include "db_manager.h"

int use_batch = 0;

pthread_t** pths;
int nprocs;
char** pth_messages;

pthread_mutex_t mutex;

void set_use_batch(int value) {
    use_batch = value;
}

int get_use_batch() {
    return use_batch;
}

int init_batch() {
    if(pthread_mutex_init(&mutex, NULL) != 0) {
        perror("pthread_mutex_init failed\n");
        log_err("pthread_mutex_init failed\n");
        return 1;
    }
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

void* exec_query() {
    DbOperator* query;
    while(true) {
        pthread_mutex_lock(&mutex);
        bqNode* node = pop_head_bq();
        //bqNode* node = pop_head_bqrefine();
        pthread_mutex_unlock(&mutex);
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
                    Column* scol = get_column(select_col_name);
                    if(scol == NULL) {
                        log_err("[server.c:execute_DbOperator()] column didn't exist in the database.\n");
                        break;
                    }
                    else {
                        if (scol->idx_type == UNIDX) {
                            if(select_data_col_unidx(scol, handle, pre_range, post_range) != 0) {
                                free(query);
                                log_err("[server.c:execute_DbOperator()] select data from column in database failed.\n");
                            }
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
                    Result* srsl_pos = get_result(select_rsl_pos);
                    Result* srsl_val = get_result(select_rsl_val);
                    if(select_data_result(srsl_pos, srsl_val, handle, pre_range, post_range) != 0) {
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
    return 0;
}

int create_thread(size_t size_p) {
    pths = calloc(size_p, sizeof(pthread_t*));
    pth_messages = calloc(size_p, sizeof(char*));
    for(size_t i = 0; i < size_p; ++i) {
        pths[i] = malloc(sizeof(pthread_t));
        char* pth_name = "thread" + i;
        pth_messages[i] = malloc((sizeof(pth_name)+1)* sizeof(char));
        strcpy(pth_messages[i],pth_name);
        int ret_thread = pthread_create(pths[i], NULL, exec_query, (void *)pth_messages[i]);
        if(ret_thread == 0) {
            log_info("Thread %d create successfully.\n", i);
        }
        else {
            log_info("Thread %d create failed.\n", i);
        }
    }
    return 0;
}

int batch_schedule_convoy() {
    show_bqrefine();
    if(create_refine_batch_queue() != 0) {
        log_err("[db_batch.c:batch_schedule_convoy] create refined batch queue failed.\n");
        return 1;
    }
    size_t bq_len = get_bq_length();
    for(size_t i = 0; i < bq_len; ++i) {
        bqNode* node = pop_head_bq();
        node->next = NULL;
        if(push_bqr_convoy(node) != 0) {
            log_err("[db_batch.c:batch_schedule_convoy] refine batch queue failed.\n");
            return 1;
        }
    }
    show_bqrefine();
    return 0;
}

int exec_batch_query() {
    void *status;
    //nprocs = get_nprocs() - 1;
    log_info("current available process number: %d\n", nprocs);
    size_t query_batch_size = get_bq_length();
    create_thread(query_batch_size);
    for(size_t i = 0; i < query_batch_size; ++i) {
        pthread_join(*pths[i], &status);
    }
    if (pthread_mutex_destroy(&mutex) != 0) {
        perror("pthread_mutex_destroy failed\n");
        log_err("pthread_mutex_destroy failed\n");
        return 1;
    }
    return 0;
}