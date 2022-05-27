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
    return create_bq();
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
                        free_batch_query(node->query);
                        free(node->share_query_handle);
                        free(node);
                        log_err("[server.c:execute_DbOperator()] column didn't exist in the database.\n");
                        break;
                    }
                    else {
                        if (scol->idx_type == UNIDX) {
                            if(select_data_col_unidx(scol, handle, pre_range, post_range) != 0) {
                                free_batch_query(node->query);
                                free(node->share_query_handle);
                                free(node);
                                log_err("[server.c:execute_DbOperator()] select data from column in database failed.\n");
                                break;
                            }
                        }
                    }
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
                        free_batch_query(node->query);
                        free(node->share_query_handle);
                        free(node);
                        log_err("[server.c:execute_DbOperator()] select data from result in database failed.\n");
                        break;
                    }
                    log_info("select data from result in database successfully.\n");
                }
                else {
                    free_batch_query(node->query);
                    free(node->share_query_handle);
                    free(node);
                    log_err("Cannot identify the select type[COL/RSL].\n");
                    break;
                }
            }
            free_batch_query(node->query);
            free(node->share_query_handle);
            free(node);
        }
    }
    return 0;
}

int exec_batch_query() {
    void *status;
    //nprocs = get_nprocs() - 1;
    log_info("current available process number: %d\n", nprocs);
    size_t query_batch_size = get_length_bq();
    pthread_t* thread_pool = malloc(query_batch_size * sizeof(pthread_t));

    for(size_t i = 0; i < query_batch_size; i++){
        char* thread_name = "thread" + i;
        int rc = pthread_create(&thread_pool[i], NULL, exec_query, (void *)thread_name);
        if(rc) {
            log_err("fail to create the thread\n");
            return 1;
        }
    }
    for(size_t i = 0; i < query_batch_size; i++) {
        pthread_join(thread_pool[i], &status);
    }

    if (pthread_mutex_destroy(&mutex) != 0) {
        perror("pthread_mutex_destroy failed\n");
        log_err("pthread_mutex_destroy failed\n");
        return 1;
    }
    free(thread_pool);
    return 0;
}

void free_batch_query(DbOperator* query) {
    switch (query->type) {
        case CREATE_DB:
            free(query->operator_fields.create_db_operator.db_name);
            free(query);
            break;
        case CREATE_TBL:
            free(query->operator_fields.create_table_operator.db_name);
            free(query->operator_fields.create_table_operator.table_name);
            free(query);
            break;
        case CREATE_COL:
            free(query->operator_fields.create_col_operator.col_name);
            free(query->operator_fields.create_col_operator.tbl_name);
            free(query);
            break;
        case INSERT:
            free(query->operator_fields.insert_operator.values);
            free(query);
            break;
        case SELECT:
            free(query->operator_fields.select_operator.handle);
            free(query->operator_fields.select_operator.pre_range);
            free(query->operator_fields.select_operator.post_range);
            if (query->operator_fields.select_operator.selectType == HANDLE_RSL) {
                free(query->operator_fields.select_operator.select_rsl_pos);
                free(query->operator_fields.select_operator.select_rsl_val);
            }
            else {
                free(query->operator_fields.select_operator.select_col);
            }
            free(query);
            break;
        case FETCH:
            free(query->operator_fields.fetch_operator.handle);
            free(query->operator_fields.fetch_operator.col_var_name);
            free(query->operator_fields.fetch_operator.rsl_vec_pos);
            free(query);
            break;
        case LOAD:
            free(query->operator_fields.load_operator.data_path);
            free(query);
            break;
        case PRINT:
            for (size_t i = 0; i < query->operator_fields.print_operator.print_num; ++i) {
                free(query->operator_fields.print_operator.print_name[i]);
            }
            free(query->operator_fields.print_operator.print_name);
            free(query);
            break;
        case AVG:
            free(query->operator_fields.avg_operator.avg_name);
            free(query->operator_fields.avg_operator.handle);
            free(query);
            break;
        case SUM:
            free(query->operator_fields.sum_operator.sum_name);
            free(query->operator_fields.sum_operator.handle);
            free(query);
            break;
        case ADD:
            free(query->operator_fields.add_operator.add_name1);
            free(query->operator_fields.add_operator.add_name2);
            free(query->operator_fields.add_operator.handle);
            free(query);
            break;
        case SUB:
            free(query->operator_fields.sub_operator.sub_name1);
            free(query->operator_fields.sub_operator.sub_name2);
            free(query->operator_fields.sub_operator.handle);
            free(query);
            break;
        case MIN:
            if(query->operator_fields.min_operator.min_type == MIN_POS_VALUE) {
                free(query->operator_fields.min_operator.handle_pos);
                free(query->operator_fields.min_operator.min_vec_pos);
            }
            free(query->operator_fields.min_operator.handle_value);
            free(query->operator_fields.min_operator.min_vec_value);
            free(query);
            break;
        case MAX:
            if(query->operator_fields.max_operator.max_type == MAX_POS_VALUE) {
                free(query->operator_fields.max_operator.handle_pos);
                free(query->operator_fields.max_operator.max_vec_pos);
            }
            free(query->operator_fields.max_operator.handle_value);
            free(query->operator_fields.max_operator.max_vec_value);
            free(query);
            break;
        default:
            free(query);
    }
}

int destroy_batch_query_queue() {
    return destroy_bq();
}

int batch_schedule_convoy() {
    show_bq_opt();
    if(create_bq_opt() != 0) {
        log_err("[db_batch.c:batch_schedule_convoy] create refined batch queue failed.\n");
        return 1;
    }
    size_t bq_len = get_length_bq();
    for(size_t i = 0; i < bq_len; ++i) {
        bqNode* node = pop_head_bq();
        node->next = NULL;
        if(push_node_convoy(node) != 0) {
            log_err("[db_batch.c:batch_schedule_convoy] refine batch queue failed.\n");
            return 1;
        }
    }
    show_bq_opt();
    return 0;
}