#define _DEFAULT_SOURCE

#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <assert.h>
#include <memory.h>
#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>

#include "message.h"
#include "db_element.h"
#include "index_btree.h"
#include "index_sort.h"
#include "utils_func.h"
#include "db_manager.h"

#define RESIZE 2
#define DIRLEN 256
#define MAX_COLUMN_SIZE 1024
#define RAND_PRIME 6108689

/**
 * there will always be only one active database at a time
 */
Db* current_db;

void* resize(void* data, size_t oc, size_t nc) {
    assert(oc <= nc);
    void* ndata = calloc(nc, sizeof(char));
    memmove(ndata, data, oc);
    free(data);
    return ndata;
}

long* resize_long(long* data, size_t oc, size_t nc) {
    assert(oc <= nc);
    long* ndata = calloc(nc, sizeof(long));
    memmove(ndata, data, oc * sizeof(long));
    free(data);
    return ndata;
}

size_t count_digits(long number){
    size_t count = 0;
    while(number != 0) {
        number /= 10;
        ++count;
    }
    return count;
}

Db* create_db(char* db_name) {
    Db* db = get_db(db_name);
    if(db != NULL) {
        log_info("database %s exists, open database %s\n", db_name, db_name);
        return current_db;
    }
    db = malloc(sizeof(Db));
    db->name = calloc(strlen(db_name)+1, sizeof(char));
    strcpy(db->name, db_name);
    db->capacity = 0;
    db->size = 0;
    db->tables = NULL;

    put_db(db_name,db);
    log_info("create db %s successfully.\n", db_name);
    return db;
}

Table* create_table(char* db_name, char* table_name, char* pricls_col_name, size_t num_columns) {
    if(strcmp(current_db->name, db_name) != 0) {
        log_err("the current active database is not database %s\n", db_name);
        return NULL;
    }
    Table* tbl = get_table(table_name);
    if(tbl != NULL) {
        log_info("the table %s exists.\n", table_name);
        return tbl;
    }
    else {
        if(current_db->capacity - current_db->size <= 0) {
            log_info("no enough table space in database %s, creating more table space.\n", current_db->name);
            size_t more_table_capacity = RESIZE * current_db->capacity + 1;
            size_t new_capacity = sizeof(Table*) * more_table_capacity;
            size_t old_capacity = sizeof(Table*) * current_db->capacity;
            if(old_capacity == 0) {
                assert(new_capacity>0);
                current_db->tables = calloc(more_table_capacity, sizeof(Table*));
            }
            else {
                current_db->tables = resize(current_db->tables, old_capacity, new_capacity);
                if(current_db->tables == NULL){
                    log_err("create more table space in database failed.\n", current_db->name);
                    free(tbl);
                    return NULL;
                }
            }
            current_db->capacity = more_table_capacity;
        }
        tbl = malloc(sizeof(Table));
        tbl->name = calloc(strlen(table_name)+1, sizeof(char));
        strcpy(tbl->name, table_name);
        tbl->aff_db_name = calloc(strlen(db_name)+1, sizeof(char));
        strcpy(tbl->aff_db_name, db_name);
        tbl->capacity = num_columns;
        tbl->size = 0;
        tbl->pricluster_index_col_name = calloc(strlen(pricls_col_name)+1, sizeof(char));
        strcpy(tbl->pricluster_index_col_name, pricls_col_name);
        if(strcmp(pricls_col_name,"NULL") == 0) {
            tbl->has_cluster_index = 0;
        }
        else {
            tbl->has_cluster_index = 1;
        }
        tbl->columns = calloc(num_columns, sizeof(Column*));
        put_table(table_name, tbl);
        current_db->tables[current_db->size] = tbl;
        current_db->size++;
        return tbl;
    }
}

Column* create_column(char* tbl_name, char* col_name) {
    Table* cur_tbl = get_table(tbl_name);
    if(cur_tbl == NULL) {
        log_err("[db_manager.c:create_column()] the associated table doesn't exist, create table %s.\n", tbl_name);
        return NULL;
    }
    if(cur_tbl->size < cur_tbl->capacity) {
        Column* col = get_column(col_name);
        if(col != NULL) {
            log_info("[db_manager.c:create_column()] column %s exists.\n", col_name);
            return col;
        }
        col = malloc(sizeof(Column));
        col->name = calloc(strlen(col_name)+1, sizeof(char));
        strcpy(col->name, col_name);
        col->aff_tbl_name = calloc(strlen(tbl_name)+1, sizeof(char));
        strcpy(col->aff_tbl_name, tbl_name);
        col->cls_type = UNCLSR;
        col->idx_type = UNIDX;
        col->data = NULL;
        col->rowId = NULL;
        col->size = 0;
        col->capacity = 0;
        put_column(col_name, col);
        cur_tbl->columns[cur_tbl->size] = col;
        cur_tbl->size++;
        return col;
    }
    else {
        log_err("[db_manager.c:create_column()] the associated table is full, cannot create new column %s\n", col_name);
        return NULL;
    }
}

/**
 * Insert a piece of data to a single column
 */
int insert_data_column(Column* col, long data, long rowId) {
    if (col->size >= col->capacity) {
        size_t new_column_length = RESIZE * col->capacity + 1;
        size_t new_length = new_column_length;
        size_t old_length = col->capacity;
        if (old_length == 0) {
            assert(new_length > 0);
            //log_info("column %s has new length %d\n",col->col_name, new_length);
            col->data = calloc(new_length, sizeof(long));
            col->rowId = calloc(new_length, sizeof(long));
        } else {
            col->data = resize_long(col->data, old_length, new_length);
            col->rowId = resize_long(col->rowId, old_length, new_length);
            if (!col->data) {
                log_err("creating more data space failed.\n");
                return 1;
            }
        }
        col->capacity = new_length;
    }
    col->data[col->size] = data;
    col->rowId[col->size] = rowId;
    col->size++;
    return 0;
}

/**
 * relational insert, the column names are omitted
 * values are inserted into the columns of the table in the order those columns were declared in table creation
 * For example, if there are 3 columns in a table when the table is created like table(col1, col2, col3)
 * Then, INSERT INTO table VALUES (1,2,3) means insert 1 to col1, insert 2 to col2, and insert 3 to col3.
 **/
int insert_data_table(Table* itbl, long* row_values) {
    for(size_t i = 0; i < itbl->size; ++i) {
        Column* icol = get_column(itbl->columns[i]->name);
        if(insert_data_column(icol, row_values[i], (long) (icol->size+i)) != 0) {
            log_err("[db_manager.c:insert_data_tbl()] insert table failed.\n");
            return 1;
        }
    }
    return 0;
}

/**
 * select data for result
 **/
int select_data_result(Result* srsl_pos, Result* srsl_val, char* handle, char* pre_range, char* post_range) {
    size_t rsl_size = srsl_pos->num_tuples;
    long* srsl_pos_payload = srsl_pos->payload;
    long* srsl_val_payload = srsl_val->payload;
    long* rsl_payload = calloc(rsl_size, sizeof(long));
    Result* rsl = malloc(sizeof(Result));
    size_t size = 0;
    if (strncmp(pre_range,"null",4) == 0) {
        long post = strtol(post_range, NULL, 0);
        for(size_t i = 0; i < rsl_size; ++i) {
            if(srsl_val_payload[i] < post) {
                rsl_payload[size] = srsl_pos_payload[i];
                size++;
            }
        }
    }
    else if (strncmp(post_range,"null",4) == 0) {
        long pre = strtol(post_range, NULL, 0);
        for(size_t i = 0; i < rsl_size; ++i) {
            if(srsl_val_payload[i] >= pre) {
                rsl_payload[size] = srsl_pos_payload[i];
                size++;
            }
        }
    }
    else {
        long pre = strtol(post_range, NULL, 0);
        long post = strtol(post_range, NULL, 0);
        for(size_t i = 0; i < rsl_size; ++i) {
            if(srsl_val_payload[i] < post && srsl_val_payload[i] >= pre) {
                rsl_payload[size] = srsl_pos_payload[i];
                size++;
            }
        }
    }

    rsl->num_tuples = size;
    rsl->data_type = LONG;
    rsl->payload = calloc(size, sizeof(long));
    memmove(rsl->payload, rsl_payload, size*sizeof(long));
    update_result(handle,rsl);

    free(rsl_payload);
    free(rsl);
    return 0;
}

/**
 * select data for un-index column
 **/
int select_data_col_unidx(Column* scol, char* handle, char* pre_range, char* post_range) {
    Result* rsl = malloc(sizeof(Result));
    size_t count = 0;
    long* scol_data = scol->data;
    long* rsl_data = calloc(scol->size, sizeof(long));

    if (strncmp(pre_range,"null",4) == 0) {
        long post = strtol(post_range, NULL, 0);
        for(size_t i = 0; i < scol->size; ++i) {
            if(scol_data[i] < post) {
                rsl_data[count] = (long) i;
                count++;
            }
        }
    }
    else if (strncmp(post_range,"null",4) == 0) {
        long pre = strtol(pre_range, NULL, 0);
        for(size_t i = 0; i < scol->size; ++i) {
            if(scol_data[i] >= pre) {
                rsl_data[count] = (long) i;
                count++;
            }
        }
    }
    else {
        long pre = strtol(pre_range, NULL, 0);
        long post = strtol(post_range, NULL, 0);
        for(size_t i = 0; i < scol->size; ++i) {
            if(scol_data[i] >= pre && scol_data[i] < post) {
                rsl_data[count] = (long) i;
                count++;
            }
        }
    }

    rsl->num_tuples = count;
    rsl->data_type = LONG;
    rsl->payload = calloc(count, sizeof(long));
    memmove(rsl->payload,rsl_data,count*sizeof(long));
    update_result(handle,rsl);

    free(rsl_data);
    free(rsl);
    return 0;
}

/**
 * select data for the column with btree index
 **/
int select_data_col_btree(Column* scol, char* handle, char* pre_range, char* post_range) {
    Result* rsl = malloc(sizeof(Result));
    char* index_name;

    if (scol->cls_type == CLSR) {
        index_name = malloc((strlen(scol->name)+strlen(".clsr.btree")+1)*sizeof(char));
        strcpy(index_name, scol->name);
        strcat(index_name, ".clsr.btree");
    }
    else if (scol->cls_type == PRICLSR) {
        index_name = malloc((strlen(scol->name)+strlen(".priclsr.btree")+1)*sizeof(char));
        strcpy(index_name, scol->name);
        strcat(index_name, ".priclsr.btree");
    }
    else if (scol->cls_type == UNCLSR) {
        index_name = malloc((strlen(scol->name)+strlen(".unclsr.btree")+1)*sizeof(char));
        strcpy(index_name, scol->name);
        strcat(index_name, ".unclsr.btree");
    }
    else {
        log_err("[db_manager.c:select_data_col_btree()] the clustered index is not supported.\n");
        return 1;
    }
    if (strncmp(pre_range,"null",4) == 0) {
        long post = strtol(post_range, NULL, 0);
        long* rsl_data = calloc(scol->size, sizeof(long));
        btree btree_index = get_index(index_name);
        btree_kvpair kvpair = btree_search(btree_index, post);
        long row_id = kvpair.row_id;
        for(long i = 0; i < row_id; ++i) {
            rsl_data[i] = i;
        }
    }
    else if (strncmp(post_range,"null",4) == 0) {
        long pre = strtol(pre_range, NULL, 0);
        long* rsl_data = calloc(scol->size, sizeof(long));
        btree btree_index = get_index(index_name);
        btree_kvpair kvpair = btree_search(btree_index, pre);
        size_t count = 0;
        for(long i = kvpair.row_id; i < (long) scol->size; ++i) {
            rsl_data[count] = i;
            count++;
        }
        rsl->num_tuples = count;
        rsl->data_type = LONG;
        rsl->payload = calloc(count, sizeof(long));
        memmove(rsl->payload,rsl_data,count* sizeof(long));
        update_result(handle,rsl);
    }
    else {
        long pre = strtol(pre_range, NULL, 0);
        long post = strtol(post_range, NULL, 0);
        long* rsl_data = calloc(scol->size, sizeof(long));
        btree btree_index = get_index(index_name);
        btree_kvpair kvpair_pre = btree_search(btree_index, pre);
        btree_kvpair kvpair_post = btree_search(btree_index, post);
        size_t count = 0;
        for(long i = kvpair_pre.row_id; i < kvpair_post.row_id; ++i) {
                rsl_data[count] = i;
                count++;
        }
        rsl->num_tuples = count;
        rsl->data_type = LONG;
        rsl->payload = calloc(count, sizeof(long));
        memmove(rsl->payload,rsl_data,count* sizeof(long));
        update_result(handle,rsl);
    }
    return 0;
}

/**
 * select data for the column with sorted index
 **/
int select_data_col_sorted(Column* scol, char* handle, char* pre_range, char* post_range) {
    Result* rsl = malloc(sizeof(Result));

    char* index_name;

    if (scol->cls_type == CLSR) {
        index_name = malloc((strlen(scol->name)+strlen(".clsr.sorted")+1)*sizeof(char));
        strcpy(index_name, scol->name);
        strcat(index_name, ".clsr.sorted");
    }
    else if (scol->cls_type == PRICLSR) {
        index_name = malloc((strlen(scol->name)+strlen(".priclsr.sorted")+1)*sizeof(char));
        strcpy(index_name, scol->name);
        strcat(index_name, ".priclsr.sorted");
    }
    else if (scol->cls_type == UNCLSR) {
        index_name = malloc((strlen(scol->name)+strlen(".unclsr.sorted")+1)*sizeof(char));
        strcpy(index_name, scol->name);
        strcat(index_name, ".unclsr.sorted");
    }
    else {
        log_err("[db_manager.c:select_data_col_sorted()] the clustered index is not supported.\n");
        return 1;
    }
    if (strncmp(pre_range,"null",4) == 0) {
        long post = strtol(post_range, NULL, 0);
        long* rsl_data = calloc(scol->size, sizeof(long));
        linknode* sorted_index = get_index(index_name);
        linknode* node_post = link_search(sorted_index, post);
        long row_id = node_post->row_id;
        for(long i = 0; i < row_id; ++i) {
            rsl_data[i] = i;
        }
    }
    else if (strncmp(post_range,"null",4) == 0) {
        long pre = strtol(pre_range, NULL, 0);
        long* rsl_data = calloc(scol->size, sizeof(long));
        linknode* sorted_index = get_index(index_name);
        linknode* node_pre = link_search(sorted_index, pre);
        size_t count = 0;
        for(long i = node_pre->row_id; i < (long) scol->size; ++i) {
            rsl_data[count] = i;
            count++;
        }
        rsl->num_tuples = count;
        rsl->data_type = LONG;
        rsl->payload = calloc(count, sizeof(long));
        memmove(rsl->payload,rsl_data,count* sizeof(long));
        update_result(handle,rsl);
        free(rsl_data);
    }
    else {
        long pre = strtol(pre_range, NULL, 0);
        long post = strtol(post_range, NULL, 0);
        long* rsl_data = calloc(scol->size, sizeof(long));
        linknode* sorted_index = get_index(index_name);
        linknode* node_pre = link_search(sorted_index, pre);
        linknode* node_post = link_search(sorted_index, post);
        size_t count = 0;
        for(long i = node_pre->row_id; i < node_post->row_id; ++i) {
            rsl_data[count] = i;
            count++;
        }
        rsl->num_tuples = count;
        rsl->data_type = LONG;
        rsl->payload = calloc(count, sizeof(long));
        memmove(rsl->payload,rsl_data,count* sizeof(long));
        update_result(handle,rsl);
        free(rsl_data);
    }
    return 0;
}

/**
 * fetch column data
 **/
int fetch_col_data(char* col_val_name, char* rsl_vec_pos, char* handle) {
    Result* rsl_pos = get_result(rsl_vec_pos);
    if(rsl_pos == NULL) {
        log_err("[db_manager.c:fetch_col_data] fetch position didn't exist.\n");
        return 1;
    }
    Column* col_val = get_column(col_val_name);
    if(col_val == NULL) {
        log_err("[db_manager.c:fetch_col_data] fetch col didn't exist.\n");
        return 1;
    }
    size_t rsl_size = rsl_pos->num_tuples;
    Result* rsl = malloc(sizeof(Result));
    rsl->data_type = LONG;
    rsl->num_tuples = rsl_size;
    rsl->payload = calloc(rsl_size, sizeof(long));
    long* row_id = rsl_pos->payload;
    long* fetch_payload = calloc(rsl_size, sizeof(long));
    for(size_t i = 0; i < rsl_size; ++i) {
        fetch_payload[i] = col_val->data[row_id[i]];
    }
    memmove(rsl->payload, fetch_payload, rsl_size*sizeof(long));
    update_result(handle, rsl);

    free(fetch_payload);
    free(rsl);
    return 0;
}

/**
 * nested-loop join two inputs, given both the values and respective positions of each input.
 **/
int nested_loop_join(char* vec_val_left,
                     char* vec_pos_left,
                     char* vec_val_right,
                     char* vec_pos_right,
                     char* handle_left,
                     char* handle_right) {

    Result* rsl_val_left = get_result(vec_val_left);
    Result* rsl_pos_left = get_result(vec_pos_left);
    Result* rsl_val_right = get_result(vec_val_right);
    Result* rsl_pos_right = get_result(vec_pos_right);
    if(rsl_val_left == NULL) {
        log_err("[db_manager.c:hash_join] fetch left value didn't exist.\n");
        return 1;
    }
    if(rsl_pos_left == NULL) {
        log_err("[db_manager.c:hash_join] fetch left position didn't exist.\n");
        return 1;
    }
    if(rsl_val_right == NULL) {
        log_err("[db_manager.c:hash_join] fetch right value didn't exist.\n");
        return 1;
    }
    if(rsl_pos_right == NULL) {
        log_err("[db_manager.c:hash_join] fetch right position didn't exist.\n");
        return 1;
    }
    Result* rsl_left = malloc(sizeof(Result));
    rsl_left->data_type = LONG;
    long* payload_left = calloc(rsl_pos_left->num_tuples, sizeof(long));

    Result* rsl_right = malloc(sizeof(Result));
    rsl_right->data_type = LONG;
    long* payload_right = calloc(rsl_pos_right->num_tuples, sizeof(long));

    long* val_payload_left = rsl_val_left->payload;
    long* val_payload_right = rsl_val_right->payload;
    long* pos_payload_left = rsl_pos_left->payload;
    long* pos_payload_right = rsl_pos_right->payload;

    size_t count = 0;
    for (size_t i = 0; i < rsl_val_left->num_tuples; ++i) {
        long cur_val_left = val_payload_left[i];
        for (size_t j = 0; j < rsl_val_right->num_tuples; ++j) {
            long cur_val_right = val_payload_right[j];
            if (cur_val_left == cur_val_right) {
                payload_left[count] = pos_payload_left[i];
                payload_right[count] = pos_payload_right[j];
                count++;
            }
        }
    }
    rsl_left->num_tuples = count;
    rsl_right->num_tuples = count;
    rsl_left->payload = calloc(count, sizeof(long));
    memmove(rsl_left->payload, payload_left, count*sizeof(long));
    rsl_right->payload = calloc(count, sizeof(long));
    memmove(rsl_right->payload, payload_right, count*sizeof(long));

    put_result(handle_left, rsl_left);
    put_result(handle_right, rsl_right);
    free(payload_right);
    free(payload_left);
    return 0;
}

long hash_function_join(long hash_input){
    long c1 = 3;
    long c2 = 10000;
    long hash_output = (c1*hash_input + c2) % RAND_PRIME;
    return hash_output;
}

/**
 * hash join two inputs, given both the values and respective positions of each input.
 **/
int hash_join(char* vec_val_left,
              char* vec_pos_left,
              char* vec_val_right,
              char* vec_pos_right,
              char* handle_left,
              char* handle_right) {

    Result* rsl_val_left = get_result(vec_val_left);
    Result* rsl_pos_left = get_result(vec_pos_left);
    Result* rsl_val_right = get_result(vec_val_right);
    Result* rsl_pos_right = get_result(vec_pos_right);
    if(rsl_val_left == NULL) {
        log_err("[db_manager.c:hash_join] fetch left value didn't exist.\n");
        return 1;
    }
    if(rsl_pos_left == NULL) {
        log_err("[db_manager.c:hash_join] fetch left position didn't exist.\n");
        return 1;
    }
    if(rsl_val_right == NULL) {
        log_err("[db_manager.c:hash_join] fetch right value didn't exist.\n");
        return 1;
    }
    if(rsl_pos_right == NULL) {
        log_err("[db_manager.c:hash_join] fetch right position didn't exist.\n");
        return 1;
    }

    long* val_payload_left = rsl_val_left->payload;
    long* val_payload_right = rsl_val_right->payload;
    long* pos_payload_left = rsl_pos_left->payload;
    long* pos_payload_right = rsl_pos_right->payload;

    Result* rsl_left = malloc(sizeof(Result));
    rsl_left->data_type = LONG;
    long* payload_left = calloc(rsl_pos_left->num_tuples, sizeof(long));

    Result* rsl_right = malloc(sizeof(Result));
    rsl_right->data_type = LONG;
    long* payload_right = calloc(rsl_pos_right->num_tuples, sizeof(long));

    long* hash_table_join = calloc(RAND_PRIME, sizeof(long));
    for (size_t i = 0; i < RAND_PRIME; ++i) {
        hash_table_join[i] = -1;
    }

    size_t count = 0;
    if(rsl_val_right->num_tuples > rsl_val_left->num_tuples) {
        for (size_t j = 0; j < rsl_val_left->num_tuples; ++j) {
            hash_table_join[hash_function_join(val_payload_left[j])] = pos_payload_left[j];
        }
        for (size_t k = 0; k < rsl_val_right->num_tuples; ++k) {
            if (hash_table_join[hash_function_join(val_payload_right[k])] != -1) {
                long left_index = hash_table_join[hash_function_join(val_payload_right[k])];
                payload_left[count] = pos_payload_left[left_index];
                payload_right[count] = pos_payload_right[k];
                count++;
            }
        }
    }
    else {
        for (size_t j = 0; j < rsl_val_right->num_tuples; ++j) {
            hash_table_join[hash_function_join(val_payload_right[j])] = pos_payload_right[j];
        }
        for (size_t k = 0; k < rsl_val_left->num_tuples; ++k) {
            if (hash_table_join[hash_function_join(val_payload_left[k])] != -1) {
                long right_index = hash_table_join[hash_function_join(val_payload_left[k])];
                payload_left[count] = pos_payload_left[k];
                payload_right[count] = pos_payload_right[right_index];
                count++;
            }
        }
    }
    rsl_left->num_tuples = count;
    rsl_right->num_tuples = count;
    rsl_left->payload = calloc(count, sizeof(long));
    memmove(rsl_left->payload, payload_left, count*sizeof(long));
    rsl_right->payload = calloc(count, sizeof(long));
    memmove(rsl_right->payload, payload_right, count*sizeof(long));

    put_result(handle_left, rsl_left);
    put_result(handle_right, rsl_right);
    free(hash_table_join);
    free(payload_left);
    free(payload_right);
    return 0;
}


/**
 * get the result of an avg query on the column
 **/
int avg_column_data(char* avg_col_name, char* handle) {
    Column* acol = get_column(avg_col_name);
    if (acol == NULL) {
        log_err("[db_manager.c:avg_col_data()]: column didn't exist in the database.\n");
        return 1;
    }
    long sum = 0;
    long* acol_data = acol->data;
    for(size_t i = 0; i < acol->size; ++i) {
        sum += acol_data[i];
    }
    double avg = (double) sum / (double) acol->size;
    Result* arsl = malloc(sizeof(Result));
    if(arsl == NULL) {
        log_err("[db_manager.c:avg_col_data()]: init new result failed.\n");
        return 1;
    }
    arsl->data_type = FLOAT;
    arsl->num_tuples = 1;
    arsl->payload = calloc(1, sizeof(double));
    memmove(arsl->payload, &avg, sizeof(double));
    update_result(handle,arsl);
    free(arsl);
    return 0;
}

/**
 * compute average of the select "result"
 **/
int avg_result_data(char* avg_rsl_name, char* handle) {
    Result* avg_rsl = get_result(avg_rsl_name);
    double avg = 0;
    if (avg_rsl == NULL) {
        log_err("[db_manager.c:avg_col_data()]: result didn't exist in the database.\n");
        return 1;
    }
    if(avg_rsl->data_type == LONG) {
        long* long_avg_payload = avg_rsl->payload;
        long sum = 0;
        for(size_t i = 0; i < avg_rsl->num_tuples; ++i) {
            sum += long_avg_payload[i];
        }
        avg = (double) sum / (double) avg_rsl->num_tuples;
    }
    else if(avg_rsl->data_type == FLOAT) {
        float* float_avg_payload = avg_rsl->payload;
        float sum = 0;
        for(size_t i = 0; i < avg_rsl->num_tuples; ++i) {
            sum += float_avg_payload[i];
        }
        avg = (double) sum / (double) avg_rsl->num_tuples;
    }
    Result* rsl = malloc(sizeof(Result));
    if(rsl == NULL) {
        log_err("[db_manager.c:avg_col_data()]: init new result failed.\n");
        return 1;
    }
    rsl->data_type = FLOAT;
    rsl->num_tuples = 1;
    rsl->payload = calloc(1, sizeof(double));
    memmove(rsl->payload, &avg, sizeof(double));
    update_result(handle,rsl);
    free(rsl);
    return 0;
}

/**
 * get the result of an sum query on the column
 **/
int sum_column_data(char* sum_col_name, char* handle) {
    Column* sum_col = get_column(sum_col_name);
    if (sum_col == NULL) {
        log_err("[db_manager.c:sum_col_data()]: column didn't exist in the database.\n");
        return 1;
    }
    long sum = 0;
    long* sum_payload = sum_col->data;
    for(size_t i = 0; i < sum_col->size; ++i) {
        sum += sum_payload[i];
    }
    Result* rsl = malloc(sizeof(Result));
    if(rsl == NULL) {
        log_err("[db_manager.c:avg_col_data()]: init new result failed.\n");
        return 1;
    }
    rsl->data_type = LONG;
    rsl->num_tuples = 1;
    rsl->payload = calloc(1, sizeof(long));
    memmove(rsl->payload, &sum, sizeof(long));
    update_result(handle,rsl);
    free(rsl);
    return 0;
}

/**
 * compute sum of the select "result"
 **/
int sum_result_data(char* sum_rsl_name, char* handle) {
    Result* sum_rsl = get_result(sum_rsl_name);
    if (sum_rsl == NULL) {
        log_err("[db_manager.c:sum_rsl_data()]: result didn't exist.\n");
        return 1;
    }
    if(sum_rsl->data_type == FLOAT) {
        double sum = 0;
        double* float_sum_payload = sum_rsl->payload;
        for(size_t i = 0; i < sum_rsl->num_tuples; ++i) {
            sum += float_sum_payload[i];
        }
        Result* rsl = malloc(sizeof(Result));
        if(rsl == NULL) {
            log_err("[db_manager.c:avg_col_data()]: init new result failed.\n");
            return 1;
        }
        rsl->data_type = FLOAT;
        rsl->num_tuples = 1;
        rsl->payload = calloc(1, sizeof(double));
        memmove(rsl->payload, &sum, sizeof(double));
        update_result(handle,rsl);
        free(rsl);
    }
    else if (sum_rsl->data_type == LONG) {
        long sum = 0;
        long* long_sum_payload = sum_rsl->payload;
        for(size_t i = 0; i < sum_rsl->num_tuples; ++i) {
            sum += long_sum_payload[i];
        }
        Result* rsl = malloc(sizeof(Result));
        if(rsl == NULL) {
            log_err("[db_manager.c:avg_col_data()]: init new result failed.\n");
            return 1;
        }
        rsl->data_type = LONG;
        rsl->num_tuples = 1;
        rsl->payload = calloc(1, sizeof(long));
        memmove(rsl->payload, &sum, sizeof(long));
        update_result(handle,rsl);
        free(rsl);
    }
    else {
        log_err("[sum_result_data]:the data type of result is not supported\n");
        return 1;
    }
    return 0;
}

/**
 * adding two numbers
 * left number is from column
 * right number is from column
 **/
int add_col_col(char* add_name1, char* add_name2, char* handle) {
    Column* add1 = get_column(add_name1);
    Column* add2 = get_column(add_name2);
    if(add1->size != add2->size) {
        log_err("two items in add operation have different number records.\n");
        return 1;
    }
    size_t count = add1->size;
    long* add_sum = calloc(count, sizeof(long));
    for(size_t i = 0; i < count; ++i) {
        add_sum[i] = add1->data[i] + add2->data[2];
    }
    Result* rsl = malloc(sizeof(Result));
    rsl->num_tuples = count;
    rsl->data_type = LONG;
    rsl->payload = calloc(count, sizeof(long));
    memmove(rsl->payload, add_sum, count* sizeof(long));
    update_result(handle,rsl);
    return 0;
}

/**
 * adding two numbers
 * left number is from result
 * right number is from result
 **/
int add_rsl_rsl(char* add_name1, char* add_name2, char* handle) {
    Result* add1 = get_result(add_name1);
    Result* add2 = get_result(add_name2);
    if(add1->num_tuples != add2->num_tuples) {
        log_err("two items in add operation have different number records.\n");
        return 1;
    }
    size_t count = add1->num_tuples;
    if(add1->data_type == LONG && add2->data_type == LONG) {
        long* add_sum = calloc(count, sizeof(long));
        long* add1_payload = add1->payload;
        long* add2_payload = add2->payload;
        for(size_t i = 0; i < count; ++i) {
            add_sum[i] = add1_payload[i] + add2_payload[i];
        }
        Result* rsl = malloc(sizeof(Result));
        rsl->num_tuples = count;
        rsl->data_type = LONG;
        rsl->payload = calloc(count, sizeof(long));
        memmove(rsl->payload, add_sum, count* sizeof(long));
        update_result(handle,rsl);
        free(add_sum);
        free(rsl);
        return 0;
    }
    else if (add1->data_type == FLOAT || add2->data_type == FLOAT) {
        double* add_sum = calloc(count, sizeof(double));
        double* add1_payload = add1->payload;
        double* add2_payload = add2->payload;
        for(size_t i = 0; i < count; ++i) {
            add_sum[i] = add1_payload[i] + add2_payload[i];
        }
        Result* rsl = malloc(sizeof(Result));
        rsl->num_tuples = count;
        rsl->data_type = FLOAT;
        rsl->payload = calloc(count, sizeof(double));
        memmove(rsl->payload, add_sum, count* sizeof(double));
        update_result(handle,rsl);
        free(add_sum);
        free(rsl);
        return 0;
    }
    else {
        log_err("[add_rsl_rsl]:data types for adding are unsupported.\n");
        return 1;
    }
}

/**
 * adding two numbers
 * left number is from column
 * right number is from result
 **/
int add_col_rsl(char* add_name1, char* add_name2, char* handle) {
    Column* add1 = get_column(add_name1);
    Result* add2 = get_result(add_name2);
    if(add1->size != add2->num_tuples) {
        log_err("two items in add operation have different number records.\n");
        return 1;
    }
    size_t count = add1->size;
    if (add2->data_type == LONG) {
        long* add_sum = calloc(count, sizeof(long));
        long* add1_payload = add1->data;
        long* add2_payload = add2->payload;
        for(size_t i = 0; i < count; ++i) {
            add_sum[i] = add1_payload[i] + add2_payload[i];
        }
        Result* rsl = malloc(sizeof(Result));
        rsl->num_tuples = count;
        rsl->data_type = LONG;
        rsl->payload = calloc(count, sizeof(long));
        memmove(rsl->payload, add_sum, count* sizeof(long));
        update_result(handle,rsl);
        return 0;
    }
    else if (add2->data_type == FLOAT) {
        double* add_sum = calloc(count, sizeof(double));
        double* add1_payload = (double*)add1->data;
        double* add2_payload = add2->payload;
        for(size_t i = 0; i < count; ++i) {
            add_sum[i] = add1_payload[i] + add2_payload[i];
        }
        Result* rsl = malloc(sizeof(Result));
        rsl->num_tuples = count;
        rsl->data_type = FLOAT;
        rsl->payload = calloc(count, sizeof(double));
        memmove(rsl->payload, add_sum, count* sizeof(double));
        update_result(handle,rsl);
        return 0;
    }
    else {
        log_err("[add_col_rsl]:data types for adding are unsupported.\n");
        return 1;
    }
}

/**
 * adding two numbers
 * left number is from result
 * right number is from column
 **/
int add_rsl_col(char* add_name1, char* add_name2, char* handle) {
    Result* add1 = get_result(add_name1);
    Column* add2 = get_column(add_name2);
    if(add1->num_tuples != add2->size) {
        log_err("two items in add operation have different number records.\n");
        return 1;
    }
    size_t count = add1->num_tuples;
    if (add1->data_type == LONG) {
        long* add_sum = calloc(count, sizeof(long));
        long* add1_payload = add1->payload;
        long* add2_payload = add2->data;
        for(size_t i = 0; i < count; ++i) {
            add_sum[i] = add1_payload[i] + add2_payload[i];
        }
        Result* rsl = malloc(sizeof(Result));
        rsl->num_tuples = count;
        rsl->data_type = LONG;
        rsl->payload = calloc(count, sizeof(long));
        memmove(rsl->payload, add_sum, count* sizeof(long));
        update_result(handle,rsl);
        return 0;
    }
    else if (add1->data_type == FLOAT) {
        double* add_sum = calloc(count, sizeof(double));
        double* add1_payload = add1->payload;
        double* add2_payload = (double *) add2->data;
        for(size_t i = 0; i < count; ++i) {
            add_sum[i] = add1_payload[i] + add2_payload[i];
        }
        Result* rsl = malloc(sizeof(Result));
        rsl->num_tuples = count;
        rsl->data_type = FLOAT;
        rsl->payload = calloc(count, sizeof(double));
        memmove(rsl->payload, add_sum, count* sizeof(double));
        update_result(handle,rsl);
        return 0;
    }
    else {
        log_err("[add_rsl_col]:data types for adding are unsupported.\n");
        return 1;
    }
}

/**
 * subtraction
 * left number is from column
 * right number is from column
 **/
int sub_col_col(char* sub_name1, char* sub_name2, char* handle) {
    Column* sub1 = get_column(sub_name1);
    Column* sub2 = get_column(sub_name2);
    if(sub1->size != sub2->size) {
        log_err("two items in sub operation have different number records.\n");
        return 1;
    }
    size_t count = sub1->size;
    long* sub_sum = calloc(count, sizeof(long));
    for(size_t i = 0; i < count; ++i) {
        sub_sum[i] = sub1->data[i] - sub2->data[i];
    }
    Result* rsl = malloc(sizeof(Result));
    rsl->num_tuples = count;
    rsl->data_type = LONG;
    rsl->payload = calloc(count, sizeof(long));
    memmove(rsl->payload, sub_sum, count* sizeof(long));
    update_result(handle,rsl);
    return 0;
}

/**
 * subtraction
 * left number is from result
 * right number is from result
 **/
int sub_rsl_rsl(char* sub_name1, char* sub_name2, char* handle) {
    Result* sub1 = get_result(sub_name1);
    Result* sub2 = get_result(sub_name2);
    if(sub1->num_tuples != sub2->num_tuples) {
        log_err("two items in add operation have different number records.\n");
        return 1;
    }
    size_t count = sub1->num_tuples;
    if(sub1->data_type == LONG && sub2->data_type == LONG) {
        long* sub_sum = calloc(count, sizeof(long));
        long* sub1_payload = sub1->payload;
        long* sub2_payload = sub2->payload;
        for(size_t i = 0; i < count; ++i) {
            sub_sum[i] = sub1_payload[i] - sub2_payload[i];
        }
        Result* rsl = malloc(sizeof(Result));
        rsl->num_tuples = count;
        rsl->data_type = LONG;
        rsl->payload = calloc(count, sizeof(long));
        memmove(rsl->payload, sub_sum, count* sizeof(long));
        update_result(handle,rsl);
        free(sub_sum);
        free(rsl);
        return 0;
    }
    else if (sub1->data_type == FLOAT || sub2->data_type == FLOAT) {
        double* sub_sum = calloc(count, sizeof(double));
        double* sub1_payload = sub1->payload;
        double* sub2_payload = sub2->payload;
        for(size_t i = 0; i < count; ++i) {
            sub_sum[i] = sub1_payload[i] - sub2_payload[i];
        }
        Result* rsl = malloc(sizeof(Result));
        rsl->num_tuples = count;
        rsl->data_type = FLOAT;
        rsl->payload = calloc(count, sizeof(double));
        memmove(rsl->payload, sub_sum, count* sizeof(double));
        update_result(handle,rsl);
        free(sub_sum);
        free(rsl);
        return 0;
    }
    else {
        log_err("[sub_rsl_rsl]:data types for adding are unsupported.\n");
        return 1;
    }
}

/**
 * max value from results
 **/
int max_rsl_value(char* max_vec, char* handle) {
    Result* vrsl = get_result(max_vec);
    if(vrsl == NULL) {
        log_err("the result %s for max didn't exist in the database", max_vec);
        return 1;
    }
    if(vrsl->data_type == LONG) {
        long* vrsl_payload = vrsl->payload;
        long max = vrsl_payload[0];
        size_t max_cnt = vrsl->num_tuples;
        for(size_t i = 1; i < max_cnt; ++i) {
            if(max < vrsl_payload[i]) {
                max = vrsl_payload[i];
            }
        }
        Result* rsl = malloc(sizeof(Result));
        rsl->data_type = LONG;
        rsl->num_tuples = 1;
        rsl->payload = calloc(1, sizeof(long));
        memmove(rsl->payload, &max, sizeof(long));
        update_result(handle,rsl);
        free(rsl);
        return 0;
    }
    else if (vrsl->data_type == FLOAT) {
        double* vrsl_payload = vrsl->payload;
        double max = vrsl_payload[0];
        size_t max_cnt = vrsl->num_tuples;
        for(size_t i = 1; i < max_cnt; ++i) {
            if(max < vrsl_payload[i]) {
                max = vrsl_payload[i];
            }
        }
        Result* rsl = malloc(sizeof(Result));
        rsl->data_type = FLOAT;
        rsl->num_tuples = 1;
        rsl->payload = calloc(1, sizeof(double));
        memmove(rsl->payload, &max, sizeof(double));
        update_result(handle,rsl);
        free(rsl);
        return 0;
    }
    else {
        log_err("[max_rsl_value]:the data type is not supported\n");
        return 1;
    }
}

/**
 * max value from results with position
 **/
int max_rsl_value_pos(char* max_vec_pos, char* max_vec_value, char* handle_pos, char* handle_value) {
    Result* rsl_pos = get_result(max_vec_pos);
    Result* rsl_value = get_result(max_vec_value);
    if(rsl_pos == NULL || rsl_value == NULL) {
        log_err("the result %s/%s for max didn't exist in the database", max_vec_pos, max_vec_value);
        return 1;
    }
    if (rsl_value->data_type == LONG) {
        long* rsl_value_payload = rsl_value->payload;
        long max = rsl_value_payload[0];
        size_t max_cnt = rsl_value->num_tuples;
        size_t pmax = 0;
        for(size_t i = 1; i < max_cnt; ++i) {
            if(max < rsl_value_payload[i]) {
                max = rsl_value_payload[i];
                pmax = i;
            }
        }
        long* max_pos_payload = rsl_pos->payload;
        long max_pos = max_pos_payload[pmax];

        Result* vrsl = malloc(sizeof(Result));
        vrsl->data_type = LONG;
        vrsl->num_tuples = 1;
        vrsl->payload = calloc(1, sizeof(long));
        memmove(vrsl->payload, &max, sizeof(long));
        update_result(handle_value,vrsl);

        Result* prsl = malloc(sizeof(Result));
        prsl->data_type = LONG;
        prsl->num_tuples = 1;
        prsl->payload = calloc(1, sizeof(long));
        memmove(prsl->payload, &max_pos, sizeof(long));
        update_result(handle_pos,prsl);
        return 0;
    }
    else if (rsl_value->data_type == FLOAT) {
        double* rsl_value_payload = rsl_value->payload;
        double max = rsl_value_payload[0];
        size_t max_cnt = rsl_value->num_tuples;
        size_t pmax = 0;
        for(size_t i = 1; i < max_cnt; ++i) {
            if(max < rsl_value_payload[i]) {
                max = rsl_value_payload[i];
                pmax = i;
            }
        }
        long* max_pos_payload = rsl_pos->payload;
        long max_pos = max_pos_payload[pmax];

        Result* vrsl = malloc(sizeof(Result));
        vrsl->data_type = FLOAT;
        vrsl->num_tuples = 1;
        vrsl->payload = calloc(1, sizeof(double));
        memmove(vrsl->payload, &max, sizeof(double));
        update_result(handle_value,vrsl);

        Result* prsl = malloc(sizeof(Result));
        prsl->data_type = LONG;
        prsl->num_tuples = 1;
        prsl->payload = calloc(1, sizeof(long));
        memmove(prsl->payload, &max_pos, sizeof(long));
        update_result(handle_pos,prsl);
        return 0;
    }
    else {
        log_err("[max_rsl_value_pos]:the data type is not supported\n");
        return 1;
    }
}

/**
 * min value from results
 **/
int min_rsl_value(char* min_vec,char* handle) {
    Result* vrsl = get_result(min_vec);
    if(vrsl == NULL) {
        log_err("the result %s for min didn't exist in the database", min_vec);
        return 1;
    }
    if(vrsl->data_type == LONG) {
        long* vrsl_payload = vrsl->payload;
        long min = vrsl_payload[0];
        size_t min_cnt = vrsl->num_tuples;
        for(size_t i = 1; i < min_cnt; ++i) {
            if(min > vrsl_payload[i]) {
                min = vrsl_payload[i];
            }
        }
        Result* rsl = malloc(sizeof(Result));
        rsl->data_type = LONG;
        rsl->num_tuples = 1;
        rsl->payload = calloc(1, sizeof(long));
        memmove(rsl->payload, &min, sizeof(long));
        update_result(handle,rsl);
        free(rsl);
        return 0;
    }
    else if (vrsl->data_type == FLOAT) {
        double* vrsl_payload = vrsl->payload;
        double min = vrsl_payload[0];
        size_t min_cnt = vrsl->num_tuples;
        for(size_t i = 1; i < min_cnt; ++i) {
            if(min > vrsl_payload[i]) {
                min = vrsl_payload[i];
            }
        }
        Result* rsl = malloc(sizeof(Result));
        rsl->data_type = FLOAT;
        rsl->num_tuples = 1;
        rsl->payload = calloc(1, sizeof(double));
        memmove(rsl->payload, &min, sizeof(double));
        update_result(handle,rsl);
        free(rsl);
        return 0;
    }
    else {
        log_err("[min_rsl_value]:the data type is not supported\n");
        return 1;
    }
}

/**
 * min value from results with position
 **/
int min_rsl_value_pos(char* min_vec_pos, char* min_vec_value, char* handle_pos, char* handle_value) {
    Result* rsl_pos = get_result(min_vec_pos);
    Result* rsl_value = get_result(min_vec_value);
    if(rsl_pos == NULL || rsl_value == NULL) {
        log_err("the result %s/%s for min didn't exist in the database", min_vec_pos, min_vec_value);
        return 1;
    }
    if (rsl_value->data_type == LONG) {
        long* rsl_value_payload = rsl_value->payload;
        long min = rsl_value_payload[0];
        size_t min_cnt = rsl_value->num_tuples;
        size_t pmin = 0;
        for(size_t i = 1; i < min_cnt; ++i) {
            if(min > rsl_value_payload[i]) {
                min = rsl_value_payload[i];
                pmin = i;
            }
        }
        long* min_pos_payload = rsl_pos->payload;
        long min_pos = min_pos_payload[pmin];

        Result* vrsl = malloc(sizeof(Result));
        vrsl->data_type = LONG;
        vrsl->num_tuples = 1;
        vrsl->payload = calloc(1, sizeof(long));
        memmove(vrsl->payload, &min, sizeof(long));
        update_result(handle_value,vrsl);

        Result* prsl = malloc(sizeof(Result));
        prsl->data_type = LONG;
        prsl->num_tuples = 1;
        prsl->payload = calloc(1, sizeof(long));
        memmove(prsl->payload, &min_pos, sizeof(long));
        update_result(handle_pos,prsl);
        return 0;
    }
    else if (rsl_value->data_type == FLOAT) {
        double* rsl_value_payload = rsl_value->payload;
        double min = rsl_value_payload[0];
        size_t min_cnt = rsl_value->num_tuples;
        size_t pmin = 0;
        for(size_t i = 1; i < min_cnt; ++i) {
            if(min > rsl_value_payload[i]) {
                min = rsl_value_payload[i];
                pmin = i;
            }
        }
        long* min_pos_payload = rsl_pos->payload;
        long min_pos = min_pos_payload[pmin];

        Result* vrsl = malloc(sizeof(Result));
        vrsl->data_type = FLOAT;
        vrsl->num_tuples = 1;
        vrsl->payload = calloc(1, sizeof(double));
        memmove(vrsl->payload, &min, sizeof(double));
        update_result(handle_value,vrsl);

        Result* prsl = malloc(sizeof(Result));
        prsl->data_type = LONG;
        prsl->num_tuples = 1;
        prsl->payload = calloc(1, sizeof(long));
        memmove(prsl->payload, &min_pos, sizeof(long));
        update_result(handle_pos,prsl);
        return 0;
    }
    else {
        log_err("[min_rsl_value_pos]:the data type is not supported\n");
        return 1;
    }
}

/**
 * print result
 **/
char* generate_print_result(size_t print_num, char** print_name) {
    size_t rsl_total_tuples = 0;
    size_t rsl_total_size = 0;
    for(size_t i = 0; i< print_num; ++i) {
        Result* rsl = get_result(print_name[i]);
        rsl_total_tuples += rsl->num_tuples;
        if(rsl->data_type == LONG) {
            for(size_t j = 0; j < rsl->num_tuples; ++j) {
                log_info("%ld\n",((long *)rsl->payload)[j]);
                rsl_total_size += count_digits(((long *)rsl->payload)[j]) + 3;
            }
        }
        else if(rsl->data_type == FLOAT) {
            for(size_t j = 0; j < rsl->num_tuples; ++j) {
                log_info("%0.2f\n",((double *)rsl->payload)[j]);
                rsl_total_size += count_digits(((long *)rsl->payload)[j]) + 6;
            }
        }
        else {
            log_err("[generate_print_result]:the data type is not supported\n");
        }
    }
    char* print_rsl = calloc(rsl_total_size, sizeof(char));
    strcpy(print_rsl, "");
    log_info("[Server results]\n");
    for(size_t i = 0; i< print_num; ++i) {
        Result* rsl = get_result(print_name[i]);
        if(rsl->data_type == LONG) {
            for(size_t j = 0; j < rsl->num_tuples; ++j) {
                log_info("%ld\n",((long *)rsl->payload)[j]);
                size_t num_digit = count_digits(((long *)rsl->payload)[j]);
                char* tmp_payload_data = calloc(num_digit+3, sizeof(char));
                sprintf(tmp_payload_data, "%ld\n", ((long *)rsl->payload)[j]);
                strcat(print_rsl,tmp_payload_data);
                free(tmp_payload_data);
            }

        }
        else if(rsl->data_type == FLOAT) {
            for(size_t j = 0; j < rsl->num_tuples; ++j) {
                log_info("%0.2f\n",((double *)rsl->payload)[j]);
                size_t num_digit = count_digits(((long *)rsl->payload)[j]);
                char* tmp_payload_data = calloc(num_digit+6, sizeof(char));
                sprintf(tmp_payload_data, "%0.2f\n", ((double *)rsl->payload)[j]);
                strcat(print_rsl,tmp_payload_data);
                free(tmp_payload_data);
            }
        }
        else {
            log_err("[generate_print_result]:the data type is not supported\n");
        }
    }
    log_info("\n");
    return print_rsl;
}


/**
 * read data from csv file
 **/
int read_csv(char* data_path) {
    message_status mes_status = OK_DONE;
    FILE *fp;
    if((fp=fopen(data_path,"r"))==NULL) {
        log_err("[db_manager.c:load_data_csv()] cannot load data %s\n", data_path);
        return 1;
    }
    char* line_free = NULL;
    size_t len = 0;
    char* line = line_free;
    ssize_t read = getline(&line, &len, fp);
    if (read == -1) {
        log_err("[db_manager.c:load_data_csv()] read file header failed.\n");
        return 1;
    }

    char* line_copy = calloc(strlen(line)+1, sizeof(char));
    memmove(line_copy,line, strlen(line)+1);
    char* line_copy_free = line_copy;
    size_t header_count = 0;
    char* sepTmp = NULL;
    while(true) {
        sepTmp = next_token_comma(&line_copy,&mes_status);
        if(sepTmp == NULL) {
            break;
        }
        else {
            header_count++;
        }
    }
    log_info("%d columns in the loading file\n", header_count);
    free(line_copy_free);

    /**
     * load the csv file that only has one column
     **/
    if (header_count == 1) {
        char* header = trim_newline(line);
        Column* lcol = get_column(header);
        if (lcol == NULL) {
            log_err("[db_manager.c:load_data_csv] cannot find column %s in database\n", header);
            fclose(fp);
            return 1;
        }
        if (lcol->idx_type == UNIDX) {
            long rowId_load = 0;
            char* line_unidx_free = NULL;
            size_t len_unidx = 0;
            char* line_unidx = line_unidx_free;
            while ((getline(&line_unidx, &len_unidx, fp)) != -1) {
                char *va = line_unidx;
                long lv = strtol(va, NULL, 0);
                if(insert_data_column(lcol, lv, rowId_load) != 0) {
                    fclose(fp);
                    return 1;
                }
                rowId_load++;
            }
            free(line_unidx);
            free(line_unidx_free);
        }
        else if (lcol->idx_type == BTREE) {
            char* index_name;
            if (lcol->cls_type == UNCLSR) {
                index_name = malloc((strlen(lcol->name)+strlen(".unclsr.btree")+1)*sizeof(char));
                strcpy(index_name, lcol->name);
                strcat(index_name, ".unclsr.btree");
            }
            else if (lcol->cls_type == PRICLSR) {
                index_name = malloc((strlen(lcol->name)+strlen(".priclsr.btree")+1)*sizeof(char));
                strcpy(index_name, lcol->name);
                strcat(index_name, ".priclsr.btree");
            }
            else if (lcol->cls_type == CLSR) {
                index_name = malloc((strlen(lcol->name)+strlen(".clsr.btree")+1)*sizeof(char));
                strcpy(index_name, lcol->name);
                strcat(index_name, ".clsr.btree");
            }
            else {
                log_err("clustered index is not supported\n");
                return 1;
            }
            btree btree_index = btree_init();
            long rowId_load = 0;
            while ((getline(&line, &len, fp)) != -1) {
                char *va = line;
                long lv = strtol(va, NULL, 0);
                btree_kvpair kv_node = {rowId_load, lv};
                btree_insert(btree_index, kv_node);
                rowId_load++;
            }
            put_index(index_name, btree_index, BTREE);

            //store the data according to index since it is clustered
            long value_array[MAX_COLUMN_SIZE];
            long row_id_array[MAX_COLUMN_SIZE];
            if (lcol->cls_type == PRICLSR || lcol->cls_type == CLSR) {
                //int tmp_idx = 0;
                //long max_idx = btree_inorder_traversal(btree_index, value_array, row_id_array, tmp_idx);
                long btree_count = 0;
                long *p = &btree_count;
                btree_inorder_traversal(btree_index, value_array, row_id_array, p);

                for(long i = 0; i < btree_count; ++i) {
                    if(insert_data_column(lcol, value_array[i], row_id_array[i]) != 0) {
                        return 1;
                    }
                }
            }
            free(index_name);

        }
        else if (lcol->idx_type == SORTED) {
            char* index_name;
            if (lcol->cls_type == UNCLSR) {
                index_name = malloc((strlen(lcol->name)+strlen(".unclsr.sorted")+1)*sizeof(char));
                strcpy(index_name, lcol->name);
                strcat(index_name, ".unclsr.sorted");
            }
            else if (lcol->cls_type == PRICLSR) {
                index_name = malloc((strlen(lcol->name)+strlen(".priclsr.sorted")+1)*sizeof(char));
                strcpy(index_name, lcol->name);
                strcat(index_name, ".priclsr.sorted");
            }
            else if (lcol->cls_type == CLSR) {
                index_name = malloc((strlen(lcol->name)+strlen(".clsr.sorted")+1)*sizeof(char));
                strcpy(index_name, lcol->name);
                strcat(index_name, ".clsr.sorted");
            }
            else {
                log_err("clustered index is not supported\n");
                return 1;
            }
            linknode* sorted_index = link_init();
            int rowId_load = 0;
            while ((getline(&line, &len, fp)) != -1) {
                char *va = line;
                long lv = strtol(va, NULL, 0);
                link_insert_head(sorted_index, rowId_load, lv);
                rowId_load++;
            }
            sorted_index = link_sort(sorted_index);
            put_index(index_name, sorted_index, SORTED);

            //store the data according to index since it is pricluster
            long value_array[MAX_COLUMN_SIZE];
            long row_id_array[MAX_COLUMN_SIZE];
            long max_idx = link_traversal(sorted_index, value_array, row_id_array);
            for(int i = 0; i < max_idx; ++i) {
                if(insert_data_column(lcol, value_array[i], row_id_array[i]) != 0) {
                    return 1;
                }
            }
            free(index_name);
        }
        else {
            log_err("the index type is not supported.\n");
            return 1;
        }
    }
    /**
     * load the csv file that has multiple columns
     **/
    else {
        char* header = trim_newline(line);
        Column** col_set = calloc(header_count, sizeof(Column*));
        for(size_t i = 0; i < header_count; ++i) {
            char* col_name = next_token_comma(&header, &mes_status);
            col_set[i] = get_column(col_name);
            if (col_set[i] == NULL) {
                log_err("[db_manager.c:load_data_csv] cannot find column %s in database\n", col_name);
                fclose(fp);
                return 1;
            }
        }
        long rowId_load = 0;
        char* line_mc = NULL;
        size_t len_mc = 0;
        char* line_mc_free = line_mc;
        while ((getline(&line_mc, &len_mc, fp)) != -1) {
            char* line_mc_head = calloc(strlen(line_mc)+1, sizeof(char));
            memmove(line_mc_head,line_mc, (strlen(line_mc)+1)*sizeof(char));
            char* line_mc_head_free = line_mc_head;
            for (size_t i = 0; i < header_count; ++i) {
                char *va = next_token_comma(&line_mc_head, &mes_status);
                long lv = strtol(va, NULL, 0);
                if (col_set[i]->idx_type == UNIDX) {
                    if (insert_data_column(col_set[i], lv, rowId_load) != 0) {
                        fclose(fp);
                        return 1;
                    }
                }
                else if (col_set[i]->idx_type == BTREE) {
                    char* index_name;
                    if (col_set[i]->cls_type == UNCLSR) {
                        index_name = malloc((strlen(col_set[i]->name)+strlen(".unclsr.btree")+1)*sizeof(char));
                        strcpy(index_name, col_set[i]->name);
                        strcat(index_name, ".unclsr.btree");
                        if (insert_data_column(col_set[i], lv, rowId_load) != 0) {
                            fclose(fp);
                            return 1;
                        }
                    }
                    else if (col_set[i]->cls_type == PRICLSR) {
                        index_name = malloc((strlen(col_set[i]->name)+strlen(".priclsr.btree")+1)*sizeof(char));
                        strcpy(index_name, col_set[i]->name);
                        strcat(index_name, ".priclsr.btree");
                    }
                    else if (col_set[i]->cls_type == CLSR) {
                        index_name = malloc((strlen(col_set[i]->name)+strlen(".clsr.btree")+1)*sizeof(char));
                        strcpy(index_name, col_set[i]->name);
                        strcat(index_name, ".clsr.btree");
                    }
                    else {
                        log_err("clustered index is not supported\n");
                        return 1;
                    }
                    log_info("index name:%s\n", index_name);
                    btree btree_index = get_index(index_name);
                    if (btree_index == NULL) {
                        btree_index = btree_init();
                        btree_kvpair kv_node = {rowId_load, lv};
                        btree_insert(btree_index, kv_node);
                        log_info("create a btree index:%s \n",index_name);
                        put_index(index_name, btree_index, BTREE);
                    }
                    else {
                        btree btree_index_cpy = btree_init();
                        memmove(btree_index_cpy, btree_index, sizeof(struct bt_node));
                        delete_index(index_name);
                        btree_kvpair kv_node = {rowId_load, lv};
                        btree_insert(btree_index_cpy, kv_node);
                        put_index(index_name, btree_index_cpy, BTREE);
                    }
                    free(index_name);
                }
                else if (col_set[i]->idx_type == SORTED) {
                    char* index_name;
                    if (col_set[i]->cls_type == UNCLSR) {
                        index_name = malloc((strlen(col_set[i]->name)+strlen(".unclsr.sorted")+1)*sizeof(char));
                        strcpy(index_name, col_set[i]->name);
                        strcat(index_name, ".unclsr.sorted");
                        if (insert_data_column(col_set[i], lv, rowId_load) != 0) {
                            fclose(fp);
                            return 1;
                        }
                    }
                    else if (col_set[i]->cls_type == PRICLSR) {
                        index_name = malloc((strlen(col_set[i]->name)+strlen(".priclsr.sorted")+1)*sizeof(char));
                        strcpy(index_name, col_set[i]->name);
                        strcat(index_name, ".priclsr.sorted");
                    }
                    else if (col_set[i]->cls_type == CLSR) {
                        index_name = malloc((strlen(col_set[i]->name)+strlen(".clsr.sorted")+1)*sizeof(char));
                        strcpy(index_name, col_set[i]->name);
                        strcat(index_name, ".clsr.sorted");
                    }
                    else {
                        log_err("clustered index is not supported\n");
                        return 1;
                    }
                    linknode* sorted_index = get_index(index_name);
                    if (sorted_index == NULL) {
                        sorted_index = link_init();
                        sorted_index = link_insert_head(sorted_index, rowId_load, lv);
                        sorted_index = link_sort(sorted_index);
                        log_info("create a sorted index:%s \n", index_name);
                        put_index(index_name, sorted_index, SORTED);
                    }
                    else {
                        //log_info("find the sorted index:%s \n", index_name);
                        linknode* sorted_index_cpy = link_init();
                        memmove(sorted_index_cpy, sorted_index, sizeof(linknode));
                        delete_index(index_name);
                        sorted_index_cpy = link_insert_head(sorted_index_cpy, rowId_load, lv);
                        sorted_index_cpy = link_sort(sorted_index_cpy);
                        put_index(index_name, sorted_index_cpy, SORTED);
                    }
                    free(index_name);
                }
                else {
                    log_err("index is not supported\n");
                    return 1;
                }
            }
            rowId_load++;
            free(line_mc_head_free);
        }
        free(line_mc);
        free(line_mc_free);
        /**
         * store the data according to index
         */
        log_info("start to store the date according to index\n");
        for (size_t col_idx = 0; col_idx < header_count; ++col_idx) {
            long value_array[MAX_COLUMN_SIZE];
            long row_id_array[MAX_COLUMN_SIZE];
            char* index_name;
            if (col_set[col_idx]->idx_type == BTREE) {
                if (col_set[col_idx]->cls_type == CLSR) {
                    index_name = malloc((strlen(col_set[col_idx]->name)+strlen(".clsr.btree")+1)*sizeof(char));
                    strcpy(index_name, col_set[col_idx]->name);
                    strcat(index_name, ".clsr.btree");
                }
                else if (col_set[col_idx]->cls_type == PRICLSR) {
                    index_name = malloc((strlen(col_set[col_idx]->name)+strlen(".priclsr.btree")+1)*sizeof(char));
                    strcpy(index_name, col_set[col_idx]->name);
                    strcat(index_name, ".priclsr.btree");
                }
                else {
                    log_info("the index is not clustered\n");
                    continue;
                }
                btree btree_index = get_index(index_name);
                long btree_count = 0;
                long *p = &btree_count;

                btree_inorder_traversal(btree_index, value_array, row_id_array, p);

                for(long j = 0; j < btree_count; ++j) {
                    if(insert_data_column(col_set[col_idx], value_array[j], row_id_array[j]) != 0) {
                        return 1;
                    }
                }
            }
            else if (col_set[col_idx]->idx_type == SORTED) {
                if (col_set[col_idx]->cls_type == CLSR) {
                    index_name = malloc((strlen(col_set[col_idx]->name)+strlen(".clsr.sorted")+1)*sizeof(char));
                    strcpy(index_name, col_set[col_idx]->name);
                    strcat(index_name, ".clsr.sorted");
                }
                else if (col_set[col_idx]->cls_type == PRICLSR) {
                    index_name = malloc((strlen(col_set[col_idx]->name)+strlen(".priclsr.sorted")+1)*sizeof(char));
                    strcpy(index_name, col_set[col_idx]->name);
                    strcat(index_name, ".priclsr.sorted");
                }
                else {
                    log_info("the index is not clustered\n");
                    continue;
                }
                linknode* sorted_index = get_index(index_name);
                long max_idx = link_traversal(sorted_index, value_array, row_id_array);
                for(size_t k = 0; k < (size_t) max_idx; ++k) {
                    if(insert_data_column(col_set[col_idx], value_array[k], row_id_array[k]) != 0) {
                        return 1;
                    }
                }
            }
            else {
                log_info("no index for this column\n");
            }
        }
        free(col_set);
    }
    free(line);
    free(line_free);
    fclose(fp);
    return 0;
}

/**
 * set the index type and clustered type
 **/
int set_column_idx_cls(Column* slcol, char* idx_type, char* cls_type) {
    if (strcmp(idx_type,"unidx") == 0) {
        slcol->idx_type = UNIDX;
        slcol->cls_type = UNCLSR;
    }
    else if (strcmp(idx_type,"btree") == 0 && strcmp(cls_type,"priclsr") == 0) {
        slcol->idx_type = BTREE;
        slcol->cls_type = PRICLSR;
        Table* tbl_aff = get_table(slcol->aff_tbl_name);
        tbl_aff->has_cluster_index = 1;
        tbl_aff->pricluster_index_col_name = malloc((strlen(slcol->name)+1)* sizeof(char));
        strcpy(tbl_aff->pricluster_index_col_name, slcol->name);
    }
    else if (strcmp(idx_type,"btree") == 0 && strcmp(cls_type,"clsr") == 0) {
        slcol->idx_type = BTREE;
        slcol->cls_type = CLSR;
    }
    else if (strcmp(idx_type,"btree") == 0 && strcmp(cls_type,"unclsr") == 0) {
        slcol->idx_type = BTREE;
        slcol->cls_type = UNCLSR;
    }
    else if (strcmp(idx_type,"sorted") == 0 && strcmp(cls_type,"priclsr") == 0) {
        slcol->idx_type = SORTED;
        slcol->cls_type = PRICLSR;
        Table* tbl_aff = get_table(slcol->aff_tbl_name);
        tbl_aff->has_cluster_index = 1;
        tbl_aff->pricluster_index_col_name = malloc((strlen(slcol->name)+1)* sizeof(char));
        strcpy(tbl_aff->pricluster_index_col_name, slcol->name);
    }
    else if (strcmp(idx_type,"sorted") == 0 && strcmp(cls_type,"clsr") == 0) {
        slcol->idx_type = SORTED;
        slcol->cls_type = CLSR;
    }
    else if (strcmp(idx_type,"sorted") == 0 && strcmp(cls_type,"unclsr") == 0) {
        slcol->idx_type = SORTED;
        slcol->cls_type = UNCLSR;
    }
    return 0;
}

int load_database() {
    char cwd[DIRLEN];
    if (getcwd(cwd, DIRLEN) == NULL) {
        log_err("current working dir path is too long");
        return 1;
    }
    strcat(cwd,"/");
    mkdir("db",0777);
    strcat(cwd,"db/");

    struct dirent* filename;
    DIR *pDir = opendir(cwd);
    if(pDir == NULL) {
        log_err("open dir %s error!\n",cwd);
        return 1;
    }
    message_status mes_status;
    while((filename = readdir(pDir)) != NULL) {
        if(strcmp(filename->d_name,".") != 0 && strcmp(filename->d_name,"..") != 0 && is_csv(filename->d_name) == true) {
            log_info("loading data %s into database\n", filename->d_name);
            char* db_file = malloc(sizeof(char) * ((strlen(cwd)+strlen(filename->d_name)+1)));
            strcpy(db_file,cwd);
            strcat(db_file,filename->d_name);
            FILE *fp;
            if((fp=fopen(db_file,"r"))==NULL) {
                log_err("cannot load file %s.\n", db_file);
                return 1;
            }
            char* line = NULL;
            size_t len = 0;
            while ((getline(&line, &len, fp)) != -1) {
                char* line_copy = malloc((strlen(line)+1) * sizeof(char));
                memmove(line_copy,line, (strlen(line)+1)*sizeof(char));
                char* line_copy_free = line_copy;
                line_copy = trim_newline(line_copy);
                mes_status = OK_DONE;
                char* db_name = next_token_comma(&line_copy,&mes_status);
                char* tbl_name = next_token_comma(&line_copy,&mes_status);
                char* tbl_pricls_col_name = next_token_comma(&line_copy,&mes_status);
                char* tbl_capacity = next_token_comma(&line_copy,&mes_status);
                char* col_name = next_token_comma(&line_copy,&mes_status);
                char* idx_type = next_token_comma(&line_copy,&mes_status);
                char* cls_type = next_token_comma(&line_copy,&mes_status);
                if(mes_status == INCORRECT_FORMAT) {
                    log_err("[db_manager.c:setup_db_csv()] tokenizing data failed.\n");
                    free(line);
                    return 1;
                }
                current_db = create_db(db_name);
                if(current_db == NULL) {
                    log_err("[db_manager.c:setup_db_csv()] setup database failed.\n");
                    free(line);
                    return 1;
                }
                Table* setup_tbl = create_table(db_name, tbl_name, tbl_pricls_col_name,
                                                (size_t) strtol(tbl_capacity, NULL, 0));
                if(setup_tbl == NULL) {
                    log_err("[db_manager.c:setup_db_csv()] setup table failed.\n");
                    free(line);
                    return 1;
                }
                Column* scol = create_column(tbl_name, col_name);
                if(scol == NULL) {
                    log_err("[db_manager.c:setup_db_csv()] setup column failed.\n");
                    free(line);
                    return 1;
                }
                log_info("table size:%d\n",setup_tbl->size);
                Column* setup_col = get_column(col_name);

                // setup the index for each column
                if(set_column_idx_cls(setup_col, idx_type, cls_type) != 0) {
                    log_err("[db_manager.c:setup_db_csv()] setup column index and clustering failed.\n");
                    free(line);
                    return 1;
                }

                char* slvle = NULL;
                long count = 0;
                while ((slvle = next_token_comma(&line_copy,&mes_status))!= NULL) {
                    if (count % 2 == 0) {
                        long rlv = strtol(slvle, NULL, 0);
                        if (setup_col->size >= setup_col->capacity) {
                            size_t new_column_length = RESIZE * setup_col->capacity + 1;
                            size_t new_length = new_column_length;
                            size_t old_length = setup_col->capacity;
                            if (old_length == 0) {
                                assert(new_length > 0);
                                setup_col->data = calloc(new_length, sizeof(long));
                                setup_col->rowId = calloc(new_length, sizeof(long));
                            } else {
                                setup_col->data = resize_long(setup_col->data, old_length, new_length);
                                setup_col->rowId = resize_long(setup_col->rowId, old_length, new_length);
                                if (!setup_col->data || !setup_col->rowId) {
                                    log_err("creating more data space failed.\n");
                                    free(setup_col->data);
                                    free(setup_col->rowId);
                                    free(setup_col);
                                }
                            }
                            setup_col->capacity = new_length;
                        }
                        setup_col->rowId[setup_col->size] = rlv;
                    } else {
                        long slv = strtol(slvle, NULL, 0);
                        setup_col->data[setup_col->size] = slv;
                        setup_col->size++;
                    }
                    count++;
                }

                /**
                 * building the index for each column after reading
                 */
                char* index_name;

                if(setup_col->idx_type == BTREE) {
                    if (setup_col->cls_type == UNCLSR) {
                        index_name = malloc((strlen(setup_col->name)+strlen(".unclsr.btree")+1)*sizeof(char));
                        strcpy(index_name, setup_col->name);
                        strcat(index_name, ".unclsr.btree");
                    }
                    else if (setup_col->cls_type == PRICLSR) {
                        index_name = malloc((strlen(setup_col->name)+strlen(".priclsr.btree")+1)*sizeof(char));
                        strcpy(index_name, setup_col->name);
                        strcat(index_name, ".priclsr.btree");
                    }
                    else if (setup_col->cls_type == CLSR) {
                        index_name = malloc((strlen(setup_col->name)+strlen(".clsr.btree")+1)*sizeof(char));
                        strcpy(index_name, setup_col->name);
                        strcat(index_name, ".clsr.btree");
                    }
                    else {
                        log_err("clustered index is not supported\n");
                        free(line);
                        return 1;
                    }
                    btree btree_index = btree_init();
                    log_info("create a btree index:%s \n",index_name);
                    for(size_t i = 0; i < setup_col->size; ++i) {
                        btree_kvpair kv_node = {setup_col->rowId[i], setup_col->data[i]};
                        btree_insert(btree_index, kv_node);
                    }
                    put_index(index_name, btree_index, BTREE);
                    free(index_name);
                }
                else if(setup_col->idx_type == SORTED) {
                    if (setup_col->cls_type == UNCLSR) {
                        index_name = malloc((strlen(setup_col->name)+strlen(".unclsr.sorted")+1)*sizeof(char));
                        strcpy(index_name, setup_col->name);
                        strcat(index_name, ".unclsr.sorted");
                    }
                    else if (setup_col->cls_type == PRICLSR) {
                        index_name = malloc((strlen(setup_col->name)+strlen(".priclsr.sorted")+1)*sizeof(char));
                        strcpy(index_name, setup_col->name);
                        strcat(index_name, ".priclsr.sorted");
                    }
                    else if (setup_col->cls_type == CLSR) {
                        index_name = malloc((strlen(setup_col->name)+strlen(".clsr.sorted")+1)*sizeof(char));
                        strcpy(index_name, setup_col->name);
                        strcat(index_name, ".clsr.sorted");
                    }
                    else {
                        log_err("clustered index is not supported\n");
                        free(line);
                        return 1;
                    }
                    linknode* sorted_index = link_init();
                    log_info("create a sorted index:%s \n", index_name);
                    for(size_t i = 0; i < setup_col->size; ++i) {
                        sorted_index = link_insert_head(sorted_index, setup_col->rowId[i], setup_col->data[i]);
                        sorted_index = link_sort(sorted_index);
                    }
                    put_index(index_name, sorted_index, SORTED);
                    free(index_name);
                }
                else if (setup_col->idx_type == UNIDX) {
                    log_info("the column is un-index. \n");
                }
                else {
                    log_err("[db_manager.c:load_database()] the index type or clustered type is not supported\n");
                    free(line);
                    return 1;
                }
                free(line_copy_free);
            }
            free(line);
            free(db_file);
            fclose(fp);
        }
    }
    closedir(pDir);
    return 0;
}

int save_database() {
    if(current_db == NULL) {
        log_err("there is no active database\n");
        return 1;
    }
    char cwd[DIRLEN];
    if (getcwd(cwd, DIRLEN) == NULL) {
        log_err("Current working dir path is too long");
        return 1;
    }
    strcat(cwd,"/");
    mkdir("db",0777);
    strcat(cwd,"db/");
    strcat(cwd,current_db->name);
    strcat(cwd,".csv");
    log_info("Current database is stored at: %s\n", cwd);
    FILE *fp = NULL;
    fp = fopen(cwd, "w+");
    if(fp == NULL) {
        log_err("cannot open the database store");
        return 1;
    }
    for(size_t i = 0; i < current_db->size; ++i) {
        Table* stbl = get_table(current_db->tables[i]->name);

        for(size_t j = 0; j < stbl->size; ++j) {
            fprintf(fp, "%s", current_db->name);
            fprintf(fp, ",%s", stbl->name);
            fprintf(fp, ",%s", stbl->pricluster_index_col_name);
            fprintf(fp, ",%zu", stbl->capacity);
            Column* scol = get_column(stbl->columns[j]->name);
            fprintf(fp, ",%s", scol->name);
            if(scol->idx_type == BTREE) {
                fprintf(fp, ",btree");
            }
            else if(scol->idx_type == SORTED) {
                fprintf(fp, ",sorted");
            }
            else if(scol->idx_type == UNIDX) {
                fprintf(fp,",unidx");
            }
            if(scol->cls_type == CLSR) {
                fprintf(fp, ",clsr");
            }
            else if(scol->cls_type == PRICLSR) {
                fprintf(fp, ",priclsr");
            }
            else if(scol->cls_type == UNCLSR) {
                fprintf(fp, ",unclsr");
            }
            for(size_t k = 0; k < scol->size; ++k) {
                fprintf(fp, ",%ld", scol->rowId[k]);
                fprintf(fp, ",%ld", scol->data[k]);
            }
            fprintf(fp, "\n");
        }
    }
    log_info("The database is successfully stored at: %s\n", cwd);
    fclose(fp);
    return 0;
}