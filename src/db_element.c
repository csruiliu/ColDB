#include "db_element.h"
#include "kv_store.h"
#include "utils_func.h"

kvstore* db_store;
kvstore* table_store;
kvstore* column_store;
kvstore* result_store;
kvstore* index_store;

/**
 *  All the function implementations used for kv store of db
 **/
void init_db_store(size_t size) {
    if(kv_allocate(&db_store, size) == 1) {
        log_err("init database store failed.");
    }
}

Db* get_db(char* db_name) {
    Db* Db = get(db_store, db_name);
    return Db;
}

void put_db(char* db_name, Db* db) {
    int flag = put(db_store,db_name,db,sizeof(Db));
    if(flag == 1) {
        log_err("persistent database %s failed", db_name);
    }
    else if(flag == 2) {
        log_info("a rehash has been done, re-put db into kv store\n");
        if(put(db_store,db_name,db,sizeof(Db)) == 1) {
            log_err("persistent database %s failed\n", db_name);
        }
    }
}

void free_db_store() {
    if(kv_deallocate(db_store) != 0) {
        log_info("free db kv store failed\n");
    } else {
        log_info("free db kv store successfully\n");
    }
}

/**
 *  All the function implementations used for kv store of table
 **/
void init_table_store(size_t size) {
    if(kv_allocate(&table_store, size) == 1) {
        log_err("init table store failed.");
    }
}

Table* get_table(char* table_name) {
    Table* tbl = get(table_store, table_name);
    return tbl;
}

void put_table(char* table_name, Table* table) {
    int flag = put(table_store, table_name, table, sizeof(Table));
    if(flag == 1) {
        log_err("persistent table %s failed", table_name);
    }
    else if(flag == 2) {
        log_info("a rehash has been done, re-put table into kv store\n");
        if(put(table_store, table_name, table, sizeof(Table)) == 1) {
            log_err("persistent table %s failed", table_name);
        }
    }
}

void free_table_store() {
    if(kv_deallocate(table_store) != 0) {
        log_info("free tbl kv store failed\n");
    } else {
        log_info("free tbl kv store successfully\n");
    }
}

/**
 *  All the function implementations used for kv store of column
 **/
void init_column_store(size_t size) {
    if(kv_allocate(&column_store, size) == 1) {
        log_err("init column store failed.");
    }
}

Column* get_column(char* col_name) {
    Column* col = get(column_store, col_name);
    return col;
}

void put_column(char* col_name, Column* col) {
    int flag = put(column_store, col_name, col, sizeof(Column));
    if(flag == 1) {
        log_err("persistent column %s failed", col_name);
    }
    else if(flag == 2) {
        log_info("a rehash has been done, re-put column into kv store\n");
        if(put(column_store, col_name, col, sizeof(Column)) == 1) {
            log_err("persistent column %s failed", col_name);
        }
    }
}

void free_column_store() {
    if(kv_deallocate(column_store) != 0) {
        log_info("free col kv store failed\n");
    } else {
        log_info("free col kv store successfully\n");
    }
}

/**
 *  All the function definitions used for kv store of result
 **/
void init_result_store(size_t size) {
    if(kv_allocate(&result_store, size) == 1) {
        log_err("init result store failed.");
    }
}

Result* get_result(char* result_name) {
    Result* result = get(result_store, result_name);
    return result;
}

void put_result_replace(char* result_name, Result* result) {
    int flag = put_replace(result_store, result_name, result, sizeof(Result));
    if(flag != 0) {
        log_err("persistent result %s failed", result_name);
    }
}

void free_result_store() {
    if(kv_deallocate(result_store) != 0) {
        log_info("free result kv store failed\n");
    } else {
        log_info("free result kv store successfully\n");
    }
}

/**
 *  All the function definitions used for kv store of index
 **/
void init_index_store(size_t size) {
    if(kv_allocate(&index_store, size) == 1) {
        log_err("init idx store failed.");
    }
}

void free_index_store() {
    if(kv_deallocate(index_store) != 0) {
        log_info("free result kv store failed\n");
    } else {
        log_info("free result kv store successfully\n");
    }
}