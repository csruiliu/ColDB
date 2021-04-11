#include "db_element.h"
#include "kv_store.h"
#include "utils_func.h"
#include "index_btree.h"
#include "index_sort.h"

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
    else {
        log_info("persistent database %s successfully\n", db_name);
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
    else {
        log_info("persistent table %s successfully\n", table_name);
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
    else {
        log_info("persistent column %s successfully\n", col_name);
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

void replace_result(char* result_name, Result* result) {
    int flag = put_replace(result_store, result_name, result, sizeof(Result));
    if(flag != 0) {
        log_err("replace result %s failed", result_name);
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

void* get_index(char* index_name) {
    void* index = get(index_store, index_name);
    return index;
}

void delete_index(char* index_name) {
    int flag = delete(index_store, index_name);
    if (flag == 0) {
        log_info("delete index %s successfully\n", index_name);
    }
    else {
        log_err("delete index %s failed\n", index_name);
    }

}

void put_index(char* index_name, void* index, IndexType index_type) {
    if (index_type == BTREE) {
        int flag = put(index_store, index_name, index, sizeof(struct bt_node));
        if(flag == 1) {
            log_err("persistent index %s failed\n", index_name);
        }
        else if(flag == 2) {
            log_info("a rehash has been done, re-put index into kv store\n");
            if(put(index_store, index_name, index, sizeof(btree)) == 1) {
                log_err("persistent index %s failed\n", index_name);
            }
        }
        else {
            log_info("persistent index %s successfully\n", index_name);
        }
    }
    else if (index_type == SORTED) {
        int flag = put(index_store, index_name, index, sizeof(linknode));
        if(flag == 1) {
            log_err("persistent index %s failed\n", index_name);
        }
        else if(flag == 2) {
            log_info("a rehash has been done, re-put index into kv store\n");
            if(put(index_store, index_name, index, sizeof(linknode)) == 1) {
                log_err("persistent index %s failed\n", index_name);
            }
        }
        else {
            log_info("persistent index %s successfully\n", index_name);
        }
    }
    else {
        log_err("the index type is not supported");
    }
}

void free_index_store() {
    if(kv_deallocate(index_store) != 0) {
        log_info("free result kv store failed\n");
    } else {
        log_info("free result kv store successfully\n");
    }
}