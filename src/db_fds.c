
#include "db_fds.h"
#include "db_kvs.h"
#include "utils.h"
/*
 *  Created by Rui Liu on 3/18/18.
 *  Source file for ColDB fundamental data structure (FDS)
 */

KVStore* db_store;
KVStore* tbl_store;
KVStore* col_store;
KVStore* rsl_store;
KVStore* idx_store;

void init_db_store(size_t size) {
    if(allocate(&db_store, size) == 1) {
        log_err("init database store failed.");
    }
}

void init_tbl_store(size_t size) {
    if(allocate(&tbl_store, size) == 1) {
        log_err("init table store failed.");
    }
}

void init_col_store(size_t size) {
    if(allocate(&col_store, size) == 1) {
        log_err("init column store failed.");
    }
}

void init_rls_store(size_t size) {
    if(allocate(&rsl_store, size) == 1) {
        log_err("init result store failed.");
    }
}

void init_idx_store(size_t size) {
    if(allocate(&idx_store, size) == 1) {
        log_err("init idx store failed.");
    }
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

void put_db(char* db_name, Db* db) {
    int flag = put(db_store,db_name,db,sizeof(Db));
    if(flag == 1) {
        log_info("persistent database %s failed", db_name);
    }
    else if(flag == 2) {
        log_info("a rehash has been done, re-put db into kv store\n");
        if(put(db_store,db_name,db,sizeof(Db)) == 1) {
            log_info("persistent database %s failed\n", db_name);
        }
    }
}



Db* get_db(char* db_name) {
    Db* Db = get(db_store, db_name);
    return Db;
}

Table* get_tbl(char* tbl_name) {
    Table* tbl = get(tbl_store,tbl_name);
    return tbl;
}

void free_db_store() {
    if(deallocate(db_store) != 0) {
        log_info("free db kv store failed\n");
    } else {
        log_info("free db kv store successfully\n");
    }
}

void free_tbl_store() {
    if(deallocate(tbl_store) != 0) {
        log_info("free tbl kv store failed\n");
    } else {
        log_info("free tbl kv store successfully\n");
    }
}

void free_col_store() {
    if(deallocate(col_store) != 0) {
        log_info("free col kv store failed\n");
    } else {
        log_info("free col kv store successfully\n");
    }
}

void free_rsl_store() {
    if(deallocate(rsl_store) != 0) {
        log_info("free rsl kv store failed\n");
    } else {
        log_info("free rsl kv store successfully\n");
    }
}

void free_idx_store() {
    if(deallocate(idx_store) != 0) {
        log_info("free idx kv store failed\n");
    } else {
        log_info("free idx kv store successfully\n");
    }
}