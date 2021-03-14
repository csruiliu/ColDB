#include "db_element.h"
#include "kv_store.h"
#include "utils_func.h"

kvstore* db_store;
kvstore* table_store;
kvstore* column_store;
kvstore* result_store;
kvstore* index_store;

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