#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <assert.h>
#include <memory.h>

#include "message.h"
#include "db_element.h"
#include "utils_func.h"

#define RESIZE 2
#define DIRLEN 256

// In this class, there will always be only one active database at a time
Db* current_db;

void* resize(void* data, size_t oc, size_t nc) {
    assert(oc <= nc);
    void* ndata = calloc(nc, sizeof(char));
    memcpy(ndata, data, oc);
    return ndata;
}

Db* create_db(char* db_name) {
    Db* db = get_db(db_name);
    if(db != NULL) {
        log_info("database %s exists, open database %s\n", db_name, db_name);
        return current_db;
    }
    db = malloc(sizeof(Db));
    db->name = malloc((strlen(db_name)+1)* sizeof(char));
    strcpy(db->name, db_name);
    db->capacity = 0;
    db->size = 0;
    db->tables = NULL;
    put_db(db_name,db);
    log_info("create db %s successfully.\n",db_name);
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
                void* t = resize(current_db->tables, old_capacity, new_capacity);
                free(current_db->tables);
                current_db->tables = calloc(more_table_capacity, sizeof(Table*));
                memcpy(current_db->tables, t, new_capacity*sizeof(char));
                if(current_db->tables == NULL){
                    log_err("create more table space in database failed.\n", current_db->name);
                    free(tbl);
                    return NULL;
                }
            }
            current_db->capacity = more_table_capacity;
        }
        tbl = malloc(sizeof(Table));
        tbl->name = malloc((strlen(table_name)+1)*sizeof(char));
        strcpy(tbl->name, table_name);
        tbl->aff_db_name = malloc((strlen(db_name)+1)*sizeof(char));
        strcpy(tbl->aff_db_name, db_name);
        tbl->capacity = num_columns;
        tbl->size = 0;
        tbl->pricluster_index_col_name = malloc((strlen(pricls_col_name)+1)* sizeof(char));
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


/*
 * Similarly, this method is meant to create a database.
 * As an implementation choice, one can use the same method
 * for creating a new database and for loading a database
 * from disk, or one can divide the two into two different
 * methods.
 */
Status add_db(const char* db_name, bool new) {
	struct Status ret_status;

	ret_status.code = OK;
	return ret_status;
}
