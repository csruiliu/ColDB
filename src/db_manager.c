#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <memory.h>

#include "db_element.h"
#include "message.h"
#include "utils_func.h"

#define RESIZE 2
#define DIRLEN 256

// In this class, there will always be only one active database at a time
Db* current_db;

Db* create_db(char* db_name) {
    Db* db = get_db(db_name);
    if(db != NULL) {
        log_info("database %s exists, open database %s\n", db_name, db_name);
        return current_db;
    }
    db = malloc(sizeof(Db));
    db->db_name = malloc((strlen(db_name)+1)* sizeof(char));
    strcpy(db->db_name,db_name);
    db->db_capacity = 0;
    db->db_size = 0;
    db->tables = NULL;
    put_db(db_name,db);
    log_info("create db %s successfully.\n",db_name);
    return db;
}


/*
 * Here you will create a table object. The Status object can be used to return
 * to the caller that there was an error in table creation
 */
Table* create_table(Db* db, const char* name, size_t num_columns, Status *ret_status) {
	ret_status->code=OK;
	return NULL;
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
