#include <utils.h>
#include <memory.h>
#include <assert.h>
#include "db_manager.h"

#define RESIZE 2
// In this class, there will always be only one active database at a time
Db *current_db;

void* resize(void* data, size_t oc, size_t nc) {
	assert(oc <= nc);
	void* ndata = calloc(nc, sizeof(char));
	memcpy(ndata, data, oc);
	return ndata;
}

Db* create_db(const char* db_name) {
	current_db = get_db(db_name);
	if(current_db != NULL) {
		log_info("database %s exists, open database %s\n", db_name, db_name);
		return current_db;
	}
	current_db = malloc(sizeof(Db));
	current_db->db_name = malloc((strlen(db_name)+1)* sizeof(char));
	strcpy(current_db->db_name,db_name);
	current_db->db_capacity = 0;
	current_db->db_size = 0;
	current_db->tables = NULL;
	put_db(db_name,current_db);
	return current_db;
}

Table* create_table(char* db_name, char* tbl_name, size_t num_columns) {
	Db* cur_db = get_db(db_name);
	if(cur_db == NULL) {
		log_err("the database doesn't exist, create/switch db %s", db_name);
		return NULL;
	}
	Table* tbl = get_tbl(tbl_name);
	if(tbl != NULL) {
		log_info("the table %s exists", tbl_name);
		return tbl;
	}
	else {
		tbl = malloc(sizeof(Table));
		if(cur_db->db_capacity - cur_db->db_size <= 0) {
			log_info("no enough table space in database %s, creating more table space.\n", cur_db->db_name);
			size_t more_table_capacity = RESIZE * cur_db->db_capacity + 1;
			size_t new_capacity = sizeof(Table*) * more_table_capacity;
			size_t old_capacity = sizeof(Table*) * cur_db->db_capacity;
			if(old_capacity == 0) {
				assert(new_capacity>0);
				cur_db->tables = calloc(more_table_capacity, sizeof(Table*));
			}
			else {
				void* t = resize(cur_db->tables, old_capacity, new_capacity);
				free(cur_db->tables);
				cur_db->tables = calloc(more_table_capacity, sizeof(Table*));
				memcpy(cur_db->tables, t, new_capacity*sizeof(char));
				if(cur_db->tables == NULL){
					log_err("create more table space in database failed.\n", cur_db->db_name);
					free(tbl);
					return NULL;
				}
			}
			cur_db->db_capacity = more_table_capacity;
		}
		cur_db->tables[cur_db->db_size] = tbl;
		cur_db->db_size++;
		tbl->tbl_name = malloc((strlen(tbl_name)+1)* sizeof(char));
		strcpy(tbl->tbl_name,tbl_name);
		tbl->table_capacity = 0;
		tbl->table_size = 0;
		tbl->pricls_col_name = NULL;
		tbl->hasCls = 0;
		put_tbl(tbl_name,);
	}

}
