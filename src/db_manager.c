#include <utils.h>
#include <memory.h>
#include <assert.h>
#include "db_manager.h"
#include "utils.h"

#define RESIZE 2
// In this class, there will always be only one active database at a time
Db *current_db;

void* resize(void* data, size_t oc, size_t nc) {
	assert(oc <= nc);
	void* ndata = calloc(nc, sizeof(char));
	memcpy(ndata, data, oc);
	return ndata;
}

int* resize_int(int* data, size_t oc, size_t nc) {
	assert(oc <= nc);
	int* ndata = calloc(nc, sizeof(int));
	memcpy(ndata, data, oc * sizeof(int));
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
		tbl = malloc(sizeof(Table));
		tbl->tbl_name = malloc((strlen(tbl_name)+1)* sizeof(char));
		strcpy(tbl->tbl_name,tbl_name);
		tbl->tbl_capacity = num_columns;
		tbl->tbl_size = 0;
		tbl->pricls_col_name = NULL;
		tbl->hasCls = 0;
		tbl->columns = calloc(num_columns, sizeof(Column*));
		put_tbl(tbl_name,tbl);
		cur_db->tables[cur_db->db_size] = tbl;
		cur_db->db_size++;
	    return tbl;
	}
}

Column* create_column(char* tbl_name, char* col_name) {
	Table* cur_tbl = get_tbl(tbl_name);
	if(cur_tbl == NULL) {
        log_err("the associated table doesn't exist, create table %s", tbl_name);
        return NULL;
	}

	if(cur_tbl->tbl_size < cur_tbl->tbl_capacity) {
        Column* col = get_col(col_name);
        if(col != NULL) {
            log_info("column %s exists\n", col_name);
            return col;
        }
        col = malloc(sizeof(Column));
        col->col_name = malloc((strlen(col_name)+1)* sizeof(char));
        strcpy(col->col_name, col_name);
        col->cls_type = UNCLSR;
        col->idx_type = UNIDX;
        col->data = NULL;
        put_col(col_name,col);
        cur_tbl->columns[cur_tbl->tbl_size] = col;
        cur_tbl->tbl_size++;
        return col;
	}
	else {
	    log_err("the associated table is full, cannot create new column %s", col_name);
	    return NULL;
	}
}

int load_data_csv(char* data_path) {
	message_status mes_status;
	FILE *fp;
	if((fp=fopen(data_path,"r"))==NULL) {
		log_err("cannot load data %s\n", data_path);
		return 1;
	}
	char *line = NULL;
	size_t len = 0;
	int read = getline(&line, &len, fp);
	if (read == -1) {
		log_err("read file header failed.\n");
		return 1;
	}
	char* line_copy = malloc((strlen(line)+1)* sizeof(char));
	strcpy(line_copy,line);
	int headerCount = 0;
	char* sepTmp = NULL;
	while(1) {
		sepTmp = next_token_comma(&line_copy,&mes_status);
		if(sepTmp == NULL) {
			break;
		}
		else {
			headerCount++;
		}
	}
	log_info("%d columns in the loading file\n", headerCount);
	if (headerCount == 1) {
		char* header = line;
		int slen = strlen(header);
		char* header_op = malloc((slen+1)* sizeof(char));
		strcpy(header_op,header);
		char* db_name = next_token_period(&header_op,&mes_status);
		char* tbl_name = next_token_period(&header_op,&mes_status);
		char* tbl_full_name = malloc((strlen(tbl_name)+strlen(db_name)+2)* sizeof(char));
		strcpy(tbl_full_name,db_name);
		strcat(tbl_full_name,".");
		strcat(tbl_full_name,tbl_name);
		for (int i = 0; i < slen; ++i) {
			if(header[i] == '\n') {
				header[i] = '\0';
			}
		}
		Column *lcol = get_col(header);
		if (lcol == NULL) {
			log_err("cannot find column %s\n", header);
			free(line_copy);
			free(header_op);
			free(tbl_full_name);
			return 1;
		}
		if(lcol->cls_type == UNCLSR) {
			int rowId_load = 0;
			while ((getline(&line, &len, fp)) != -1) {
				char *va = line;
				int lv = atoi(va);
				if(insert_data_col(lcol,lv,rowId_load) != 0) {
					return 1;
				}
				rowId_load++;
			}
		}
	}
	else {

	}
	return 0;
}

int insert_data_col(Column* col, int data, int rowId) {
	if (col->col_size >= col->col_capacity) {
		size_t new_column_length = RESIZE * col->col_capacity + 1;
		size_t new_length = new_column_length;
		size_t old_length = col->col_capacity;
		if (old_length == 0) {
			assert(new_length > 0);
			log_info("column %s has new length %d\n",col->col_name, new_length);
			col->data = calloc(new_length, sizeof(int));
			col->rowId = calloc(new_length, sizeof(int));
		} else {
			int* dd = resize_int(col->data, old_length, new_length);
			int* dr = resize_int(col->rowId, old_length, new_length);
			free(col->data);
			free(col->rowId);
			col->data = calloc(new_length, sizeof(int));
			col->rowId = calloc(new_length, sizeof(int));
			memcpy(col->data, dd, new_length * sizeof(int));
			memcpy(col->rowId, dr, new_length * sizeof(int));
			if (!col->data) {
				log_err("creating more data space failed.\n");
				return -1;
			}
		}
		col->col_capacity = new_length;
	}
	col->data[col->col_size] = data;
	col->rowId[col->col_size] = rowId;
	col->col_size++;
	return 0;
}