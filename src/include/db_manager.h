//
// Created by rui on 3/18/18.
//

#ifndef COLDB_DB_MANAGER_H
#define COLDB_DB_MANAGER_H

#include "db_fds.h"
#include "parse.h"

Db* create_db(char* db_name);

Table* create_table(char* db_name, char* tbl_name, size_t num_columns);

Column* create_column(char* tbl_name, char* col_name);

int load_data_csv(char* data_path);

int persist_data_csv();

int setup_db_csv();

int set_column_idx_cls(Column* slcol, char* idx_type, char* cls_type);

int insert_data_col(Column* col, int data, int rowId);

int insert_data_tbl(Table* itbl, int* row_values);

int select_data_col_unidx(Column* scol, char* handle, char* pre_range, char* post_range);

int select_data_rsl(Result* srsl_pos, Result* srsl_val, char* handle, char* pre_range, char* post_range);

int fetch_col_data(char* col_val_name, char* rsl_vec_pos, char* handle);

char* generate_print_result(size_t print_num, char** print_name);

#endif //COLDB_DB_MANAGER_H
