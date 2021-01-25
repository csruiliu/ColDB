//
// Created by rui on 3/18/18.
//

#ifndef COLDB_DB_MANAGER_H
#define COLDB_DB_MANAGER_H

#include "db_fds.h"
#include "parse.h"

/**
 *  create db, table, column
 **/
Db* create_db(char* db_name);

Table* create_table(char* db_name, char* tbl_name, char* pricls_col_name, size_t num_columns);

Column* create_column(char* tbl_name, char* col_name);

/**
 *  prepare database after setup
 **/
int setup_db_csv();

/**
 *  load/save data from/to disk using CVS format
 **/
int load_data_csv(char* data_path);

int save_data_csv();

int insert_data_col(Column* col, int data, int rowId);

int insert_data_tbl(Table* itbl, int* row_values);



int set_column_idx_cls(Column* slcol, char* idx_type, char* cls_type);


int select_data_col_unidx(Column* scol, char* handle, char* pre_range, char* post_range);

int fetch_col_data(char* col_val_name, char* rsl_vec_pos, char* handle);

int avg_col_data(char* avg_name, char* handle);

int sum_col_data(char* sum_name, char* handle);

int add_col_col(char* add_name1, char* add_name2, char* handle);

int add_col_rsl(char* add_name1, char* add_name2, char* handle);

int sub_col_col(char* sub_name1, char* sub_name2, char* handle);



int set_column_idx_cls(Column* slcol, char* idx_type, char* cls_type);




int select_data_col_unidx(Column* scol, char* handle, char* pre_range, char* post_range);

int select_data_rsl(Result* srsl_pos, Result* srsl_val, char* handle, char* pre_range, char* post_range);

int fetch_col_data(char* col_val_name, char* rsl_vec_pos, char* handle);

char* generate_print_result(size_t print_num, char** print_name);

int avg_col_data(char* avg_name, char* handle);

int avg_rsl_data(char* avg_name, char* handle);

int sum_col_data(char* sum_name, char* handle);

int sum_rsl_data(char* sum_name, char* handle);

int add_col_col(char* add_name1, char* add_name2, char* handle);

int add_rsl_rsl(char* add_name1, char* add_name2, char* handle);

int add_col_rsl(char* add_name1, char* add_name2, char* handle);

int add_rsl_col(char* add_name1, char* add_name2, char* handle);

int sub_col_col(char* sub_name1, char* sub_name2, char* handle);

int sub_rsl_rsl(char* sub_name1, char* sub_name2, char* handle);

int max_rsl_value(char* max_vec, char* handle);

int max_rsl_value_pos(char* max_vec_pos, char* max_vec_value, char* handle_pos, char* handle_value);

int min_rsl_value(char* min_vec,char* handle);

int min_rsl_value_pos(char* min_vec_pos, char* min_vec_value, char* handle_pos, char* handle_value);

int tbl_psylayout_cls(Table* tbl_aff, IndexType idx_type);

#endif //COLDB_DB_MANAGER_H
