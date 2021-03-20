#ifndef DB_MANAGER_H
#define DB_MANAGER_H

extern Db *current_db;

/**
 *  create db, table, column
 **/
Db* create_db(char* db_name);

Table* create_table(char* db_name, char* table_name, char* pricls_col_name, size_t num_columns);

Column* create_column(char* table_name, char* col_name);

/**
 * relational insert
 */
int insert_data_table(Table* itbl, int* row_values);

/**
 * select data for un-index column
 */
int select_data_col_unidx(Column* scol, char* handle, char* pre_range, char* post_range);

/**
 * select data for result
 */
int select_data_result(Result* srsl_pos, Result* srsl_val, char* handle, char* pre_range, char* post_range);

/**
 * fetch column data
 */
int fetch_col_data(char* col_val_name, char* rsl_vec_pos, char* handle);

/**
 * print result
 */
char* generate_print_result(size_t print_num, char** print_name);

/**
 *  read data from csv file
 **/
int read_csv(char* data_path);

/**
 *  load database when start, save database when shutdown
 **/
int load_database();

int save_database();

#endif //DB_MANAGER_H