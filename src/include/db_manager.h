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
 * get the result of an avg query on the column
 **/
int avg_column_data(char* avg_col_name, char* handle);

/**
 * compute average of the select "result"
 **/
int avg_result_data(char* avg_rsl_name, char* handle);

/**
 * get the result of an sum query on the column
 **/
int sum_column_data(char* avg_col_name, char* handle);

/**
 * compute sum of the select "result"
 **/
int sum_result_data(char* avg_rsl_name, char* handle);

/**
 * compute add query
 **/
int add_col_col(char* add_name1, char* add_name2, char* handle);
int add_rsl_rsl(char* add_name1, char* add_name2, char* handle);
int add_col_rsl(char* add_name1, char* add_name2, char* handle);
int add_rsl_col(char* add_name1, char* add_name2, char* handle);

/**
 * compute max query
 **/
int max_rsl_value(char* max_vec, char* handle);
int max_rsl_value_pos(char* max_vec_pos, char* max_vec_value, char* handle_pos, char* handle_value);

/**
 * compute min query
 **/
int min_rsl_value(char* min_vec,char* handle);
int min_rsl_value_pos(char* min_vec_pos, char* min_vec_value, char* handle_pos, char* handle_value);

/**
 * compute subtraction
 **/
int sub_col_col(char* sub_name1, char* sub_name2, char* handle);
int sub_rsl_rsl(char* sub_name1, char* sub_name2, char* handle);

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