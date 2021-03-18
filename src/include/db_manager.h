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
 *  read data from csv file
 **/
int read_csv(char* data_path);

/**
 *  load database when start, save database when shutdown
 **/
int load_database();

int save_database();

#endif //DB_MANAGER_H