#ifndef DB_MANAGER_H
#define DB_MANAGER_H

extern Db *current_db;

/**
 *  create db, table, column
 **/
Db* create_db(char* db_name);

Table* create_table(char* db_name, char* table_name, char* pricls_col_name, size_t num_columns);

Column* create_column(char* table_name, char* col_name);

int setup_db_csv();

#endif //DB_MANAGER_H