//
// Created by rui on 3/18/18.
//

#ifndef COLDB_DB_MANAGER_H
#define COLDB_DB_MANAGER_H

#include "db_fds.h"
#include "parse.h"

Db* create_db(const char* db_name);

Table* create_table(char* db_name, char* tbl_name, size_t num_columns);

Column* create_column(char* tbl_name, char* col_name);

int load_data_csv(char* data_path);

#endif //COLDB_DB_MANAGER_H
