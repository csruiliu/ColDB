//
// Created by rui on 3/18/18.
//

#ifndef COLDB_DB_MANAGER_H
#define COLDB_DB_MANAGER_H

#include "db_fds.h"
#include "parse.h"

Status add_db(const char* db_name, bool new);

Table* create_table(Db* db, const char* name, size_t num_columns, Status *status);

#endif //COLDB_DB_MANAGER_H
