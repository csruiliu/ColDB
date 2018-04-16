//
// Created by rui on 4/12/18.
//

#ifndef COLDB_DB_BATCH_H
#define COLDB_DB_BATCH_H

#include "parse.h"

void setUseBatch(int value);

int getUseBatch();

int init_batch();

int batch_add(DbOperator *query);

int exec_batch_query();

#endif //COLDB_DB_BATCH_H
