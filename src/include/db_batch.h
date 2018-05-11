//
// Created by rui on 4/12/18.
//

#ifndef COLDB_DB_BATCH_H
#define COLDB_DB_BATCH_H

#include <pthread.h>
#include "sys/sysinfo.h"
#include "parse.h"
#include "memory.h"

void setUseBatch(int value);

int getUseBatch();

int init_batch();

int batch_add(DbOperator *query);

int exec_batch_query();

int batch_schedule_convoy();

int create_thread(int size_p);

#endif //COLDB_DB_BATCH_H
