
#ifndef BATCH_QUERY_H
#define BATCH_QUERY_H

#include <pthread.h>
#include <memory.h>
#include <sys/sysinfo.h>

#include "parse.h"

void set_use_batch(int value);

int get_use_batch();

int init_batch();

int batch_add(DbOperator *query);

int exec_batch_query();

void free_batch_query(DbOperator* query);

int destroy_batch_query_queue();

int batch_schedule_convoy();

#endif //BATCH_QUERY_H
