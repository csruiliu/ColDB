
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

void free_batch_query();

int batch_schedule_convoy();

pthread_t* create_thread(size_t thread_id);

#endif //BATCH_QUERY_H
