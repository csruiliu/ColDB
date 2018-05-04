//
// Created by rui on 4/12/18.
//


#include "db_batch.h"
#include "db_batch_queue.h"
#include "utils.h"


int useBatch = 0;

void setUseBatch(int value) {
    useBatch = value;
}

int getUseBatch() {
    return useBatch;
}

int init_batch() {
    return create_batch_queue();
}

int batch_add(DbOperator *query) {
    bqNode* node = create_node(query);
    if(node == NULL) {
        return 1;
    }
    if(add_batch_queue(node) != 0) {
        return 1;
    }
    return 0;
}

int batch_schedule() {
    int bq_len = get_queue_length();
    for (int i = 0; i < bq_len; ++i) {
        bqNode* node = pop_batch_queue();

    }
    return 0;
}

int exec_batch_query() {
    show_batch_query();
    return 0;
}
