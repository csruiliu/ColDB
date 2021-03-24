#ifndef INDEX_SORT_H
#define INDEX_SORT_H

typedef struct node {
    int row_id;
    int data;
    struct node *next;
} node;

void insert_head(int row_id, int data);

node* delete(int row_id);

void sort();

void print_list();

int length();

bool is_empty();

#endif //INDEX_SORT_H