#ifndef INDEX_SORT_H
#define INDEX_SORT_H

typedef struct node {
    long row_id;
    long data;
    struct node *next;
} linknode;

linknode* link_init();

linknode* link_insert_head(linknode* head, long row_id, long data);

linknode* link_delete(linknode* head, long row_id);

linknode* link_sort(linknode* head);

long link_traversal(linknode* head, long value_array[], long row_id_array[]);

long link_length(linknode* head);

#endif //INDEX_SORT_H