#ifndef INDEX_SORT_H
#define INDEX_SORT_H

typedef struct node {
    int row_id;
    int data;
    struct node *next;
} linknode;

linknode* link_init();

linknode* link_insert_head(linknode* head, int row_id, int data);

linknode* link_delete(linknode* head, int row_id);

linknode* link_sort(linknode* head);

int link_traversal(linknode* head, int value_array[], int row_id_array[]);

int link_length(linknode* head);

#endif //INDEX_SORT_H