#ifndef INDEX_BTREE_H
#define INDEX_BTREE_H

#include <stdbool.h>

typedef struct kvpair {
    int row_id;
    int value;
} kv_pair;

typedef struct bt_node *btree;

/**
 * create a new empty tree
 */
btree btree_init();

/**
 * free a tree
 */
void btree_destroy(btree t);

/**
 * insert a new element into a tree
 */
void btree_insert(btree t, struct kvpair kv_node);

/**
 * return the key-value pair if it is present in a tree
 */
struct kvpair btree_search(btree t, int key);

/**
 * return all keys in the tree in order and store it in the array
 */
int btree_inorder_traversal(btree t, int value_array[], int row_id_array[], int index);

#endif //INDEX_BTREE_H
