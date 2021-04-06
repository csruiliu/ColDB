#ifndef INDEX_BTREE_H
#define INDEX_BTREE_H

#include <stdbool.h>

typedef struct btree_kvpair {
    long row_id;
    long value;
} btree_kvpair;

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
void btree_insert(btree t, btree_kvpair kv_node);

/**
 * return the key-value pair if it is present in a tree
 */
btree_kvpair btree_search(btree t, long row_id);

/**
 * return all keys in the tree in order and store it in the array
 */
long btree_inorder_traversal(btree t, long value_array[], long row_id_array[], long index);

#endif //INDEX_BTREE_H
