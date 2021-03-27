#ifndef INDEX_BTREE_H
#define INDEX_BTREE_H

#include <stdbool.h>

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
void btree_insert(btree t, int key);

/**
 * return the key if it is present in a tree
 */
int btree_search(btree t, int key);

/**
 * return all keys in the tree in order
 * and store it in the array
 */
void btree_all_keys(btree t);

#endif //INDEX_BTREE_H
