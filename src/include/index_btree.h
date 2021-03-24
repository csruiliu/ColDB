
#ifndef INDEX_BTREE_H
#define INDEX_BTREE_H

#define T 127
#define MAX_KV_NUM (2 * T - 1)
#define MAX_CHILD_NUM (MAX_KV_NUM + 1)

#include <stdbool.h>

typedef int type_t;

/**
 * only consider int type so far, so the btree_kv are only int
 **/
typedef struct btree_kv {
    type_t rowId;
    type_t value;
} bkv;

typedef struct btree_t {
    size_t kv_num;                   /* number of keys in this node */
    bool leaf;                     /* it is a leaf node or not, '1' is leaf, '0' is not */
    bkv kvs[MAX_KV_NUM];
    struct btree_t* child[MAX_CHILD_NUM]; /* subtrees of this tree*/
} BTree;

BTree* btree_init();

BTree* btree_insert(BTree* btree_n, int rowId, int value);

BTree* btree_delete_by_rowId(BTree* btree_n, int rowId, int value);

BTree* btree_delete_by_value(BTree* btree_n, int rowId, int value);

BTree* btree_split_child(BTree* parent, size_t pos, BTree* child);

BTree* btree_insert_notfull(BTree* btree_n, int rowId, int value);

#endif //INDEX_BTREE_H
