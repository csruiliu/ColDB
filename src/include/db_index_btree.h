//
// Created by rui on 4/3/18.
//

#ifndef COLDB_DB_INDEX_H
#define COLDB_DB_INDEX_H

#define T 127
#define MAX_KEY_NUM 2 * T - 1
#define MAX_CHILD_NUM MAX_KEY_NUM + 1

typedef int type_t;

typedef struct btree_t {
    int key_num;                   /* number of keys in this node*/
    type_t max_key;                /* max key in this subtree */
    type_t min_key;                /* min key in this subtree */
    bool leaf;                     /* it is a leaf node or not, '1' is leaf, '0' is not */
    type_t key[MAX_KEY_NUM];
    struct btree_t* child[MAX_CHILD_NUM]; /* subtrees of this tree*/

} BTree, BTNode;

BTree* btree_init();

BTree* btree_insert(BTree* btree, type_t key);

BTree* btree_delete(BTree* btree, type_t key);

BTree* btree_split_child(BTree* parent, int pos, BTree* child);

BTree* btree_insert_notfull(BTree* btree, type_t key);

#endif //COLDB_DB_INDEX_H
