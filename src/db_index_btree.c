//
// Created by rui on 4/3/18.
//

#include <utils.h>
#include <db_manager.h>
#include <string.h>

#include "db_index_btree.h"

BTree* btree_init() {
    BTree* btree = (BTree*) malloc(sizeof(BTree));
    if(btree == NULL) {
        log_err("[%d] init btree failed!\n");
        return NULL;
    }
    btree->kv_num = 0;
    btree->leaf = true;
    return btree;
}


BTree* btree_insert(BTree* btree, int rowId, int value) {
    if(btree->kv_num == MAX_KV_NUM) {

    }
    return btree_insert_notfull(btree, rowId, value);
}

BTree* btree_delete(BTree* btree, int rowId, int value) {
    return NULL;
}

/**
 * insert key into a btree node which is not full
 **/
BTree* btree_insert_notfull(BTree* btree_n, int rowId, int value) {
    //the tree is a leaf node
    if(btree_n->leaf) {
        if(value >= btree_n->kvs[btree_n->kv_num-1].value) {
            btree_n->kvs[btree_n->kv_num].value = value;
            btree_n->kvs[btree_n->kv_num].rowId = rowId;
            btree_n->kv_num++;
        }
        else if (value <= btree_n->kvs[0].value) {
            for(int i = btree_n->kv_num-1; i >= 0; --i) {
                btree_n->kvs[i+1] = btree_n->kvs[i];
            }
            btree_n->kvs[0].value = value;
            btree_n->kvs[0].rowId = rowId;
            btree_n->kv_num++;
        }
        else {
            for(int j = btree_n->kv_num-1; j > 0; --j) {
                if(value <= btree_n->kvs[j].value && value > btree_n->kvs[j-1].value) {
                    btree_n->kvs[j+1] = btree_n->kvs[j];
                    btree_n->kvs[j].value = value;
                    btree_n->kvs[j].rowId = rowId;
                    btree_n->kv_num++;
                    break;
                }
                else {
                    btree_n->kvs[j+1] = btree_n->kvs[j];
                }
            }
        }
    }
    //the tree node is not a leaf node, then we must insert key into the appropriate leaf node
    else {
        if(value >= btree_n->kvs[btree_n->kv_num-1].value) {
            //since the b-tree is built from bottom to top, is the parent is full then means all the regarding children are full, so split
            if(btree_n->child[MAX_CHILD_NUM-1]->kv_num == MAX_KV_NUM) {
                btree_split_child(btree_n,MAX_CHILD_NUM-1,btree_n->child[MAX_CHILD_NUM-1]);
                if(value > btree_n->kvs[btree_n->kv_num-1].value) {
                    btree_insert_notfull(btree_n->child[btree_n->kv_num],rowId,value);
                }
                else {
                    btree_insert_notfull(btree_n->child[btree_n->kv_num-1],rowId,value);
                }
            }
            else {
                btree_insert_notfull(btree_n->child[MAX_CHILD_NUM-1],rowId,value);
            }
        }
        else if(value <= btree_n->kvs[0].value) {
            if(btree_n->child[0]->kv_num == MAX_KV_NUM) {
                btree_split_child(btree_n,0,btree_n->child[0]);
                if(value > btree_n->kvs[0].value) {
                    btree_insert_notfull(btree_n->child[1],rowId,value);
                }
                else {
                    btree_insert_notfull(btree_n->child[0],rowId,value);
                }
            }
            else {
                btree_insert_notfull(btree_n->child[0],rowId,value);
            }
        }
        else {
            for(int i = btree_n->kv_num-1; i > 0; --i) {
                if(value <= btree_n->kvs[i].value && value > btree_n->kvs[i-1].value) {
                    if(btree_n->child[i]->kv_num == MAX_KV_NUM) {
                        btree_split_child(btree_n,i,btree_n->child[i]);
                        if(value > btree_n->kvs[i].value) {
                            btree_insert_notfull(btree_n->child[i+1],rowId,value);
                        }
                        else {
                            btree_insert_notfull(btree_n->child[i],rowId,value);
                        }
                    }
                    else {
                        btree_insert_notfull(btree_n->child[i],rowId,value);
                    }
                    break;
                }
            }
        }
    }
    return btree_n;
}

/**
 * split the pos-th child of the parent, parent is non-full and child is full
 * the larger half is added to new allocated child
 **/
BTree* btree_split_child(BTree* parent, int pos, BTree* child) {
    BTree* new_child = (BTree*) malloc(sizeof(BTree));
    if(new_child == NULL) {
        log_err("init new btree for splitting failed!\n");
        return NULL;
    }
    new_child->leaf = child->leaf;

    //copy last T-1 items to new child
    for(int i = 0; i <= T-2; ++i) {
        new_child->kvs[i] = child->kvs[i+T];
        child->kvs[i+T].value = 0;
        child->kvs[i+T].rowId = -1;
    }
    new_child->kv_num = T-1;
    child->kv_num = T-1;
    //if child is leaf node, also need to copy regarding children
    if(child->leaf == false) {
        for(int i = MAX_CHILD_NUM-1; i <= T; --i) {
            new_child->child[i-T] = child->child[i];
            child->child[i] = NULL;
        }
    }
    //move parents kv for new median value
    for(int j = parent->kv_num-1; j >= pos; --j) {
        parent->kvs[j+1] = parent->kvs[j];
    }
    //move parents children for new median value
    for(int k = parent->kv_num; k > pos; --k) {
        parent->child[k+1] = parent->child[k];
    }
    parent->kvs[pos] = child->kvs[T];
    parent->kv_num++;
    parent->child[pos] = child;
    parent->child[pos+1] = new_child;
}






