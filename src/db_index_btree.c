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
    btree->key_num = 0;
    btree->leaf = '1';
    btree->max_key = -1;
    btree->min_key = -1;
    return btree;
}


BTree* btree_insert(BTree* btree, type_t key) {
    if(btree->key_num == KEY_NUM) {

    }
    return btree_insert_notfull(btree, key);
}

BTree* btree_delete(BTree* btree, type_t key) {
    return NULL;
}

BTree* btree_insert_notfull(BTree* btree, type_t key) {
    //the tree is a leaf node
    if(btree->leaf == '1') {
        if(btree->max_key == -1 && btree->min_key == -1) {
            btree->max_key = key;
            btree->min_key = key;
            btree->key[0] = key;
            btree->key_num = 1;
        }
        else {
            if(key >= btree->max_key) {
                btree->key[btree->key_num] = key;
                btree->key_num++;
                btree->max_key = key;
            }
            else if(key <= btree->min_key) {
                for(int i = btree->key_num; i > 0; --i) {
                    btree->key[i] = btree->key[i-1];
                }
                btree->key[0] = key;
                btree->min_key = key;
            }
            else {
                //we have min_key, so don't need to consider j = 0
                for(int j = btree->key_num-1; j > 0; --j) {
                    btree->key[j+1] = btree->key[j];
                    if(key < btree->key[j] && key > btree->key[j-1]) {
                        btree->key[j] = key;
                        break;
                    }
                }
            }
        }
    }
    //the tree is not a leaf node
    else {
        if (key < btree->min_key) {
            if(btree->child[0]->key_num == KEY_NUM) {
                btree_split_child(btree, 0, btree->child[0]);
            }
            else {
                btree_insert_notfull(btree->child[0], key);
            }
        }
        else if (key > btree->max_key) {
            if(btree->child[btree->key_num]->key_num == KEY_NUM) {
                btree_split_child(btree, 0, btree->child[0]);
            }
            else {
                btree_insert_notfull(btree->child[btree->key_num], key);
            }
        }
        else {
            for(int k = btree->key_num - 1; k > 0; --k) {
                if(key < btree->key[k] && key > btree->key[k-1]) {
                    btree_insert_notfull(btree->child[k], key);
                    break;
                }
            }
        }
    }
    return btree;
}

/**
 * split the pos-th child of the parent, parent is nonfull and child is full
 * the larger half is added to new allocated child
 **/
BTree* btree_split_child(BTree* parent, int pos, BTree* child) {
    BTree* new_child = (BTree*) malloc(sizeof(BTree));
    if(new_child == NULL) {
        log_err("init new btree for splitting failed!\n");
        return NULL;
    }
    new_child->leaf = child->leaf;
    //if child is leaf node
    if(child->leaf == '1') {
        //copy the last half keys of the parents to the new node
        for(int i = 0; i <= T - 2; ++i) {
            new_child->key[i] = child->key[i+T];
        }
        new_child->key_num = T - 1;
        child->key_num = T - 1;
        //move previous keys for new allocated child
        for(int j = parent->key_num-1; j >= pos; --j) {
            parent->key[j+1] = parent->key[j];
        }
        //move previous child for new allocated child
        for(int k = parent->key_num; k > pos; --k) {
            parent->child[k+1] = parent->child[k];
        }
        parent->key[pos] = child->key[T];
        parent->key_num++;
        parent->child[pos] = child;
        parent->child[pos+1] = new_child;
    }
    //else child is not a leaf node
    else {
        for(int j = 0; j <= T-2; ++j) {
            new_child->child[j] = child->child[j+T];
        }
        child->key_num = T-1;
    }
}






