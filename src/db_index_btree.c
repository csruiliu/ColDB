//
// Created by rui on 4/3/18.
//

#include <utils.h>
#include <db_manager.h>
#include "db_index_btree.h"

BTree* btree_init() {
    BTree* btree = (BTree*) malloc(sizeof(BTree));
    if(btree == NULL) {
        log_info("[%d] init btree failed!\n");
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
        for(int i = btree->key_num - 1; i >= 0; --i) {
            if(key < btree->key[i]) {
                break;
            }
        }

    }
    return NULL;
}








