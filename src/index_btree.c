#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "utils_func.h"
#include "index_btree.h"

#define MAX_KEYS (16)

struct bt_node {
    //is this a leaf node? 1 is a leaf node, 0 is not a leaf node
    bool is_leaf;
    //how many keys does this node contain
    long num_keys;
    // the keys in the node
    btree_kvpair keys[MAX_KEYS];
    //kids[i] holds nodes < keys[i]
    struct bt_node* kids[MAX_KEYS+1];
};

btree btree_init() {
    btree b;
    b = malloc(sizeof(*b));
    assert(b);
    b->is_leaf = true;
    b->num_keys = 0;

    return b;
}

void btree_destroy(btree t) {
    int i;
    if (!t->is_leaf) {
        for(i = 0; i < t->num_keys+1; ++i) {
            btree_destroy(t->kids[i]);
        }
    }
    free(t);
}

/**
 * search key in a tree
 */
static long search_key(long n, btree_kvpair *a, long key) {
    long low;
    long high;
    long mid;

    low = -1;
    high = n;

    while(low + 1 < high) {
        mid = (low + high) / 2;
        if (a[mid].value == key) {
            return mid;
        }
        else if (a[mid].value < key) {
            low = mid;
        }
        else {
            high = mid;
        }
    }

    return high;
}

btree_kvpair btree_search(btree t, long key) {
    long pos;

    // have to check for empty tree
    if (t->num_keys == 0) {
        //create a meaningless kv pair
        btree_kvpair null_node = {-1, -1};
        return null_node;
    }

    pos = search_key(t->num_keys, t->keys, key);

    if(pos < t->num_keys && t->keys[pos].value == key) {
        return t->keys[pos];
    }
    else {
        if (t->is_leaf == 0) {
            return btree_search(t->kids[pos], key);
        }
        else {
            btree_kvpair null_node = {-1, -1};
            return null_node;
        }
    }
}

/**
 * insert a new key into a tree
 * returns new right sibling if the node splits
 * and puts the median in *median
 * else returns 0
 */
static btree btree_insert_internal(btree t, btree_kvpair kv_node, btree_kvpair *median) {
    long pos;
    //int mid;
    btree_kvpair mid;
    btree b_new;

    pos = search_key(t->num_keys, t->keys, kv_node.value);

    if(pos < t->num_keys && t->keys[pos].value == kv_node.value) {
        //do nothing
        return NULL;
    }

    if (t->is_leaf) {
        // every key above pos moves up one space
        memmove(&t->keys[pos+1], &t->keys[pos], sizeof(*(t->keys)) * (t->num_keys - pos));
        t->keys[pos] = kv_node;
        t->num_keys++;
    }
    else {
        b_new = btree_insert_internal(t->kids[pos], kv_node, &mid);

        if (b_new) {
            //every key above pos moves up one space
            memmove(&t->keys[pos+1], &t->keys[pos], sizeof(*(t->keys)) * (t->num_keys - pos));
            //new kid goes in pos + 1
            memmove(&t->kids[pos+2], &t->kids[pos+1], sizeof(*(t->keys)) * (t->num_keys - pos));

            t->keys[pos] = mid;
            t->kids[pos+1] = b_new;
            t->num_keys++;
        }
    }

    //we waste a tiny bit of space by splitting now instead of on next insert
    if(t->num_keys >= MAX_KEYS) {
        mid.value = t->num_keys / 2;
        *median = t->keys[mid.value];
        b_new = malloc(sizeof(*b_new));
        b_new->num_keys = t->num_keys - mid.value - 1;
        b_new->is_leaf = t->is_leaf;
        memmove(b_new->keys, &t->keys[mid.value+1], sizeof(*(t->keys)) * b_new->num_keys);

        if(!t->is_leaf) {
            memmove(b_new->kids, &t->kids[mid.value+1], sizeof(*(t->kids)) * (b_new->num_keys + 1));
        }
        t->num_keys = mid.value;
        return b_new;
    }
    else {
        return NULL;
    }
}

void btree_insert(btree t, btree_kvpair kv_node) {
    btree b_left;
    btree b_right;
    btree_kvpair median;

    b_right = btree_insert_internal(t, kv_node, &median);

    if (b_right != NULL) {
        //basic issue here is that we are at the root, so if we split, we have to make a new root
        b_left = malloc(sizeof (*b_left));
        assert(b_left);

        //copy root to b_left
        memmove(b_left, t, sizeof(*t));

        //make root point to b_left and b_right
        t->num_keys = 1;
        t->is_leaf = 0;
        t->keys[0] = median;
        t->kids[0] = b_left;
        t->kids[1] = b_right;
    }
}

long btree_inorder_traversal(btree t, long value_array[], long row_id_array[], long index) {
    if (t != NULL) {
        btree_inorder_traversal(t->kids[0], value_array, row_id_array, index);
        for(int i = 0; i < t->num_keys; ++i) {
            printf("row_id:%ld, value:%ld \n", t->keys[i].row_id, t->keys[i].value);
            value_array[index] = t->keys[i].value;
            row_id_array[index] = t->keys[i].row_id;
            index++;
        }
        btree_inorder_traversal(t->kids[1], value_array, row_id_array, index);
    }
    return index;
}