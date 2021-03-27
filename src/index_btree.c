#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "utils_func.h"
#include "index_btree.h"

#define MAX_KEYS (16)

struct bt_node {
    //is this a leaf node? 1 is a leaf node, 0 is not a leaf node
    int is_leaf;
    //how many keys does this node contain
    int num_keys;
    // the keys in the node
    int keys[MAX_KEYS];
    //kids[i] holds nodes < keys[i]
    struct bt_node* kids[MAX_KEYS+1];
};

btree btree_init() {
    btree b;
    b = malloc(sizeof(*b));
    assert(b);
    b->is_leaf = 1;
    b->num_keys = 0;

    return b;
}

void btree_destroy(btree t) {
    size_t i;
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
static int search_key(int n, const int *a, int key) {
    int low;
    int high;
    int mid;

    low = -1;
    high = n;

    while(low + 1 < high) {
        mid = (low + high) / 2;
        if (a[mid] == key) {
            return mid;
        }
        else if (a[mid] < key) {
            low = mid;
        }
        else {
            high = mid;
        }
    }

    return high;
}

int btree_search(btree t, int key) {
    int pos;

    // have to check for empty tree
    if (t->num_keys == 0) {
        return 0;
    }

    pos = search_key(t->num_keys, t->keys, key);

    if(pos < t->num_keys && t->keys[pos] == key) {
        return 1;
    }
    else {
        return (!t->is_leaf && btree_search(t->kids[pos], key));
    }
}

/**
 * insert a new key into a tree
 * returns new right sibling if the node splits
 * and puts the median in *median
 * else returns 0
 */

static btree btree_insert_internal(btree t, int key, int *median) {
    int pos;
    int mid;
    btree b_new;

    pos = search_key(t->num_keys, t->keys, key);

    if(pos < t->num_keys && t->keys[pos] == key) {
        return 0;
    }

    if (t->is_leaf) {
        // every key above pos moves up one space
        memmove(&t->keys[pos+1], &t->keys[pos], sizeof(*(t->keys)) * (t->num_keys - pos));
        t->keys[pos] = key;
        t->num_keys++;
    }
    else {
        b_new = btree_insert_internal(t->kids[pos], key, &mid);

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
        mid = t->num_keys / 2;
        *median = t->keys[mid];
        b_new = malloc(sizeof(*b_new));
        b_new->num_keys = t->num_keys - mid - 1;
        b_new->is_leaf = t->is_leaf;
        memmove(b_new->keys, &t->keys[mid+1], sizeof(*(t->keys)) * b_new->num_keys);

        if(!t->is_leaf) {
            memmove(b_new->kids, &t->kids[mid+1], sizeof(*(t->kids)) * (b_new->num_keys + 1));
        }
        t->num_keys = mid;
        return b_new;
    }
    else {
        return 0;
    }
}

void btree_insert(btree t, int key) {
    btree b_left;
    btree b_right;
    int median;

    b_right = btree_insert_internal(t, key, &median);

    if (b_right) {
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

void btree_inorder_traversal(btree t) {
    if (t != NULL) {
        btree_inorder_traversal(t->kids[0]);
        for(size_t i = 0; i < t->num_keys; ++i) {
            printf("%d", t->keys[i]);
        }
        btree_inorder_traversal(t->kids[1]);
    }
}