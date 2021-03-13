#include <stdlib.h>
#include <memory.h>
#include "kv_store.h"
#include "utils_func.h"

#define PRIME 19
#define MAX_PROB 10

int allocate(kvstore** kv_store, size_t size) {
    *kv_store = malloc(sizeof(kv_store));
    if(*kv_store == NULL) {
        return 1;
    }
    (*kv_store)->size = size;
    (*kv_store)->count = 0;
    (*kv_store)->kv_pair = calloc(size,sizeof(kvpair));
    if((*kv_store)->kv_pair == NULL) {
        return 1;
    }
    return 0;
}

int put(kvstore* kv_store, char* key, void* value, size_t value_size) {
    int index = hash_func(key, PRIME, kv_store->size);
    kvpair* cur_ikv = &(kv_store->kv_pair[index]);
    int prob = 0;
    while(cur_ikv->key != NULL) {
        index++;
        if (prob > MAX_PROB) {
            if(rehash(&kv_store,(kv_store->size)*4+1, value_size)!= 0) {
                log_err("rehash the kv store failed.\n");
                return 1;
            }
            else{
                return 2;
            }
        }
        prob++;
        if (index >= kv_store->size) {
            index = 0;
        }
        cur_ikv = &(kv_store->kv_pair[index]);
    }
    kv_store->kv_pair[index].key = malloc(sizeof(char)*(strlen(key)+1));
    kv_store->kv_pair[index].value = malloc(value_size);
    if(kv_store->kv_pair[index].key == NULL || kv_store->kv_pair[index].value == NULL) {
        return 1;
    }
    strcpy(kv_store->kv_pair[index].key,key);
    memcpy(kv_store->kv_pair[index].value, value, value_size);
    kv_store->count++;
    return 0;
}

