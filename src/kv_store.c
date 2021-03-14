#include <stdlib.h>
#include <memory.h>
#include "kv_store.h"
#include "utils_func.h"

#define PRIME 19
#define MAX_PROB 10

int kv_allocate(kvstore** kv_store, size_t size) {
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

int kv_deallocate(kvstore* kv_store) {
    for(int i = 0; i < kv_store->size; ++i) {
        kvpair* ikv = &(kv_store->kv_pair[i]);
        if(ikv != NULL) {
            free(ikv->key);
            free(ikv->value);
        }
    }
    free(kv_store);
    return 0;
}

int put(kvstore* kv_store, char* key, void* value, size_t value_size) {
    int index = hash_func(key, PRIME, kv_store->size);
    kvpair* cur_ikv = &(kv_store->kv_pair[index]);
    int prob = 0;
    while(cur_ikv->key != NULL) {
        index++;
        if (prob > MAX_PROB) {
            if(kv_rehash(&kv_store,(kv_store->size)*4+1, value_size)!= 0) {
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

int put_replace(kvstore* kv_store, char* key, void* value, size_t value_size) {
    int index = hash_func(key, PRIME, kv_store->size);
    kvpair* cur_ikv = &(kv_store->kv_pair[index]);
    if(cur_ikv->key == NULL) {
        kv_store->kv_pair[index].key = malloc(sizeof(char)*(strlen(key)+1));
        kv_store->kv_pair[index].value = malloc(value_size);
        if(kv_store->kv_pair[index].key == NULL || kv_store->kv_pair[index].value == NULL) {
            return 1;
        }
        strcpy(kv_store->kv_pair[index].key, key);
        memcpy(kv_store->kv_pair[index].value, value, value_size);
        kv_store->count++;
    }
    else {
        free(kv_store->kv_pair[index].key);
        free(kv_store->kv_pair[index].value);
        kv_store->kv_pair[index].key = malloc(sizeof(char)*(strlen(key)+1));
        kv_store->kv_pair[index].value = malloc(value_size);
        if(kv_store->kv_pair[index].key == NULL || kv_store->kv_pair[index].value == NULL) {
            return 1;
        }
        strcpy(kv_store->kv_pair[index].key, key);
        memcpy(kv_store->kv_pair[index].value, value, value_size);
    }
    return 0;
}

void* get(kvstore* kv_store, char* key) {
    int index = hash_func(key, PRIME, kv_store->size);
    kvpair* ikv = &(kv_store->kv_pair[index]);
    int prob = 0;
    while(ikv->key != NULL) {
        if(strcmp(ikv->key, key) == 0) {
            return ikv->value;
        }
        index++;
        if(prob > MAX_PROB) {
            log_err("cannot find the kv pair given the key.\n");
            break;
        }
        prob++;
        if(index >= kv_store->size) {
            index = 0;
        }
        ikv = &(kv_store->kv_pair[index]);
    }
    return NULL;
}

int kv_rehash(kvstore** kv_store, size_t nc, size_t value_size) {
    log_info("rehash start.\n");
    kvstore* new_db_store;
    new_db_store = malloc(sizeof(kvstore));
    if(new_db_store == NULL) {
        return 1;
    }
    new_db_store->size = nc;
    new_db_store->count = 0;
    new_db_store->kv_pair = calloc(nc, sizeof(kvpair));
    if(new_db_store->kv_pair == NULL) {
        return 1;
    }
    size_t oc = (*kv_store)->size;
    for(int i = 0; i < oc; ++i) {
        kvpair* oikv = &((*kv_store)->kv_pair[i]);
        if(oikv->key != NULL) {
            int index = hash_func(oikv->key, PRIME, nc);
            kvpair* nikv = &(new_db_store->kv_pair[index]);
            while(nikv->key != NULL) {
                index++;
                if (index >= nc) {
                    index = 0;
                }
                nikv = &(new_db_store->kv_pair[index]);
            }
            new_db_store->kv_pair[index].key = malloc((strlen(oikv->key)+1)* sizeof(char));
            strcpy(new_db_store->kv_pair[index].key, oikv->key);
            new_db_store->kv_pair[index].value = malloc(value_size);
            memcpy(new_db_store->kv_pair[index].value, oikv->value, value_size);
            new_db_store->count++;
        }
    }
    kv_deallocate(*kv_store);
    *kv_store = malloc(sizeof(kvstore));
    memcpy(*kv_store, new_db_store, sizeof(kvstore));
    log_info("rehash complete.\n");
    return 0;
}

