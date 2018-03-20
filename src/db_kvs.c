#include <stdlib.h>
#include <memory.h>
#include <math.h>
#include <utils.h>
#include "db_kvs.h"

#define PRIME 19
#define MAX_PROB 10

int allocate(KVStore** kvStore, size_t size) {
    *kvStore = malloc(sizeof(KVStore));
    if(*kvStore == NULL) {
        return 1;
    }
    (*kvStore)->size = size;
    (*kvStore)->count = 0;
    (*kvStore)->kvs = calloc(size,sizeof(KV));
    if((*kvStore)->kvs == NULL) {
        return 1;
    }
    return 0;
}

int put(KVStore* kvStore, char* key, void* value, size_t value_size) {
    int index = hash_func(key, PRIME, kvStore->size);
    KV* cur_ikv = &(kvStore->kvs[index]);
    int prob = 0;
    while(cur_ikv->key != NULL) {
        index++;
        if (prob > MAX_PROB) {
            if(rehash(&kvStore,(kvStore->size)*4+1,value_size)!= 0) {
                log_err("rehash the kv store failed.\n");
                return 1;
            }
            else{
                return 2;
            }
        }
        prob++;
        if (index >= kvStore->size) {
            index = 0;
        }
        cur_ikv = &(kvStore->kvs[index]);
    }
    kvStore->kvs[index].key = malloc(sizeof(char)*(strlen(key)+1));
    kvStore->kvs[index].value = malloc(value_size);
    if(kvStore->kvs[index].key == NULL || kvStore->kvs[index].value == NULL) {
        return 1;
    }
    strcpy(kvStore->kvs[index].key,key);
    memcpy(kvStore->kvs[index].value,value,value_size);
    kvStore->count++;
    return 0;
}

int rehash(KVStore** kvStore, size_t nc, size_t value_size) {
    log_info("rehash start.\n");
    KVStore* new_db_store;
    new_db_store = malloc(sizeof(KVStore));
    if(new_db_store == NULL) {
        return 1;
    }
    new_db_store->size = nc;
    new_db_store->count = 0;
    new_db_store->kvs = calloc(nc, sizeof(KV));
    if(new_db_store->kvs == NULL) {
        return 1;
    }
    size_t oc = (*kvStore)->size;
    for(int i = 0; i < oc; ++i) {
        KV* oikv = &((*kvStore)->kvs[i]);
        if(oikv->key != NULL) {
            int index = hash_func(oikv->key, PRIME, nc);
            KV* nikv = &(new_db_store->kvs[index]);
            while(nikv->key != NULL) {
                index++;
                if (index >= nc) {
                    index = 0;
                }
                nikv = &(new_db_store->kvs[index]);
            }
            new_db_store->kvs[index].key = malloc((strlen(oikv->key)+1)* sizeof(char));
            strcpy(new_db_store->kvs[index].key,oikv->key);
            new_db_store->kvs[index].value = malloc(value_size);
            memcpy(new_db_store->kvs[index].value,oikv->value,value_size);
            new_db_store->count++;
        }
    }
    deallocate(*kvStore);
    *kvStore = malloc(sizeof(KVStore));
    memcpy(*kvStore, new_db_store, sizeof(KVStore));
    log_info("rehash complete.\n");
    return 0;
}

void* get(KVStore* kvStore, char* key) {
    int index = hash_func(key, PRIME, kvStore->size);
    KV* ikv = &(kvStore->kvs[index]);
    int prob = 0;
    while(ikv->key != NULL) {
        if(strcmp(ikv->key, key) == 0) {
            return ikv->value;
        }
        index++;
        if(prob > PRIME) {
            log_err("cannot find the kv pair given the key.\n");
            break;
        }
        prob++;
        if(index >= kvStore->size) {
            index = 0;
        }
        ikv = &(kvStore->kvs[index]);
    }
    return NULL;
}

int hash_func(const char* s, const int a, const int m) {
    long hash = 0;
    const int len_s = strlen(s);
    for (int i = 0; i < len_s; i++) {
        hash += (long)pow(a, len_s - (i+1)) * s[i];
        hash = hash % m;
    }
    return (int)hash;
}

int deallocate(KVStore* kvStore) {
    for(int i = 0; i < kvStore->size; ++i) {
        KV* ikv = &(kvStore->kvs[i]);
        if(ikv->key != NULL) {
            free(ikv->key);
            free(ikv->value);
        }
    }
    free(kvStore);
    return 0;
}