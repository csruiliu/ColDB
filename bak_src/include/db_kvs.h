#ifndef COLDB_DB_KVS_H
#define COLDB_DB_KVS_H

typedef struct KV {
    char* key;
    void* value;
} KV;

typedef struct KVStore {
    size_t size;
    size_t count;
    KV* kvs;
} KVStore;

int allocate(KVStore** kvStore, size_t size);

int put(KVStore* kvStore, char* key, void* value, size_t value_size);

void* get(KVStore* kvStore, char* key);

int deallocate(KVStore* kvStore);

int rehash(KVStore** kvStore, size_t nc, size_t value_size);

int put_replace(KVStore* kvStore, char* key, void* value, size_t value_size);

#endif
