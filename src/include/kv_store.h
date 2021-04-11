#ifndef KVSTORE_H
#define KVSTORE_H

typedef struct kvpair {
    char* key;
    void* value;
} kvpair;

typedef struct kvstore {
    size_t size;
    size_t count;
    kvpair* kv_pair;
} kvstore;

int kv_allocate(kvstore** kv_store, size_t size);

int kv_deallocate(kvstore* kv_store);

int put(kvstore* kv_store, char* key, void* value, size_t value_size);

int delete(kvstore* kv_store, char* key);

int put_replace(kvstore* kv_store, char* key, void* value, size_t value_size);

void* get(kvstore* kv_store, char* key);

int kv_rehash(kvstore** kv_store, size_t nc, size_t value_size);

#endif