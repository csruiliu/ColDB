#ifndef ELEMENT_H
#define ELEMENT_H

// Limits the size of a name in our database to 64 characters
#define MAX_SIZE_NAME 64

#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <stddef.h>

typedef enum IndexType {
    UNIDX,
    BTREE,
    SORTED
} IndexType;

/**
 * EXTRA
 * DataType
 * Flag to mark what type of data is held in the struct.
 * You can support additional types by including this enum and using void*
 * in place of int* in db_operator similar to the way IndexType supports
 * additional types.
 **/
typedef enum DataType {
    //INT,
    LONG,
    FLOAT
} DataType;

/**
 * Cluster Index Type
 * UNCLSR: non-cluster index
 * CLSR: cluster index
 * PRICLSR: primary cluster index
 **/
typedef enum ClsType {
    UNCLSR,
    CLSR,
    PRICLSR
} ClsType;

/**
 * Column
 * Defines a column structure, which is composed of multiple data and row id
 * - name: the name of the column, which follows the format [db_name.table_name.col_name]
 * - aff_table_name: the table_name which the column belongs to
 * - data: the pointer that point to the actual data in the column
 * - rowId: the pointer that point to the row id of the actual data in the column
 * - size: the current size of the column
 * - capacity: the max amount of column objects in the column
 * - idx_type: the index type of the column
 * - cls_type: the cluster index type of the column
 */
typedef struct Column {
    char* name;
    char* aff_tbl_name;
    long* data;
    long* rowId;
    size_t size;
    size_t capacity;
    IndexType idx_type;
    ClsType cls_type;
} Column;

/**
 * Table
 * Defines a table structure, which is composed of multiple columns.
 * We do not require you to dynamically manage the size of your tables,
 * although you are free to append to the struct if you would like to (i.e.,
 * include a size_t table_size).
 * - name: the name of the associated table, which follows the format [db_name.table_name]
 * - aff_db_name: the db_name which the table belongs to
 * - columns: the pointer to the array of tables contained in the db.
 * - table_size: the size of the current column objects
 * - table_capacity: the max amount of column objects in table
 * - pricluster_index_col_name: the name of primary clustering column
 * - has_cluster_index: the table has primary clustering column or not, 1 means has primary clustring column, 0 is not.
 **/
typedef struct Table {
    char* name;
    char* aff_db_name;
    Column** columns;
    size_t size;
    size_t capacity;
    char* pricluster_index_col_name;
    int has_cluster_index;
} Table;

/**
 * DB
 * Defines a database structure, which is composed of multiple tables.
 * - name: the name of the associated database.
 * - tables: the pointer to the array of tables contained in the db.
 * - tables_size: the size of the array holding table objects
 * - tables_capacity: the amount of pointers that can be held in the currently allocated memory slot
 **/
typedef struct Db {
    char* name;
    Table** tables;
    size_t size;
    size_t capacity;
} Db;

/**
 *  Declares the type of a result, which includes the number of tuples in the result,
 *  the data type of the result, and a pointer to the result data
 **/
typedef struct Result {
    size_t num_tuples;
    DataType data_type;
    void *payload;
} Result;

/**
 *  Declares the type of a index
 *  the name of an index, and a pointer to the index
typedef struct Index {
    char* name;
    void* index_instance;
} Index;
**/

/**
 *  All the function definitions used for kv store of db
 **/
void init_db_store(size_t size);

Db* get_db(char *db_name);

void put_db(char* db_name, Db* db);

void free_db_store();

/**
 *  All the function definitions used for kv store of table
 **/
void init_table_store(size_t size);

Table* get_table(char* table_name);

void put_table(char* table_name, Table* table);

void free_table_store();
/**
 *  All the function definitions used for kv store of column
 **/
void init_column_store(size_t size);

Column* get_column(char* col_name);

void put_column(char* col_name, Column* col);

void free_column_store();

/**
 *  All the function definitions used for kv store of result
 **/
void init_result_store(size_t size);

Result* get_result(char* result_name);

void replace_result(char* result_name, Result* result);

void free_result_store();

/**
 *  All the function definitions used for kv store of index
 **/
void init_index_store(size_t size);

void* get_index(char* index_name);

void put_index(char* index_name, void* index, IndexType index_type);

void replace_index(char* index_name, void* index, IndexType index_type);

void free_index_store();

#endif
