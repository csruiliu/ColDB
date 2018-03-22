/*
 *  Created by Rui Liu on 3/18/18.
 *  Header file for ColDB fundamental data structure (FDS)
 */
#ifndef COLDB_DB_FDS_H
#define COLDB_DB_FDS_H

#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <stddef.h>


typedef enum IndexType {
    UNIDX,
    BTREE,
    SORTED
} IndexType;

typedef enum ClsType {
    UNCLSR,
    CLSR,
    PRICLSR
} ClsType;

/**
 * DataType
 * Defines the data type in column, which include INT, LONG INT, FLOAT.
 * Thus, ColDB only supports these three data types.
 **/
typedef enum DataType {
    INT,
    LONG,
    FLOAT
} DataType;

/**
 * Declares the type of a result column, which includes the number of tuples in the result, the data type of the result, and a pointer to the result data
 **/
typedef struct Result {
    size_t num_tuples;
    DataType data_type;
    void *payload;
} Result;

/**
 * Column
 * Defines a column structure, which is composed of col-name and data record.
 * - col_name, the name associated with the column, which follows the format [db-name.table-name.column-name].
 *   Thus, column names must be unique.
 * - data_count, the number of data in the column
 * - data, this is the pointer to an array of data contained in the .
 * - data_size, the current max size of the data records in the column.
 **/
typedef struct Column {
    char* col_name;
    int* data;
    size_t data_count;
    size_t data_size;
    void* index;
    IndexType idx_type;
    ClsType cls_type;
} Column;

typedef struct Table {
    char* tbl_name;
    char* pricls_col_name;
    int hasCls;
    Column** columns;
    size_t table_size;
    size_t table_capacity;
} Table;

/**
 * Db
 * Defines a database structure, which is composed of multiple tables.
 * - db_name: the name of the associated database.
 * - tables: the pointer to the array of tables contained in the db.
 * - db_size: the size of the array holding table objects
 * - db_capacity: the amount of pointers that can be held in the currently allocated memory slot
 **/
typedef struct Db {
    char* db_name;
    Table** tables;
    size_t db_size;
    size_t db_capacity;
} Db;

extern Db *current_db;

void init_db_store(size_t size);
void init_tbl_store(size_t size);
void init_col_store(size_t size);
void init_rls_store(size_t size);
void init_idx_store(size_t size);

void put_db(char* db_name, Db* db);
int put_tbl(char* tbl_name, Table* tbl);

Db* get_db(char *db_name);
Table* get_tbl(char* tbl_name);

void free_db_store();
void free_tbl_store();
void free_col_store();
void free_rsl_store();
void free_idx_store();

#endif //COLDB_DB_FDS_H
