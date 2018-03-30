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
 * - col_name: the name associated with the column, which follows the format [db-name.table-name.column-name].
 *   Thus column names must be unique.
 * - data: this is the pointer to an array of data contained in the column.
 * - col_size: the size of the current data in the column
 * - col_capacity: the max amount of data in the column
 * - idx_type: the index type of the column
 * - cls_type: the clustering type of the index in the column
 **/
typedef struct Column {
    char* col_name;
    int* data;
    int* rowId;
    size_t col_size;
    size_t col_capacity;
    IndexType idx_type;
    ClsType cls_type;
} Column;

/**
 * Table
 * Defines a table structure, which is composed of multiple columns.
 * - tbl_name: the name of the associated table, which follows the format [db-name.table-name]
 * - columns: the pointer to the array of tables contained in the db.
 * - tbl_size: the size of the current column objects
 * - tbl_capacity: the max amount of column objects in table
 * - pricls_col_name: the name of primary clustering column
 * - hasCls: the table has primary clustering column or not, 1 means has primary clustring column, 0 is not.
 **/
typedef struct Table {
    char* tbl_name;
    Column** columns;
    size_t tbl_size;
    size_t tbl_capacity;
    char* pricls_col_name;
    int hasCls;
} Table;

/**
 * Db
 * Defines a database structure, which is composed of multiple tables.
 * - db_name: the name of the associated database.
 * - tables: the pointer to the array of tables contained in the db.
 * - db_size: the size of the current table objects
 * - db_capacity: the max amount of table objects in database
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
void put_tbl(char* tbl_name, Table* tbl);
void put_col(char* col_name, Column* col);

Db* get_db(char *db_name);
Table* get_tbl(char* tbl_name);
Column* get_col(char* col_name);

void free_db_store();
void free_tbl_store();
void free_col_store();
void free_rsl_store();
void free_idx_store();

#endif //COLDB_DB_FDS_H
