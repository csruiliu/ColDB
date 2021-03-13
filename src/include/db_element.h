#ifndef ELEMENT_H
#define ELEMENT_H

// Limits the size of a name in our database to 64 characters
#define MAX_SIZE_NAME 64

/**
 * EXTRA
 * DataType
 * Flag to mark what type of data is held in the struct.
 * You can support additional types by including this enum and using void*
 * in place of int* in db_operator similar to the way IndexType supports
 * additional types.
 **/
typedef enum DataType {
    INT,
    LONG,
    FLOAT
} DataType;

typedef struct Column {
    char name[MAX_SIZE_NAME];
    int* data;
    // You will implement column indexes later.
    void* index;
    //struct ColumnIndex *index;
    //bool clustered;
} Column;

/**
 * Table
 * Defines a table structure, which is composed of multiple columns.
 * We do not require you to dynamically manage the size of your tables,
 * although you are free to append to the struct if you would like to (i.e.,
 * include a size_t table_size).
 * name, the name associated with the table. table names must be unique
 *     within a database, but tables from different databases can have the same
 *     name.
 * - col_count, the number of columns in the table
 * - col,umns this is the pointer to an array of columns contained in the table.
 * - table_length, the size of the columns in the table.
 **/
typedef struct Table {
    char name [MAX_SIZE_NAME];
    Column *columns;
    size_t col_count;
    size_t table_length;
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
    char name[MAX_SIZE_NAME];
    Table *tables;
    size_t tables_size;
    size_t tables_capacity;
} Db;

/**
 *  Declares the type of a result column, which includes the number of tuples in the result,
 *  the data type of the result, and a pointer to the result data
 **/
typedef struct Result {
    size_t num_tuples;
    DataType data_type;
    void *payload;
} Result;

#endif
