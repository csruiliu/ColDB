#ifndef OPERATOR_H
#define OPERATOR_H

#include "db_element.h"

/**
 * Limits the size of a name in our database to 64 characters
 */
#define HANDLE_MAX_SIZE 64

/**
* Defines a comparator flag between two values
**/
typedef enum ComparatorType {
    NO_COMPARISON = 0,
    LESS_THAN = 1,
    GREATER_THAN = 2,
    EQUAL = 4,
    LESS_THAN_OR_EQUAL = 5,
    GREATER_THAN_OR_EQUAL = 6
} ComparatorType;

/**
 *  an enum which allows us to differentiate between columns and results
 **/
typedef enum GeneralizedColumnType {
    RESULT,
    COLUMN
} GeneralizedColumnType;

/**
 *  a union type holding either a column or a result struct
 **/
typedef union GeneralizedColumnPointer {
    Result* result;
    Column* column;
} GeneralizedColumnPointer;

/**
 *  unifying type holding either a column or a result
 **/
typedef struct GeneralizedColumn {
    GeneralizedColumnType column_type;
    GeneralizedColumnPointer column_pointer;
} GeneralizedColumn;

/**
 * Comparator
 * A comparator defines a comparison operation over a column.
 **/
typedef struct Comparator {
    long int p_low; // used in equality and ranges.
    long int p_high; // used in range compares.
    GeneralizedColumn* gen_col;
    ComparatorType type1;
    ComparatorType type2;
    char* handle;
} Comparator;

/**
 * necessary fields for insertion
 **/
typedef struct InsertOperator {
    Table* table;
    int* values;
} InsertOperator;

/**
 * necessary fields for open
 **/
typedef struct OpenOperator {
    char* db_name;
} OpenOperator;

/**
 *  used to refer to a column in our client context
 **/
typedef struct GeneralizedColumnHandle {
    char name[HANDLE_MAX_SIZE];
    GeneralizedColumn generalized_column;
} GeneralizedColumnHandle;

/**
 * holds the information necessary to refer to generalized columns (results or columns)
 **/
typedef struct ClientContext {
    GeneralizedColumnHandle* chandle_table;
    int chandles_in_use;
    int chandle_slots;
} ClientContext;

/**
 * necessary fields for create db command
 **/
typedef struct CreateDbOperator {
    char* db_name;
} CreateDbOperator;

/**
 * necessary fields for create table command
 **/
typedef struct CreateTableOperator {
    char* db_name;
    char* table_name;
    size_t col_count;
} CreateTableOperator;

/**
 * necessary fields for create column command
 **/
typedef struct CreateColOperator {
    char* tbl_name;
    char* col_name;
} CreateColOperator;

/**
 * necessary fields for create index command
 **/
typedef struct CreateIdxOperator {
    char* idx_col_name;
    char* idx_tbl_name;
    IndexType col_idx;
    bool is_cluster;
} CreateIdxOperator;

/**
 * Supported type of operator
 **/
typedef enum OperatorType {
    CREATE_DB,
    CREATE_TBL,
    CREATE_COL,
    CREATE_IDX,
    LOAD,
    SHUTDOWN,
    INSERT,
    SELECT,
    FETCH,
    PRINT,
    AVG,
    SUM,
    ADD,
    SUB,
    MAX,
    MIN,
    BATCH_QUERY,
    BATCH_EXEC
} OperatorType;

/**
 * Union type holding the fields of any operator
 **/
typedef union OperatorFields {
    InsertOperator insert_operator;
    CreateDbOperator create_db_operator;
    CreateTableOperator create_table_operator;
    CreateColOperator create_col_operator;
    CreateIdxOperator create_idx_operator;
} OperatorFields;



/**
 * DbOperator holds the following fields:
 * type: the type of operator to perform (i.e. insert, select, ...)
 * operator fields: the fields of the operator in question
 * client_fd: the file descriptor of the client that this operator will return to
 * context: the context of the operator in question. This context holds the local results of the client in question.
 **/
typedef struct DbOperator {
    OperatorType type;
    OperatorFields operator_fields;
    int client_fd;
    ClientContext* context;
} DbOperator;

#endif
