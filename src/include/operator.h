#ifndef OPERATOR_H__
#define OPERATOR_H__
#include "db_element.h"

// Limits the size of a name in our database to 64 characters
#define HANDLE_MAX_SIZE 64

/**
 * necessary fields for insertion
 **/
typedef struct InsertOperator {
    Table* table;
    int* values;
} InsertOperator;

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
 * necessary fields for open
 **/
typedef struct OpenOperator {
    char* db_name;
} OpenOperator;

/**
 *  unifying type holding either a column or a result
 **/
typedef struct GeneralizedColumn {
    GeneralizedColumnType column_type;
    GeneralizedColumnPointer column_pointer;
} GeneralizedColumn;

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
 * Union type holding the fields of any operator
 **/
typedef union OperatorFields {
    InsertOperator insert_operator;
    OpenOperator open_operator;
} OperatorFields;

/**
 * Supported type of operator
 **/
typedef enum OperatorType {
    CREATE,
    INSERT,
    OPEN,
} OperatorType;

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
