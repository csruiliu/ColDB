#ifndef PARSE_H__
#define PARSE_H__

#include "message.h"
#include "db_fds.h"

/*
 * tells the databaase what type of operator this is
 */
typedef enum OperatorType {
    CREATE,
    INSERT,
    OPEN,
} OperatorType;

/*
 * an enum which allows us to differentiate between columns and results
 */
typedef enum GeneralizedColumnType {
    RESULT,
    COLUMN
} GeneralizedColumnType;

/*
 * a union type holding either a column or a result struct
 */
typedef union GeneralizedColumnPointer {
    Result* result;
    Column* column;
} GeneralizedColumnPointer;

/*
 * unifying type holding either a column or a result
 */
typedef struct GeneralizedColumn {
    GeneralizedColumnType column_type;
    GeneralizedColumnPointer column_pointer;
} GeneralizedColumn;

/*
 * used to refer to a column in our client context
 */
typedef struct GeneralizedColumnHandle {
    char* name;
    GeneralizedColumn generalized_column;
} GeneralizedColumnHandle;


/*
 * holds the information necessary to refer to generalized columns (results or columns)
 */
typedef struct ClientContext {
    GeneralizedColumnHandle* chandle_table;
    int chandles_in_use;
    int chandle_slots;
} ClientContext;

/*
 * necessary fields for insertion
 */
typedef struct InsertOperator {
    Table* table;
    int* values;
} InsertOperator;

/*
 * necessary fields for open
 */
typedef struct OpenOperator {
    char* db_name;
} OpenOperator;


/*
 * union type holding the fields of any operator
 */
typedef union OperatorFields {
    InsertOperator insert_operator;
    OpenOperator open_operator;
} OperatorFields;

typedef struct DbOperator {
    OperatorType type;
    OperatorFields operator_fields;
    int client_fd;
    ClientContext* context;
} DbOperator;

/**
 * Error codes used to indicate the outcome of an API call
 **/
typedef enum StatusCode {
    /* The operation completed successfully */
            OK,
    /* There was an error with the call. */
            ERROR,
} StatusCode;

// status declares an error code and associated message
typedef struct Status {
    StatusCode code;
    char* error_message;
} Status;

DbOperator* parse_command(char* query_command, message* send_message, int client, ClientContext* context);

#endif
