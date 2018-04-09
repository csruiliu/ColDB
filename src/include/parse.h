#ifndef PARSE_H__
#define PARSE_H__

#include "message.h"
#include "db_fds.h"

typedef enum MaxType {
    MAX_POS_VALUE,
    MAX_VALUE
} MaxType;

typedef enum MinType {
    MIN_POS_VALUE,
    MIN_VALUE
} MinType;

/*
 * tells the database what type of operator this is
 */
typedef enum HandleType {
    HANDLE_COL,
    HANDLE_RSL
} HandleType;

/*
 * tells the database what type of operator this is
 */
typedef enum OperatorType {
    CREATE_DB,
    CREATE_TBL,
    CREATE_COL,
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
    MIN
} OperatorType;


/*
 * necessary fields for Min
 */
typedef struct MinOperator {
    MinType min_type;
    char* min_vec_pos;
    char* min_vec_value;
    char* handle_pos;
    char* handle_value;
} MinOperator;

/*
 * necessary fields for Max
 */
typedef struct MaxOperator {
    MaxType max_type;
    char* max_vec_pos;
    char* max_vec_value;
    char* handle_pos;
    char* handle_value;
} MaxOperator;

/*
 * necessary fields for Sub
 */
typedef struct SubOperator {
    HandleType sub_type1;
    HandleType sub_type2;
    char* sub_name1;
    char* sub_name2;
    char* handle;
} SubOperator;

/*
 * necessary fields for add
 */
typedef struct AddOperator {
    HandleType add_type1;
    HandleType add_type2;
    char* add_name1;
    char* add_name2;
    char* handle;
} AddOperator;

/*
 * necessary fields for sum
 */
typedef struct SumOperator {
    HandleType handle_type;
    char* sum_name;
    char* handle;
} SumOperator;

/*
 * necessary fields for avg
 */
typedef struct AvgOperator {
    HandleType handle_type;
    char* avg_name;
    char* handle;
} AvgOperator;

/*
 * necessary fields for print
 */
typedef struct PrintOperator {
    size_t print_num;
    char** print_name;
} PrintOperator;


/*
 * necessary fields for fetch
 */
typedef struct FetchOperator {
    char* col_var_name;
    char* rsl_vec_pos;
    char* handle;
} FetchOperator;

/*
 * necessary fields for select
 */
typedef struct SelectOperator {
    char* select_col;
    char* select_rsl_pos;
    char* select_rsl_val;
    char* pre_range;
    char* post_range;
    char* handle;
    HandleType selectType;
} SelectOperator;

/*
 * necessary fields for insertion
 */
typedef struct InsertOperator {
    Table* insert_tbl;
    int* values;
} InsertOperator;

/*
 * necessary fields for open
 */
typedef struct OpenOperator {
    char* db_name;
} OpenOperator;

/*
 * necessary fields for create db command
 */
typedef struct CreateDbOperator {
    char* db_name;
} CreateDbOperator;

/*
 * necessary fields for create table command
 */
typedef struct CreateTblOperator {
    char* db_name;
    char* tbl_name;
    size_t col_count;
} CreateTblOperator;

/*
 * necessary fields for create column command
 */
typedef struct CreateColOperator {
    char* tbl_name;
    char* col_name;
} CreateColOperator;

/*
 * necessary fields for load data command
 */
typedef struct LoadOperator {
    char* data_path;
} LoadOperator;



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
 * union type holding the fields of any operator
 */
typedef union OperatorFields {
    InsertOperator insert_operator;
    CreateDbOperator create_db_operator;
    CreateTblOperator create_tbl_operator;
    CreateColOperator create_col_operator;
    LoadOperator load_operator;
    SelectOperator select_operator;
    FetchOperator fetch_operator;
    PrintOperator print_operator;
    AvgOperator avg_operator;
    SumOperator sum_operator;
    AddOperator add_operator;
    SubOperator sub_operator;
    MaxOperator max_operator;
    MinOperator min_operator;
} OperatorFields;

/*
 * DbOperator holds the following fields:
 * type: the type of operator to perform (i.e. insert, select, ...)
 * operator fields: the fields of the operator in question
 * client_fd: the file descriptor of the client that this operator will return to
 * context: the context of the operator in question. This context holds the local results of the client in question.
 */
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
