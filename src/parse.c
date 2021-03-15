/**
 * This file contains methods necessary to parse input from the client.
 * Mostly, functions in parse.c will take in string input and map these
 * strings into database operators. This will require checking that the
 * input from the client is in the correct format and maps to a valid
 * database operator.
 **/

#define _BSD_SOURCE
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>

#include "parse.h"
#include "utils_func.h"

/**
 * This method takes in a string representing the arguments to create a table.
 * It parses those arguments, checks that they are valid, and creates a table.
 **/
/*
message_status parse_create_tbl(char* create_arguments) {
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* table_name = next_token(create_arguments_index, &status);
    char* db_name = next_token(create_arguments_index, &status);
    char* col_cnt = next_token(create_arguments_index, &status);

    // not enough arguments
    if (status == INCORRECT_FORMAT) {
        return status;
    }

    // Get the table name free of quotation marks
    table_name = trim_quote(table_name);

    // read and chop off last char, which should be a ')'
    int last_char = strlen(col_cnt) - 1;
    if (col_cnt[last_char] != ')') {
        return INCORRECT_FORMAT;
    }
    // replace the ')' with a null terminating character. 
    col_cnt[last_char] = '\0';

    // check that the database argument is the current active database
    if (strcmp(current_db->name, db_name) != 0) {
        cs165_log(stdout, "query unsupported. Bad db name");
        return QUERY_UNSUPPORTED;
    }

    // turn the string column count into an integer, and check that the input is valid.
    int column_cnt = atoi(col_cnt);
    if (column_cnt < 1) {
        return INCORRECT_FORMAT;
    }
    Status create_status;
    create_table(current_db, table_name, column_cnt, &create_status);
    if (create_status.code != OK) {
        cs165_log(stdout, "adding a table failed.");
        return EXECUTION_ERROR;
    }

    return status;
}
*/
/**
 * This method takes in a string representing the arguments to create a database.
 * It parses those arguments, checks that they are valid, and creates a database.
 **/
/*
message_status parse_create_db(char* create_arguments) {
    char *token;
    token = strsep(&create_arguments, ",");
    // not enough arguments if token is NULL
    if (token == NULL) {
        return INCORRECT_FORMAT;                    
    } else {
        // create the database with given name
        char* db_name = token;
        // trim quotes and check for finishing parenthesis.
        db_name = trim_quotes(db_name);
        int last_char = strlen(db_name) - 1;
        if (last_char < 0 || db_name[last_char] != ')') {
            return INCORRECT_FORMAT;
        }
        // replace final ')' with null-termination character.
        db_name[last_char] = '\0';

        token = strsep(&create_arguments, ",");
        if (token != NULL) {
            return INCORRECT_FORMAT;
        }
        if (add_db(db_name, true).code == OK) {
            return OK_DONE;
        } else {
            return EXECUTION_ERROR;
        }
    }
}
*/
/**
 * parse_create parses a create statement and then passes the necessary arguments off to the next function
 **/
/*
message_status parse_create(char* create_arguments) {
    message_status mes_status;
    char *tokenizer_copy, *to_free;
    // Since strsep destroys input, we create a copy of our input. 
    tokenizer_copy = to_free = malloc((strlen(create_arguments)+1) * sizeof(char));
    char *token;
    strcpy(tokenizer_copy, create_arguments);
    // check for leading parenthesis after create. 
    if (strncmp(tokenizer_copy, "(", 1) == 0) {
        tokenizer_copy++;
        // token stores first argument. Tokenizer copy now points to just past first ","
        token = next_token(&tokenizer_copy, &mes_status);
        if (mes_status == INCORRECT_FORMAT) {
            return mes_status;
        } else {
            // pass off to next parse function. 
            if (strcmp(token, "db") == 0) {
                mes_status = parse_create_db(tokenizer_copy);
            } else if (strcmp(token, "tbl") == 0) {
                mes_status = parse_create_tbl(tokenizer_copy);
            } else {
                mes_status = UNKNOWN_COMMAND;
            }
        }
    } else {
        mes_status = UNKNOWN_COMMAND;
    }
    free(to_free);
    return mes_status;
}
*/

/**
 * parse_insert reads in the arguments for a create statement and 
 * then passes these arguments to a database function to insert a row.
 **/
DbOperator* parse_insert(char* query_command, message* send_message) {
    unsigned int columns_inserted = 0;
    char* token = NULL;
    char* insert_table_name = next_token_comma(&query_command, &send_message->status);
    if (send_message->status == INCORRECT_FORMAT) {
        return NULL;
    }
    Table* insert_table = get_table(insert_table_name);
    if (insert_table == NULL) {
        send_message->status = OBJECT_NOT_FOUND;
        return NULL;
    }
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = INSERT;
    dbo->operator_fields.insert_operator.table = insert_table;
    dbo->operator_fields.insert_operator.values = malloc(sizeof(int) * insert_table->size);
    while ((token = strsep(&query_command, ",")) != NULL) {
        int insert_val = atoi(token);
        dbo->operator_fields.insert_operator.values[columns_inserted] = insert_val;
        columns_inserted++;
    }
    if (columns_inserted != insert_table->size) {
        send_message->status = INCORRECT_FORMAT;
        free (dbo);
        return NULL;
    }
    return dbo;
}


/**
 * parse_create parses a create statement and then passes the necessary arguments off to the next function
 **/
DbOperator* parse_create_db(char* query_command) {
    char* db_name = trim_quote(query_command);
    int last_char = (int)strlen(db_name) - 1;
    if (last_char < 0 || db_name[last_char] != ')') {
        return NULL;
    }
    // add new null terminator
    db_name[last_char] = '\0';
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->operator_fields.create_db_operator.db_name = malloc((strlen(db_name)+1)*sizeof(char));
    strcpy(dbo->operator_fields.create_db_operator.db_name, db_name);
    dbo->type = CREATE_DB;
    return dbo;
}

/**
 * parse_command takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 **/
DbOperator* parse_command(char* query_command, message* send_message, int client_socket, ClientContext* context) {
    DbOperator *dbo = NULL; // = malloc(sizeof(DbOperator)); // calloc?

    if (strncmp(query_command, "--", 2) == 0) {
        send_message->status = OK_DONE;
        // The -- signifies a comment line, no operator needed.  
        return NULL;
    }

    char *equals_pointer = strchr(query_command, '=');
    char *handle = query_command;
    if (equals_pointer != NULL) {
        // handle exists, store here. 
        *equals_pointer = '\0';
        coldb_log(stdout, "FILE HANDLE: %s\n", handle);
        query_command = ++equals_pointer;
    } else {
        handle = NULL;
    }

    coldb_log(stdout, "QUERY: %s\n", query_command);

    send_message->status = OK_WAIT_FOR_RESPONSE;
    query_command = trim_whitespace(query_command);

    // check what command is given.
    if (strncmp(query_command, "create(db,", 10) == 0) {
        query_command += 10;
        dbo = parse_create_db(query_command);
    }
    else if (strncmp(query_command, "relational_insert", 17) == 0) {
        query_command += 17;
        dbo = parse_insert(query_command, send_message);
    }
    else {
        return NULL;
    }
    
    dbo->client_fd = client_socket;
    dbo->context = context;
    return dbo;
}
