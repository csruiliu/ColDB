/**
 * This file contains methods necessary to parse input from the client.
 * Mostly, functions in parse.c will take in string input and map these
 * strings into database operators. This will require checking that the
 * input from the client is in the correct format and maps to a valid
 * database operator.
 **/

#define _DEFAULT_SOURCE

#include <string.h>

#include "parse.h"
#include "utils_func.h"

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
 * This method takes in a string representing the arguments to create a table.
 * It parses those arguments, checks that they are valid, and creates a table.
 **/
DbOperator* parse_create_tbl(char* query_command, message* send_message) {
    DbOperator* dbo;
    char* table_name = next_token_comma(&query_command, &send_message->status);
    char* db_name = next_token_comma(&query_command, &send_message->status);
    char* col_cnt = next_token_comma(&query_command, &send_message->status);
    if (send_message->status == INCORRECT_FORMAT) {
        return NULL;
    }

    table_name = trim_quote(table_name);
    int last_char = (int)strlen(col_cnt) - 1;
    if (col_cnt[last_char] != ')') {
        return NULL;
    }
    col_cnt[last_char] = '\0';
    size_t column_cnt = atoi(col_cnt);
    if (column_cnt < 1) {
        return NULL;
    }
    dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE_TBL;
    dbo->operator_fields.create_table_operator.db_name = malloc((strlen(db_name)+1)* sizeof(char));
    strcpy(dbo->operator_fields.create_table_operator.db_name,db_name);
    dbo->operator_fields.create_table_operator.table_name = malloc((strlen(table_name)+strlen(db_name)+2)* sizeof(char));
    strcpy(dbo->operator_fields.create_table_operator.table_name, db_name);
    strcat(dbo->operator_fields.create_table_operator.table_name, ".");
    strcat(dbo->operator_fields.create_table_operator.table_name, table_name);
    dbo->operator_fields.create_table_operator.col_count = column_cnt;
    return dbo;
}

/**
 * This method takes in a string representing the arguments to create a column.
 * It parses those arguments, checks that they are valid, and creates a column.
 **/
DbOperator* parse_create_col(char* query_command, message* send_message) {
    DbOperator* dbo;
    char** create_arguments_index = &query_command;
    char* col_name = next_token_comma(create_arguments_index, &send_message->status);
    col_name = trim_quote(col_name);
    char* full_tbl_name = next_token_comma(create_arguments_index,&send_message->status);
    if (send_message->status == INCORRECT_FORMAT) {
        return NULL;
    }
    int last_char = (int)strlen(full_tbl_name) - 1;
    if (full_tbl_name[last_char] != ')') {
        return NULL;
    }
    full_tbl_name[last_char] = '\0';
    dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE_COL;
    dbo->operator_fields.create_col_operator.tbl_name = malloc((strlen(full_tbl_name)+1)* sizeof(char));
    strcpy(dbo->operator_fields.create_col_operator.tbl_name, full_tbl_name);
    dbo->operator_fields.create_col_operator.col_name = malloc((strlen(col_name)+strlen(full_tbl_name)+2)* sizeof(char));
    strcpy(dbo->operator_fields.create_col_operator.col_name, full_tbl_name);
    strcat(dbo->operator_fields.create_col_operator.col_name, ".");
    strcat(dbo->operator_fields.create_col_operator.col_name, col_name);
    return dbo;
}

/**
 * This method takes in a string representing the arguments to create a index for a column.
 * It parses those arguments, checks that they are valid, and creates the index.
 **/
DbOperator* parse_create_idx(char* query_command, message* send_message) {
    DbOperator* dbo;
    char* col_name = next_token_comma(&query_command, &send_message->status);
    char* col_idx = next_token_comma(&query_command, &send_message->status);
    char* is_cluster = next_token_comma(&query_command, &send_message->status);
    if(send_message->status == INCORRECT_FORMAT) {
        return NULL;
    }
    int last_char = (int)strlen(is_cluster) - 1;
    if (is_cluster[last_char] != ')') {
        return NULL;
    }
    is_cluster[last_char] = '\0';
    dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE_IDX;
    dbo->operator_fields.create_idx_operator.idx_col_name = malloc((strlen(col_name)+1)* sizeof(char));
    strcpy(dbo->operator_fields.create_idx_operator.idx_col_name,col_name);

    if(strcmp(col_idx, "btree") == 0) {
        dbo->operator_fields.create_idx_operator.col_idx = BTREE;
    }
    else if (strcmp(col_idx, "sorted") == 0) {
        dbo->operator_fields.create_idx_operator.col_idx = SORTED;
    }
    else {
        dbo->operator_fields.create_idx_operator.col_idx = UNIDX;
    }
    if(strcmp(is_cluster, "clustered") == 0) {
        dbo->operator_fields.create_idx_operator.is_cluster = true;
    }
    else {
        dbo->operator_fields.create_idx_operator.is_cluster = false;
    }
    return dbo;
}

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
 * parse_load reads in the arguments for a load statement and
 * then passes these arguments to a database function to load the existing csv file.
 **/
DbOperator* parse_load(char* query_command) {
    DbOperator* dbo;
    char* data_path = trim_quote(query_command);
    int last_char = (int)strlen(data_path) - 1;
    if (last_char < 0 || data_path[last_char] != ')') {
        return NULL;
    }
    data_path[last_char] = '\0';
    dbo = malloc(sizeof(DbOperator));
    dbo->type = LOAD;
    dbo->operator_fields.load_operator.data_path = malloc((strlen(data_path)+1)*sizeof(char));
    strcpy(dbo->operator_fields.load_operator.data_path,data_path);
    return dbo;
}

/**
 * parse select command
 **/
DbOperator* parse_select(char* query_command, char* handle, message* send_message) {
    char* select_name = next_token_comma(&query_command,&send_message->status);
    if(has_period(select_name)) {
        char* pre_range = next_token_comma(&query_command,&send_message->status);
        char* post_range = next_token_comma(&query_command,&send_message->status);
        if(send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }
        int last_char = (int)strlen(post_range) - 1;
        if (last_char < 0 || post_range[last_char] != ')') {
            return NULL;
        }
        post_range[last_char] = '\0';
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = SELECT;
        dbo->operator_fields.select_operator.selectType = HANDLE_COL;
        dbo->operator_fields.select_operator.handle = malloc((strlen(handle)+1)* sizeof(char));
        strcpy(dbo->operator_fields.select_operator.handle,handle);
        dbo->operator_fields.select_operator.pre_range = malloc((strlen(pre_range)+1)* sizeof(char));
        strcpy(dbo->operator_fields.select_operator.pre_range,pre_range);
        dbo->operator_fields.select_operator.post_range = malloc((strlen(post_range)+1)* sizeof(char));
        strcpy(dbo->operator_fields.select_operator.post_range,post_range);
        dbo->operator_fields.select_operator.select_col = malloc((strlen(select_name)+1)* sizeof(char));
        strcpy(dbo->operator_fields.select_operator.select_col,select_name);
        return dbo;
    }
    else {
        char* select_val = next_token_comma(&query_command,&send_message->status);
        char* pre_range = next_token_comma(&query_command,&send_message->status);
        char* post_range = next_token_comma(&query_command,&send_message->status);
        if(send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }
        int last_char = (int)strlen(post_range) - 1;
        if (last_char < 0 || post_range[last_char] != ')') {
            return NULL;
        }
        post_range[last_char] = '\0';
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = SELECT;
        dbo->operator_fields.select_operator.selectType = HANDLE_RSL;
        dbo->operator_fields.select_operator.handle = malloc((strlen(handle)+1)* sizeof(char));
        strcpy(dbo->operator_fields.select_operator.handle,handle);
        dbo->operator_fields.select_operator.pre_range = malloc((strlen(pre_range)+1)* sizeof(char));
        strcpy(dbo->operator_fields.select_operator.pre_range,pre_range);
        dbo->operator_fields.select_operator.post_range = malloc((strlen(post_range)+1)* sizeof(char));
        strcpy(dbo->operator_fields.select_operator.post_range,post_range);
        dbo->operator_fields.select_operator.select_rsl_pos = malloc((strlen(select_name)+1)* sizeof(char));
        strcpy(dbo->operator_fields.select_operator.select_rsl_pos,select_name);
        dbo->operator_fields.select_operator.select_rsl_val = malloc((strlen(select_val)+1)* sizeof(char));
        strcpy(dbo->operator_fields.select_operator.select_rsl_val,select_val);
        return dbo;
    }
}

/**
 * parse fetch command
 **/
DbOperator* parse_fetch(char* query_command, char* handle, message* send_message) {
    char* col_var_name = next_token_comma(&query_command, &send_message->status);
    char* rsl_vec_pos = next_token_comma(&query_command, &send_message->status);
    if (send_message->status == INCORRECT_FORMAT) {
        return NULL;
    }
    int last_char = (int)strlen(rsl_vec_pos) - 1;
    if (last_char < 0 || rsl_vec_pos[last_char] != ')') {
        return NULL;
    }
    rsl_vec_pos[last_char] = '\0';
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = FETCH;
    dbo->operator_fields.fetch_operator.col_var_name = malloc((strlen(col_var_name)+1)* sizeof(char));
    strcpy(dbo->operator_fields.fetch_operator.col_var_name, col_var_name);
    dbo->operator_fields.fetch_operator.rsl_vec_pos = malloc((strlen(rsl_vec_pos)+1)* sizeof(char));
    strcpy(dbo->operator_fields.fetch_operator.rsl_vec_pos, rsl_vec_pos);
    dbo->operator_fields.fetch_operator.handle = malloc((strlen(handle)+1)* sizeof(char));
    strcpy(dbo->operator_fields.fetch_operator.handle, handle);
    return dbo;
}

/**
 * parse print command
 **/
DbOperator* parse_print(char* query_command, message* send_message) {
    char* cmd_copy = malloc((strlen(query_command)+1)* sizeof(char));
    strcpy(cmd_copy,query_command);
    size_t count = 0;
    while(1) {
        char* tmp = strsep(&cmd_copy, ",");
        if(tmp != NULL) {
            count++;
        }
        else {
            break;
        }
    }
    int last_char = (int)strlen(query_command) - 1;
    if (last_char < 0 || query_command[last_char] != ')') {
        return NULL;
    }
    query_command[last_char] = '\0';
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = PRINT;
    dbo->operator_fields.print_operator.print_num = count;
    dbo->operator_fields.print_operator.print_name = calloc(count, sizeof(char*));
    for(size_t i = 0; i < count; ++i) {
        char* pitem = next_token_comma(&query_command, &send_message->status);
        if(send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }
        dbo->operator_fields.print_operator.print_name[i] = malloc((strlen(pitem)+1)* sizeof(char));
        strcpy(dbo->operator_fields.print_operator.print_name[i], pitem);
    }
    return dbo;
}

/**
 * parse avg command
 **/
DbOperator* parse_avg(char* handle, char* query_command) {
    DbOperator* dbo = malloc(sizeof(DbOperator));
    if(has_period(query_command)) {
        dbo->operator_fields.avg_operator.handle_type = HANDLE_COL;
    }
    else {
        dbo->operator_fields.avg_operator.handle_type = HANDLE_RSL;
    }
    int last_char = (int)strlen(query_command) - 1;
    if (last_char < 0 || query_command[last_char] != ')') {
        return NULL;
    }
    query_command[last_char] = '\0';
    dbo->type = AVG;
    dbo->operator_fields.avg_operator.avg_name = malloc((strlen(query_command)+1)* sizeof(char));
    strcpy(dbo->operator_fields.avg_operator.avg_name,query_command);
    dbo->operator_fields.avg_operator.handle = malloc((strlen(handle)+1)* sizeof(char));
    strcpy(dbo->operator_fields.avg_operator.handle, handle);
    return dbo;
}

/**
 * parse sum command
 **/
DbOperator* parse_sum(char* handle, char* query_command) {
    DbOperator* dbo = malloc(sizeof(DbOperator));
    if(has_period(query_command)) {
        dbo->operator_fields.sum_operator.handle_type = HANDLE_COL;
    }
    else {
        dbo->operator_fields.sum_operator.handle_type = HANDLE_RSL;
    }
    int last_char = (int)strlen(query_command) - 1;
    if (last_char < 0 || query_command[last_char] != ')') {
        return NULL;
    }
    query_command[last_char] = '\0';
    dbo->type = SUM;
    dbo->operator_fields.sum_operator.sum_name = malloc((strlen(query_command)+1)* sizeof(char));
    strcpy(dbo->operator_fields.sum_operator.sum_name,query_command);
    dbo->operator_fields.sum_operator.handle = malloc((strlen(handle)+1)* sizeof(char));
    strcpy(dbo->operator_fields.sum_operator.handle, handle);
    return dbo;
}

/**
 * parse add command
 **/
DbOperator* parse_add(char* handle, char* query_command, message* send_message) {
    char* vec_val1 = next_token_comma(&query_command, &send_message->status);
    char* vec_val2 = next_token_comma(&query_command, &send_message->status);
    if (send_message->status == INCORRECT_FORMAT) {
        return NULL;
    }
    int last_char = (int)strlen(vec_val2) - 1;
    if (last_char < 0 || vec_val2[last_char] != ')') {
        return NULL;
    }
    vec_val2[last_char] = '\0';
    DbOperator* dbo = malloc(sizeof(DbOperator));

    if(has_period(vec_val1)) {
        dbo->operator_fields.add_operator.add_type1 = HANDLE_COL;
    }
    else {
        dbo->operator_fields.add_operator.add_type1 = HANDLE_RSL;
    }

    if(has_period(vec_val2)) {
        dbo->operator_fields.add_operator.add_type2 = HANDLE_COL;
    }
    else {
        dbo->operator_fields.add_operator.add_type2 = HANDLE_RSL;
    }
    dbo->type = ADD;
    dbo->operator_fields.add_operator.add_name1 = malloc((strlen(vec_val1)+1)* sizeof(char));
    strcpy(dbo->operator_fields.add_operator.add_name1,vec_val1);
    dbo->operator_fields.add_operator.add_name2 = malloc((strlen(vec_val2)+1)* sizeof(char));
    strcpy(dbo->operator_fields.add_operator.add_name2,vec_val2);
    dbo->operator_fields.add_operator.handle = malloc((strlen(handle)+1)* sizeof(char));
    strcpy(dbo->operator_fields.add_operator.handle, handle);
    return dbo;
}

/**
 * parse sub command
 **/
DbOperator* parse_sub(char* handle, char* query_command, message* send_message) {
    char* vec_val1 = next_token_comma(&query_command, &send_message->status);
    char* vec_val2 = next_token_comma(&query_command, &send_message->status);
    if (send_message->status == INCORRECT_FORMAT) {
        return NULL;
    }
    int last_char = (int)strlen(vec_val2) - 1;
    if (last_char < 0 || vec_val2[last_char] != ')') {
        return NULL;
    }
    vec_val2[last_char] = '\0';
    DbOperator* dbo = malloc(sizeof(DbOperator));

    if(has_period(vec_val1)) {
        dbo->operator_fields.sub_operator.sub_type1 = HANDLE_COL;
    }
    else {
        dbo->operator_fields.sub_operator.sub_type1 = HANDLE_RSL;
    }

    if(has_period(vec_val2)) {
        dbo->operator_fields.sub_operator.sub_type2 = HANDLE_COL;
    }
    else {
        dbo->operator_fields.sub_operator.sub_type2 = HANDLE_RSL;
    }
    dbo->type = SUB;
    dbo->operator_fields.sub_operator.sub_name1 = malloc((strlen(vec_val1)+1)* sizeof(char));
    strcpy(dbo->operator_fields.sub_operator.sub_name1,vec_val1);
    dbo->operator_fields.sub_operator.sub_name2 = malloc((strlen(vec_val2)+1)* sizeof(char));
    strcpy(dbo->operator_fields.sub_operator.sub_name2,vec_val2);
    dbo->operator_fields.sub_operator.handle = malloc((strlen(handle)+1)* sizeof(char));
    strcpy(dbo->operator_fields.sub_operator.handle, handle);
    return dbo;
}

/**
 * parse max command
 **/
DbOperator* parse_max(char* handle, char* query_command, message* send_message) {
    if(has_comma(handle)) {
        char* max_handle_pos = next_token_comma(&handle, &send_message->status);
        char* max_handle_value = next_token_comma(&handle, &send_message->status);
        char* max_vec_pos = next_token_comma(&query_command, &send_message->status);
        char* max_vec_value = next_token_comma(&query_command, &send_message->status);
        int last_char = (int)strlen(max_vec_value) - 1;
        if (last_char < 0 || max_vec_value[last_char] != ')') {
            return NULL;
        }
        max_vec_value[last_char] = '\0';
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = MAX;
        dbo->operator_fields.max_operator.max_type = MAX_POS_VALUE;
        dbo->operator_fields.max_operator.handle_value = malloc((strlen(max_handle_value)+1)* sizeof(char));
        strcpy(dbo->operator_fields.max_operator.handle_value,max_handle_value);
        dbo->operator_fields.max_operator.handle_pos = malloc((strlen(max_handle_pos)+1)* sizeof(char));
        strcpy(dbo->operator_fields.max_operator.handle_pos,max_handle_pos);
        dbo->operator_fields.max_operator.max_vec_pos = malloc((strlen(max_vec_pos)+1)* sizeof(char));
        strcpy(dbo->operator_fields.max_operator.max_vec_pos,max_vec_pos);
        dbo->operator_fields.max_operator.max_vec_value = malloc((strlen(max_vec_value)+1)* sizeof(char));
        strcpy(dbo->operator_fields.max_operator.max_vec_value,max_vec_value);
        return dbo;
    }
    else {
        int last_char = (int)strlen(query_command) - 1;
        if (last_char < 0 || query_command[last_char] != ')') {
            return NULL;
        }
        query_command[last_char] = '\0';
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = MAX;
        dbo->operator_fields.max_operator.max_type = MAX_VALUE;
        dbo->operator_fields.max_operator.handle_value = malloc((strlen(handle)+1)* sizeof(char));
        strcpy(dbo->operator_fields.max_operator.handle_value,handle);
        dbo->operator_fields.max_operator.max_vec_value = malloc((strlen(query_command)+1)* sizeof(char));
        strcpy(dbo->operator_fields.max_operator.max_vec_value,query_command);
        return dbo;
    }
}

/**
 * parse min command
 **/
DbOperator* parse_min(char* handle, char* query_command, message* send_message) {
    if(has_comma(handle)) {

    }
    else {
        int last_char = (int)strlen(query_command) - 1;
        if (last_char < 0 || query_command[last_char] != ')') {
            return NULL;
        }
        query_command[last_char] = '\0';
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = MIN;
        dbo->operator_fields.min_operator.min_type = MIN_VALUE;
        dbo->operator_fields.min_operator.handle_value = malloc((strlen(handle)+1)* sizeof(char));
        strcpy(dbo->operator_fields.min_operator.handle_value,handle);
        dbo->operator_fields.min_operator.min_vec_value = malloc((strlen(query_command)+1)* sizeof(char));
        strcpy(dbo->operator_fields.min_operator.min_vec_value,query_command);
        return dbo;
    }
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
    else if (strncmp(query_command, "create(tbl,", 11) == 0) {
        query_command += 11;
        dbo = parse_create_tbl(query_command, send_message);
    }
    else if(strncmp(query_command, "create(col,", 11) == 0) {
        query_command += 11;
        dbo = parse_create_col(query_command, send_message);
    }
    else if(strncmp(query_command, "create(idx,", 11) == 0) {
        query_command += 11;
        dbo = parse_create_idx(query_command, send_message);
    }
    else if (strncmp(query_command, "load(", 5) == 0) {
        query_command += 5;
        dbo = parse_load(query_command);
    }
    else if (strncmp(query_command, "relational_insert(", 18) == 0) {
        query_command += 18;
        dbo = parse_insert(query_command, send_message);
    }
    else if (strncmp(query_command, "select(", 7) == 0) {
        query_command += 7;
        dbo = parse_select(query_command, handle, send_message);
    }
    else if (strncmp(query_command, "fetch(", 6) == 0) {
        query_command += 6;
        dbo = parse_fetch(query_command, handle, send_message);
    }
    else if (strncmp(query_command, "print(", 6) == 0) {
        query_command += 6;
        dbo = parse_print(query_command, send_message);
    }
    else if (strncmp(query_command, "avg(", 4) == 0) {
        query_command += 4;
        dbo = parse_avg(handle, query_command);
    }
    else if (strncmp(query_command, "sum(", 4) == 0) {
        query_command += 4;
        dbo = parse_sum(handle, query_command);
    }
    else if (strncmp(query_command, "add(", 4) == 0) {
        query_command += 4;
        dbo = parse_add(handle, query_command, send_message);
    }
    else if (strncmp(query_command, "sub(", 4) == 0) {
        query_command += 4;
        dbo = parse_sub(handle, query_command, send_message);
    }
    else if (strncmp(query_command, "max(", 4) == 0) {
        query_command += 4;
        dbo = parse_max(handle, query_command, send_message);
    }
    else if (strncmp(query_command, "min(", 4) == 0) {
        query_command += 4;
        dbo = parse_min(handle, query_command, send_message);
    }
    else if (strncmp(query_command, "batch_queries()", 13) == 0) {
        dbo = malloc(sizeof(DbOperator));
        dbo->type = BATCH_QUERY;
    }
    else if (strncmp(query_command, "batch_execute()", 15) == 0) {
        dbo = malloc(sizeof(DbOperator));
        dbo->type = BATCH_EXEC;
    }
    else if (strncmp(query_command, "shutdown", 8) == 0) {
        dbo = malloc(sizeof(DbOperator));
        dbo->type = SHUTDOWN;
    }
    else {
        return NULL;
    }
    
    dbo->client_fd = client_socket;
    dbo->context = context;
    return dbo;
}
