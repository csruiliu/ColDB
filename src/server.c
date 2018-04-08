/** server.c
 * CS165 Fall 2015
 *
 * This file provides a basic unix socket implementation for a server
 * used in an interactive client-server database.
 * The client should be able to send messages containing queries to the
 * server.  When the server receives a message, it must:
 * 1. Respond with a status based on the query (OK, UNKNOWN_QUERY, etc.)
 * 2. Process any appropriate queries, if applicable.
 * 3. Return the query response to the client.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <unistd.h>
#include <libexplain/bind.h>

#include "db_manager.h"
#include "common.h"
#include "utils.h"

#define DEFAULT_QUERY_BUFFER_SIZE 1024

void free_query(DbOperator* query) {
    if (query->type == CREATE_TBL) {
        free(query->operator_fields.create_tbl_operator.db_name);
        free(query->operator_fields.create_tbl_operator.tbl_name);
    }
    else if (query->type == CREATE_COL) {
        free(query->operator_fields.create_col_operator.col_name);
        free(query->operator_fields.create_col_operator.tbl_name);
    }
    else if (query->type == LOAD) {
        free(query->operator_fields.load_operator.data_path);
    }
    else if (query->type == CREATE_DB){
        free(query->operator_fields.create_db_operator.db_name);
    }
    free(query);
}

/** execute_DbOperator takes as input the DbOperator and executes the query.
 * This should be replaced in your implementation (and its implementation possibly moved to a different file).
 * It is currently here so that you can verify that your server and client can send messages.
 **/
char* execute_DbOperator(DbOperator* query) {
    if (query == NULL) {
        return "unsupported command, try again.\n";
    }
    else if (query->type == CREATE_DB) {
        char* db_name = query->operator_fields.create_db_operator.db_name;
        current_db = create_db(db_name);
        if(current_db == NULL) {
            free_query(query);
            log_err("[server.c:execute_DbOperator()] create database failed.\n");
            return "create database failed.\n";
        }
        free_query(query);
        log_info("create database successfully.\n");
        return "create database successfully.\n";
    }
    else if (query->type == CREATE_TBL) {
        char* db_name = query->operator_fields.create_tbl_operator.db_name;
        char* tbl_name = query->operator_fields.create_tbl_operator.tbl_name;
        size_t col_count = query->operator_fields.create_tbl_operator.col_count;
        Table* tbl = create_table(db_name, tbl_name, col_count);
        if(tbl == NULL) {
            free_query(query);
            log_err("[server.c:execute_DbOperator()] create table failed.\n");
            return "create table failed.\n";
        }
        free_query(query);
        log_info("create table successfully.\n");
        return "create table successfully.\n";
    }
    else if (query->type == CREATE_COL){
        char* full_tbl_name = query->operator_fields.create_col_operator.tbl_name;
        char* full_col_name = query->operator_fields.create_col_operator.col_name;
        Column* col = create_column(full_tbl_name, full_col_name);
        if(col == NULL) {
            free_query(query);
            log_err("[server.c:execute_DbOperator()] create column failed.\n");
            return "create column failed.\n";
        }
        free_query(query);
        log_info("create column successfully.\n");
        return "create column successfully.\n";
    }
    else if (query->type == LOAD) {
        char* data_path = query->operator_fields.load_operator.data_path;
        if(load_data_csv(data_path) != 0) {
            free_query(query);
            log_err("[server.c:execute_DbOperator()] load data into database failed.\n");
            return "load data into database failed.\n";
        }
        free_query(query);
        log_info("load data into database successfully.\n");
        return "load data into database successfully.\n";
    }
    else if (query->type == INSERT) {
        Table* insert_tbl = query->operator_fields.insert_operator.insert_tbl;
        if(insert_data_tbl(insert_tbl,query->operator_fields.insert_operator.values)) {
            free_query(query);
            log_err("[server.c:execute_DbOperator()] insert data into database failed.");
            return "insert data into database failed.\n";
        }
        free_query(query);
        log_info("insert data into database successfully.\n");
        return "insert data into database successfully.\n";
    }
    else if (query->type == SELECT) {
        if(query->operator_fields.select_operator.selectType == HANDLE_COL) {
            char* select_col_name = query->operator_fields.select_operator.select_col;
            char* handle = query->operator_fields.select_operator.handle;
            char* pre_range = query->operator_fields.select_operator.pre_range;
            char* post_range = query->operator_fields.select_operator.post_range;
            Column* scol = get_col(select_col_name);
            if(scol == NULL) {
                log_err("[server.c:execute_DbOperator()] column didn't exist in the database.\n");
                return "column didn't exist in the database.\n";
            }
            if (scol->idx_type == UNIDX) {
                if(select_data_col_unidx(scol, handle, pre_range, post_range) != 0) {
                    free_query(query);
                    log_err("[server.c:execute_DbOperator()] select data from column in database failed.\n");
                    return "select data from column in database failed.\n";
                }
            }
            free_query(query);
            log_info("select data from column in database successfully.\n");
            return "select data from column in database successfully.\n";
        }
        else {
            char* select_rsl_pos = query->operator_fields.select_operator.select_rsl_pos;
            char* select_rsl_val = query->operator_fields.select_operator.select_rsl_val;
            char* handle = query->operator_fields.select_operator.handle;
            char* pre_range = query->operator_fields.select_operator.pre_range;
            char* post_range = query->operator_fields.select_operator.post_range;
            Result* srsl_pos = get_rsl(select_rsl_pos);
            Result* srsl_val = get_rsl(select_rsl_val);
            if(select_data_rsl(srsl_pos, srsl_val, handle, pre_range, post_range) != 0) {
                free_query(query);
                log_err("[server.c:execute_DbOperator()] select data from result in database failed.\n");
                return "select data from result in database failed.\n";
            }
            free_query(query);
            log_info("select data from result in database successfully.\n");
            return "select data from result in database successfully.\n";
        }
    }
    else if (query->type == FETCH) {
        char* col_val_name = query->operator_fields.fetch_operator.col_var_name;
        char* rsl_vec_pos = query->operator_fields.fetch_operator.rsl_vec_pos;
        char* handle = query->operator_fields.fetch_operator.handle;
        if(fetch_col_data(col_val_name,rsl_vec_pos,handle) != 0) {
            free_query(query);
            return "fetch data failed.\n";
        }
        free_query(query);
        log_info("[server.c:execute_DbOperator()] fetch data successfully.\n");
        return "fetch data successfully.\n";
    }
    else if (query->type == AVG) {
        char* avg_name = query->operator_fields.avg_operator.avg_name;
        char* handle = query->operator_fields.avg_operator.handle;
        if (query->operator_fields.avg_operator.handle_type == HANDLE_COL) {
            if (avg_col_data(avg_name,handle) != 0) {
                free_query(query);
                return "get ave of data failed.\n";
            }
        }
        else {
            if (avg_rsl_data(avg_name,handle) != 0) {
                free_query(query);
                return "get ave of data failed.\n";
            }
        }
        return "calculate ave of data successfully.\n";
    }
    else if (query->type == PRINT) {
        size_t print_num = query->operator_fields.print_operator.print_num;
        char** print_name = query->operator_fields.print_operator.print_name;
        char* print_result = generate_print_result(print_num, print_name);
        if (print_result == NULL){
            free_query(query);
            log_err("[server.c:execute_DbOperator()] fetch data failed.\n");
            return "fetch data failed.\n";
        }
        return print_result;
    }
    else if (query->type == SHUTDOWN) {
        if(persist_data_csv() != 0) {
            log_err("persist all the data failed.\n");
        }
        free_db_store();
        free_tbl_store();
        free_col_store();
        free_rsl_store();
        log_info("persist all the data and shutdown the server.\n");
        return "persist all the data and shutdown the server.\n";
    }
    else {
        free(query);
        log_info("unsupported command, try again.\n");
        return "unsupported command, try again.\n";
    }
}

/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
void handle_client(int client_socket) {
    int done = 0;
    int length = 0;

    log_info("Connected to socket: %d.\n", client_socket);

    // Create two messages, one from which to read and one from which to receive
    message send_message;
    message recv_message;

    // create the client context here
    ClientContext* client_context = NULL;

    init_db_store(100000);
    init_tbl_store(500000);
    init_col_store(2500000);
    init_rls_store(2500000);
    //init_idx_store(2500000);

    if(setup_db_csv() != 0) {
        free_db_store();
        free_tbl_store();
        free_col_store();
        free_rsl_store();
        log_err("setup db from csv data failed, exit.\n");
        log_info("Connection closed at socket %d!\n", client_socket);
        close(client_socket);
        exit(1);
    }


    // Continually receive messages from client and execute queries.
    // 1. Parse the command
    // 2. Handle request if appropriate
    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    // 4. Send response of request.
    do {
        length = recv(client_socket, &recv_message, sizeof(message), 0);
        if (length < 0) {
            log_err("Client connection closed!\n");
            exit(1);
        } else if (length == 0) {
            done = 1;
        }

        if (!done) {
            char recv_buffer[recv_message.length + 1];
            length = recv(client_socket, recv_buffer, recv_message.length,0);
            recv_message.payload = recv_buffer;
            recv_message.payload[recv_message.length] = '\0';


            // 1. Parse command
            DbOperator* query = parse_command(recv_message.payload, &send_message, client_socket, client_context);

            // 2. Handle request
            char* result = execute_DbOperator(query);
            if(strncmp(recv_message.payload,"shutdown",8) == 0) {
                break;
            }
            send_message.length = strlen(result);
            char send_buffer[send_message.length + 1];
            strcpy(send_buffer, result);
            send_message.payload = send_buffer;

            // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                log_err("Failed to send message.");
                exit(1);
            }

            // 4. Send response of request
            if (send(client_socket, result, send_message.length, 0) == -1) {
                log_err("Failed to send message.");
                exit(1);
            }
        }
    } while (!done);

    log_info("Connection closed at socket %d!\n", client_socket);
    close(client_socket);
}

/**
 * setup_server()
 *
 * This sets up the connection on the server side using unix sockets.
 * Returns a valid server socket fd on success, else -1 on failure.
 **/
int setup_server() {
    int server_socket;
    size_t len;
    struct sockaddr_un local;

    log_info("Attempting to setup server...\n");

    if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    local.sun_family = AF_UNIX;
    strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    unlink(local.sun_path);

    /*
    int on = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
    {
        log_err("L%d: Failed to set socket as reusable.\n", __LINE__);
        return -1;
    }
    */

    len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
    if (bind(server_socket, (struct sockaddr *)&local, len) == -1) {
        int err = errno;
        log_err("L%d: Socket failed to bind.\n", __LINE__);
        fprintf(stderr, "%s\n", explain_errno_bind(err, server_socket,(struct sockaddr *)&local, len));
        return -1;
    }

    if (listen(server_socket, 5) == -1) {
        log_err("L%d: Failed to listen on socket.\n", __LINE__);
        return -1;
    }

    return server_socket;
}

// Currently this main will setup the socket and accept a single client.
// After handling the client, it will exit.
// You will need to extend this to handle multiple concurrent clients
// and remain running until it receives a shut-down command.
int main(void)
{
    int server_socket = setup_server();
    if (server_socket < 0) {
        exit(1);
    }

    log_info("Waiting for a connection %d ...\n", server_socket);

    struct sockaddr_un remote;
    socklen_t t = sizeof(remote);
    int client_socket = 0;

    if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1) {
        log_err("L%d: Failed to accept a new connection.\n", __LINE__);
        exit(1);
    }

    handle_client(client_socket);

    return 0;
}
