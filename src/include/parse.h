#ifndef PARSE_H
#define PARSE_H

#include "message.h"
#include "operator.h"

DbOperator* parse_command(char* query_command, message* send_message, int client, ClientContext* context);

#endif