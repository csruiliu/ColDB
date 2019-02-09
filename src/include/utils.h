// utils.h
// CS165 Fall 2015
//
// Provides utility and helper functions that may be useful throughout.
// Includes debugging tools.

#ifndef __UTILS_H__
#define __UTILS_H__

#include <stdarg.h>
#include <stdio.h>
#include <stdbool.h>
#include "message.h"


bool has_comma(char *str);

bool has_period(char *str);

char* next_token_comma(char **tokenizer, message_status *status);

char* next_token_period(char **tokenizer, message_status *status);

bool is_csv(char* filename);

int hash_func(const char* s, const int a, const int m);

/**
 * parse a table name from a full column name
 **/

char* parse_tbl_name(char* fullColName);

/**
 * trims newline characters from a string (in place)
 **/

char* trim_newline(char *str);

/**
 * trims parenthesis characters from a string (in place)
 **/

char* trim_parenthesis(char *str);

/**
 * trims whitespace characters from a string (in place)
 **/

char* trim_whitespace(char *str);

/**
 * trims quotations characters from a string (in place)
 **/

char* trim_quotes(char *str);

// cs165_log(out, format, ...)
// Writes the string from @format to the @out pointer, extendable for
// additional parameters.
//
// Usage: cs165_log(stderr, "%s: error at line: %d", __func__, __LINE__);
void cs165_log(FILE* out, const char *format, ...);

// log_err(format, ...)
// Writes the string from @format to stderr, extendable for
// additional parameters. Like cs165_log, but specifically to stderr.
//
// Usage: log_err("%s: error at line: %d", __func__, __LINE__);
void log_err(const char *format, ...);

// log_info(format, ...)
// Writes the string from @format to stdout, extendable for
// additional parameters. Like cs165_log, but specifically to stdout.
// Only use this when appropriate (e.g., denoting a specific checkpoint),
// else defer to using printf.
//
// Usage: log_info("Command received: %s", command_string);
void log_info(const char *format, ...);

#endif /* __UTILS_H__ */
