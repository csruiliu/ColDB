// utils.h
//
// Provides utility and helper functions that may be useful throughout.
// Includes debugging tools.

#ifndef UTILS_FUNC_H
#define UTILS_FUNC_H

#include <stdarg.h>
#include <stdio.h>

/**
 * a hash function for kv store
 **/
int hash_func(const char* s, size_t a, size_t m);

/**
 * trims newline characters from a string (in place)
 **/
char* trim_newline(char* str);

/**
 * trims parenthesis characters from a string (in place)
 **/
char* trim_parenthesis(char* str);

/**
 * trims whitespace characters from a string (in place)
 **/
char* trim_whitespace(char* str);

/**
 * trims quotations characters from a string (in place)
 **/
char* trim_quote(char* str);

/**
 * coldb_log(out, format, ...)
 * Writes the string from @format to the @out pointer, extendable for
 * additional parameters.
 * Usage: coldb_log(stderr, "%s: error at line: %d", __func__, __LINE__);
 **/
void coldb_log(FILE* out, const char *format, ...);

/**
 * log_err(format, ...)
 * Writes the string from @format to stderr, extendable for
 * additional parameters. Like coldb_log, but specifically to stderr.
 * Usage: log_err("%s: error at line: %d", __func__, __LINE__);
 **/
void log_err(const char* format, ...);

/**
 * log_info(format, ...)
 * Writes the string from @format to stdout, extendable for
 * additional parameters. Like coldb_log, but specifically to stdout.
 * Only use this when appropriate (e.g., denoting a specific checkpoint),
 * else defer to using printf.
 * Usage: log_info("Command received: %s", command_string);
 */
void log_info(const char* format, ...);

#endif /* __UTILS_H__ */
