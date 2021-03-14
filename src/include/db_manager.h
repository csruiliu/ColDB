#ifndef DB_MANAGER_H
#define DB_MANAGER_H

extern Db *current_db;

Db* create_db(char* db_name);

#endif //DB_MANAGER_H