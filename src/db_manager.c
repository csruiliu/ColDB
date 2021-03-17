#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <assert.h>
#include <memory.h>
#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>

#include "message.h"
#include "db_element.h"
#include "utils_func.h"

#define RESIZE 2
#define DIRLEN 256

// In this class, there will always be only one active database at a time
Db* current_db;

void* resize(void* data, size_t oc, size_t nc) {
    assert(oc <= nc);
    void* ndata = calloc(nc, sizeof(char));
    memcpy(ndata, data, oc);
    return ndata;
}

int* resize_int(int* data, size_t oc, size_t nc) {
    assert(oc <= nc);
    int* ndata = calloc(nc, sizeof(int));
    memcpy(ndata, data, oc * sizeof(int));
    return ndata;
}

Db* create_db(char* db_name) {
    Db* db = get_db(db_name);
    if(db != NULL) {
        log_info("database %s exists, open database %s\n", db_name, db_name);
        return current_db;
    }
    db = malloc(sizeof(Db));
    db->name = malloc((strlen(db_name)+1)* sizeof(char));
    strcpy(db->name, db_name);
    db->capacity = 0;
    db->size = 0;
    db->tables = NULL;

    put_db(db_name,db);
    log_info("create db %s successfully.\n",db_name);
    return db;
}

Table* create_table(char* db_name, char* table_name, char* pricls_col_name, size_t num_columns) {
    if(strcmp(current_db->name, db_name) != 0) {
        log_err("the current active database is not database %s\n", db_name);
        return NULL;
    }
    Table* tbl = get_table(table_name);
    if(tbl != NULL) {
        log_info("the table %s exists.\n", table_name);
        return tbl;
    }
    else {
        if(current_db->capacity - current_db->size <= 0) {
            log_info("no enough table space in database %s, creating more table space.\n", current_db->name);
            size_t more_table_capacity = RESIZE * current_db->capacity + 1;
            size_t new_capacity = sizeof(Table*) * more_table_capacity;
            size_t old_capacity = sizeof(Table*) * current_db->capacity;
            if(old_capacity == 0) {
                assert(new_capacity>0);
                current_db->tables = calloc(more_table_capacity, sizeof(Table*));
            }
            else {
                void* t = resize(current_db->tables, old_capacity, new_capacity);
                free(current_db->tables);
                current_db->tables = calloc(more_table_capacity, sizeof(Table*));
                memcpy(current_db->tables, t, new_capacity*sizeof(char));
                if(current_db->tables == NULL){
                    log_err("create more table space in database failed.\n", current_db->name);
                    free(tbl);
                    return NULL;
                }
            }
            current_db->capacity = more_table_capacity;
        }
        tbl = malloc(sizeof(Table));
        tbl->name = malloc((strlen(table_name)+1)*sizeof(char));
        strcpy(tbl->name, table_name);
        tbl->aff_db_name = malloc((strlen(db_name)+1)*sizeof(char));
        strcpy(tbl->aff_db_name, db_name);
        tbl->capacity = num_columns;
        tbl->size = 0;
        tbl->pricluster_index_col_name = malloc((strlen(pricls_col_name)+1)* sizeof(char));
        strcpy(tbl->pricluster_index_col_name, pricls_col_name);
        if(strcmp(pricls_col_name,"NULL") == 0) {
            tbl->has_cluster_index = 0;
        }
        else {
            tbl->has_cluster_index = 1;
        }
        tbl->columns = calloc(num_columns, sizeof(Column*));
        put_table(table_name, tbl);
        current_db->tables[current_db->size] = tbl;
        current_db->size++;
        return tbl;
    }
}

Column* create_column(char* tbl_name, char* col_name) {
    Table* cur_tbl = get_table(tbl_name);
    if(cur_tbl == NULL) {
        log_err("[db_manager.c:create_column()] the associated table doesn't exist, create table %s.\n", tbl_name);
        return NULL;
    }
    if(cur_tbl->size < cur_tbl->capacity) {
        Column* col = get_column(col_name);
        if(col != NULL) {
            log_info("[db_manager.c:create_column()] column %s exists.\n", col_name);
            return col;
        }
        col = malloc(sizeof(Column));
        col->name = malloc((strlen(col_name)+1)* sizeof(char));
        strcpy(col->name, col_name);
        col->aff_tbl_name = malloc((strlen(tbl_name)+1)* sizeof(char));
        strcpy(col->aff_tbl_name, tbl_name);
        col->cls_type = UNCLSR;
        col->idx_type = UNIDX;
        col->data = NULL;
        col->rowId = NULL;
        col->size = 0;
        col->capacity = 0;
        put_column(col_name,col);
        cur_tbl->columns[cur_tbl->size] = col;
        cur_tbl->size++;
        return col;
    }
    else {
        log_err("[db_manager.c:create_column()] the associated table is full, cannot create new column %s", col_name);
        return NULL;
    }
}


int set_column_idx_cls(Column* slcol, char* idx_type, char* cls_type) {
    if (strcmp(idx_type,"unidx") == 0) {
        slcol->idx_type = UNIDX;
        slcol->cls_type = UNCLSR;
    }
    else if (strcmp(idx_type,"btree") == 0 && strcmp(cls_type,"priclsr") == 0) {
        slcol->idx_type = BTREE;
        slcol->cls_type = PRICLSR;
        Table* tbl_aff = get_table(slcol->aff_tbl_name);
        tbl_aff->has_cluster_index = 1;
        tbl_aff->pricluster_index_col_name = malloc((strlen(slcol->name)+1)* sizeof(char));
        strcpy(tbl_aff->pricluster_index_col_name, slcol->name);
    }
    else if (strcmp(idx_type,"btree") == 0 && strcmp(cls_type,"clsr") == 0) {
        slcol->idx_type = BTREE;
        slcol->cls_type = CLSR;
    }
    else if (strcmp(idx_type,"btree") == 0 && strcmp(cls_type,"unclsr") == 0) {
        slcol->idx_type = BTREE;
        slcol->cls_type = UNCLSR;
    }
    else if (strcmp(idx_type,"sorted") == 0 && strcmp(cls_type,"priclsr") == 0) {
        slcol->idx_type = SORTED;
        slcol->cls_type = PRICLSR;
        Table* tbl_aff = get_table(slcol->aff_tbl_name);
        tbl_aff->has_cluster_index = 1;
        tbl_aff->pricluster_index_col_name = malloc((strlen(slcol->name)+1)* sizeof(char));
        strcpy(tbl_aff->pricluster_index_col_name, slcol->name);
    }
    else if (strcmp(idx_type,"sorted") == 0 && strcmp(cls_type,"clsr") == 0) {
        slcol->idx_type = SORTED;
        slcol->cls_type = CLSR;
    }
    else if (strcmp(idx_type,"sorted") == 0 && strcmp(cls_type,"unclsr") == 0) {
        slcol->idx_type = SORTED;
        slcol->cls_type = UNCLSR;
    }
    return 0;
}

int setup_db_csv() {
    char cwd[DIRLEN];
    if (getcwd(cwd, DIRLEN) == NULL) {
        log_err("current working dir path is too long");
        return 1;
    }
    strcat(cwd,"/");
    mkdir("db",0777);
    strcat(cwd,"db/");

    struct dirent* filename;
    DIR *pDir = opendir(cwd);
    if(pDir == NULL) {
        log_err("open dir %s error!\n",cwd);
        return 1;
    }
    message_status mes_status;
    while((filename = readdir(pDir)) != NULL) {
        if(strcmp(filename->d_name,".") != 0 && strcmp(filename->d_name,"..") != 0 && is_csv(filename->d_name) == true) {
            log_info("loading data %s into database\n", filename->d_name);
            char* db_file = malloc(sizeof(char) * ((strlen(cwd)+strlen(filename->d_name)+1)));
            strcpy(db_file,cwd);
            strcat(db_file,filename->d_name);
            FILE *fp;
            if((fp=fopen(db_file,"r"))==NULL) {
                log_err("cannot load file %s.\n", db_file);
                return 1;
            }
            char* line = NULL;
            size_t len = 0;
            while ((getline(&line, &len, fp)) != -1) {
                line = trim_newline(line);
                mes_status = OK_DONE;
                char* db_name = next_token_comma(&line,&mes_status);
                char* tbl_name = next_token_comma(&line,&mes_status);
                char* tbl_pricls_col_name = next_token_comma(&line,&mes_status);
                char* tbl_capacity = next_token_comma(&line,&mes_status);
                char* col_name = next_token_comma(&line,&mes_status);
                char* idx_type = next_token_comma(&line,&mes_status);
                char* cls_type = next_token_comma(&line,&mes_status);
                if(mes_status == INCORRECT_FORMAT) {
                    log_err("[db_manager.c:setup_db_csv()] tokenizing data failed.\n");
                    return 1;
                }
                current_db = create_db(db_name);
                if(current_db == NULL) {
                    log_err("[db_manager.c:setup_db_csv()] setup database failed.\n");
                    return 1;
                }
                Table* setup_tbl = create_table(db_name, tbl_name, tbl_pricls_col_name, atoi(tbl_capacity));
                if(setup_tbl == NULL) {
                    log_err("[db_manager.c:setup_db_csv()] setup table failed.\n");
                    return 1;
                }
                Column* scol = create_column(tbl_name, col_name);
                if(scol == NULL) {
                    log_err("[db_manager.c:setup_db_csv()] setup column failed.\n");
                    return 1;
                }
                log_info("table size:%d\n",setup_tbl->size);
                Column* setup_col = get_column(col_name);
                if(set_column_idx_cls(setup_col,idx_type,cls_type) != 0) {
                    log_err("[db_manager.c:setup_db_csv()] setup column index and clustering failed.\n");
                    return 1;
                }
                char* slvle = NULL;
                int count = 0;
                while ((slvle = next_token_comma(&line,&mes_status))!= NULL) {
                    if(count % 2 == 0) {
                        int rlv = atoi(slvle);
                        if(setup_col->size >= setup_col->capacity) {
                            size_t new_column_length = RESIZE * setup_col->capacity + 1;
                            size_t new_length = new_column_length;
                            size_t old_length = setup_col->capacity;
                            if(old_length == 0){
                                assert(new_length > 0);
                                setup_col->data = calloc(new_length,sizeof(int));
                                setup_col->rowId = calloc(new_length,sizeof(int));
                            }
                            else {
                                int* dd = resize_int(setup_col->data, old_length, new_length);
                                int* dr = resize_int(setup_col->rowId, old_length, new_length);
                                free(setup_col->data);
                                free(setup_col->rowId);
                                setup_col->data = calloc(new_length, sizeof(int));
                                setup_col->rowId = calloc(new_length, sizeof(int));
                                memcpy(setup_col->data,dd,new_length*sizeof(int));
                                memcpy(setup_col->rowId,dr,new_length*sizeof(int));
                                if(!setup_col->data || !setup_col->rowId) {
                                    log_err("creating more data space failed.\n");
                                    free(setup_col);
                                }
                            }
                            setup_col->capacity = new_length;
                        }
                        setup_col->rowId[setup_col->size] = rlv;
                    }
                    else {
                        int slv = atoi(slvle);
                        setup_col->data[setup_col->size] = slv;

                        setup_col->size++;
                    }
                    count++;
                }
            }
            free(line);
            free(db_file);
            fclose(fp);
        }
    }
    closedir(pDir);
    return 0;
}
