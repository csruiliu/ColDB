#include <utils.h>
#include <memory.h>
#include <assert.h>
#include <zconf.h>
#include <sys/stat.h>
#include <dirent.h>
#include "db_manager.h"
#include "utils.h"

#define RESIZE 2
#define DIRLEN 256

// In this class, there will always be only one active database at a time
Db *current_db;

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
	current_db = get_db(db_name);
	if(current_db != NULL) {
		log_info("database %s exists, open database %s\n", db_name, db_name);
		return current_db;
	}
	current_db = malloc(sizeof(Db));
	current_db->db_name = malloc((strlen(db_name)+1)* sizeof(char));
	strcpy(current_db->db_name,db_name);
	current_db->db_capacity = 0;
	current_db->db_size = 0;
	current_db->tables = NULL;
	put_db(db_name,current_db);
	return current_db;
}

Table* create_table(char* db_name, char* tbl_name, size_t num_columns) {
	if(strcmp(current_db->db_name,db_name) != 0) {
		log_err("the current active database is not database %s\n", db_name);
		return NULL;
	}
	Table* tbl = get_tbl(tbl_name);
	if(tbl != NULL) {
		log_info("the table %s exists", tbl_name);
		return tbl;
	}
	else {
		if(current_db->db_capacity - current_db->db_size <= 0) {
			log_info("no enough table space in database %s, creating more table space.\n", current_db->db_name);
			size_t more_table_capacity = RESIZE * current_db->db_capacity + 1;
			size_t new_capacity = sizeof(Table*) * more_table_capacity;
			size_t old_capacity = sizeof(Table*) * current_db->db_capacity;
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
					log_err("create more table space in database failed.\n", current_db->db_name);
					free(tbl);
					return NULL;
				}
			}
			current_db->db_capacity = more_table_capacity;
		}
		tbl = malloc(sizeof(Table));
		tbl->tbl_name = malloc((strlen(tbl_name)+1)*sizeof(char));
		strcpy(tbl->tbl_name,tbl_name);
		tbl->db_name_aff = malloc((strlen(db_name)+1)*sizeof(char));
		strcpy(tbl->db_name_aff,db_name);
		tbl->tbl_capacity = num_columns;
		tbl->tbl_size = 0;
		tbl->pricls_col_name = NULL;
		tbl->hasCls = 0;
		tbl->columns = calloc(num_columns, sizeof(Column*));
		put_tbl(tbl_name,tbl);
		current_db->tables[current_db->db_size] = tbl;
		current_db->db_size++;
	    return tbl;
	}
}

Column* create_column(char* tbl_name, char* col_name) {
	Table* cur_tbl = get_tbl(tbl_name);
	if(cur_tbl == NULL) {
        log_err("the associated table doesn't exist, create table %s", tbl_name);
        return NULL;
	}
	if(cur_tbl->tbl_size < cur_tbl->tbl_capacity) {
        Column* col = get_col(col_name);
        if(col != NULL) {
            log_info("column %s exists\n", col_name);
            return col;
        }
        col = malloc(sizeof(Column));
        col->col_name = malloc((strlen(col_name)+1)* sizeof(char));
        strcpy(col->col_name, col_name);
		col->tbl_name_aff = malloc((strlen(tbl_name))* sizeof(char));
        strcpy(col->tbl_name_aff, tbl_name);
		col->cls_type = UNCLSR;
        col->idx_type = UNIDX;
        col->data = NULL;
        col->rowId = NULL;
        col->col_size = 0;
        col->col_capacity = 0;
        put_col(col_name,col);
        cur_tbl->columns[cur_tbl->tbl_size] = col;
        cur_tbl->tbl_size++;
        return col;
	}
	else {
	    log_err("the associated table is full, cannot create new column %s", col_name);
	    return NULL;
	}
}

int load_data_csv(char* data_path) {
	message_status mes_status;
	FILE *fp;
	if((fp=fopen(data_path,"r"))==NULL) {
		log_err("cannot load data %s\n", data_path);
		return 1;
	}
	char *line = NULL;
	size_t len = 0;
	int read = getline(&line, &len, fp);
	if (read == -1) {
		log_err("read file header failed.\n");
		return 1;
	}
	char* line_copy = malloc((strlen(line)+1)* sizeof(char));
	strcpy(line_copy,line);
	int headerCount = 0;
	char* sepTmp = NULL;
	while(1) {
		sepTmp = next_token_comma(&line_copy,&mes_status);
		if(sepTmp == NULL) {
			break;
		}
		else {
			headerCount++;
		}
	}
	log_info("%d columns in the loading file\n", headerCount);
    if (headerCount == 1) {
        char* header = trim_newline(line);
        Column* lcol = get_col(header);
        if (lcol == NULL) {
            log_err("[db_manager.c:load_data_csv] cannot find column %s in database\n", header);
            free(line_copy);
            fclose(fp);
            return 1;
        }
        if(lcol->cls_type == UNCLSR) {
            int rowId_load = 0;
            while ((getline(&line, &len, fp)) != -1) {
                char *va = line;
                int lv = atoi(va);
                if(insert_data_col(lcol,lv,rowId_load) != 0) {
                    free(line_copy);
                    fclose(fp);
                    return 1;
                }
                rowId_load++;
            }
        }
        //TODO
    }
    else {
        //TODO
    }
	return 0;
}

int insert_data_col(Column* col, int data, int rowId) {
	if (col->col_size >= col->col_capacity) {
		size_t new_column_length = RESIZE * col->col_capacity + 1;
		size_t new_length = new_column_length;
		size_t old_length = col->col_capacity;
		if (old_length == 0) {
			assert(new_length > 0);
			//log_info("column %s has new length %d\n",col->col_name, new_length);
			col->data = calloc(new_length, sizeof(int));
			col->rowId = calloc(new_length, sizeof(int));
		} else {
			int* dd = resize_int(col->data, old_length, new_length);
			int* dr = resize_int(col->rowId, old_length, new_length);
			free(col->data);
			free(col->rowId);
			col->data = calloc(new_length, sizeof(int));
			col->rowId = calloc(new_length, sizeof(int));
			memcpy(col->data, dd, new_length * sizeof(int));
			memcpy(col->rowId, dr, new_length * sizeof(int));
			if (!col->data) {
				log_err("creating more data space failed.\n");
				return 1;
			}
		}
		col->col_capacity = new_length;
	}
	col->data[col->col_size] = data;
	col->rowId[col->col_size] = rowId;
	col->col_size++;
	return 0;
}

int persist_data_csv() {
	if(current_db == NULL) {
		log_err("there is no active database\n");
		return 1;
	}
    char cwd[DIRLEN];
    if (getcwd(cwd, DIRLEN) == NULL) {
        log_err("current working dir path is too long");
        return 1;
    }
	strcat(cwd,"/");
	mkdir("db",0777);
	strcat(cwd,"db/");
    strcat(cwd,current_db->db_name);
    strcat(cwd,".csv");
	log_info("current database is stored at: %s\n", cwd);
	FILE *fp = NULL;
	fp = fopen(cwd, "w+");
	if(fp == NULL) {
		log_err("cannot open the database store");
		return 1;
	}
	fprintf(fp, "%s", current_db->db_name);
	for(int i = 0; i < current_db->db_size; ++i) {
		Table* stbl = get_tbl(current_db->tables[i]->tbl_name);
		fprintf(fp, ",%s", stbl->tbl_name);
		fprintf(fp, ",%d", stbl->tbl_capacity);
		for(int j = 0; j < stbl->tbl_size; ++j) {
			Column* scol = get_col(stbl->columns[j]->col_name);
			fprintf(fp, ",%s", scol->col_name);
			if(scol->idx_type == BTREE) {
				fprintf(fp, ",btree");
			}
			else if(scol->idx_type == SORTED) {
				fprintf(fp, ",sorted");
			}
			else if(scol->idx_type == UNIDX) {
				fprintf(fp,",unidx");
			}
			if(scol->cls_type == CLSR) {
				fprintf(fp, ",clsr");
			}
			else if(scol->cls_type == PRICLSR) {
				fprintf(fp, ",priclsr");
			}
			else if(scol->cls_type == UNCLSR) {
				fprintf(fp, ",unclsr");
			}
			for(int k = 0; k < scol->col_size; ++k) {
				fprintf(fp, ",%d", scol->rowId[k]);
				fprintf(fp, ",%d", scol->data[k]);
			}
			fprintf(fp, "\n");
		}
	}
	fclose(fp);
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
    mes_status = OK_DONE;
    while((filename = readdir(pDir)) != NULL) {
        if(strcmp(filename->d_name,".") != 0 && strcmp(filename->d_name,"..") != 0) {
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
                char* db_name = next_token_comma(&line,&mes_status);
                char* tbl_name = next_token_comma(&line,&mes_status);
                char* tbl_capacity = next_token_comma(&line,&mes_status);
                char* col_name = next_token_comma(&line,&mes_status);
                char* idx_type = next_token_comma(&line,&mes_status);
				char* cls_type = next_token_comma(&line,&mes_status);
                if(mes_status == INCORRECT_FORMAT) {
					log_err("tokenizing data failed.\n");
                	return 1;
                }
				Db* setup_db = create_db(db_name);
                if(setup_db == NULL) {
                	log_err("[db_manager.c:setup_db_csv()] setup database failed.\n");
                	return 1;
                }
				Table* setup_tbl = create_table(db_name,tbl_name, atoi(tbl_capacity));
				if(setup_tbl == NULL) {
					log_err("[db_manager.c:setup_db_csv()] setup table failed.\n");
					return 1;
				}
                Column* scol = create_column(tbl_name,col_name);
            	if(scol == NULL) {
					log_err("[db_manager.c:setup_db_csv()] setup column failed.\n");
					return 1;
            	}
            	Column* setup_col = get_col(col_name);
            	if(set_column_idx_cls(setup_col,idx_type,cls_type) != 0) {
					log_err("[db_manager.c:setup_db_csv()] setup column index and clustering failed.\n");
					return 1;
            	}
				char* slvle = NULL;
				int count = 0;
				while ((slvle = next_token_comma(&line,&mes_status))!= NULL) {
					if(count % 2 == 0) {
						int rlv = atoi(slvle);
						if(setup_col->col_size >= setup_col->col_capacity) {
							size_t new_column_length = RESIZE * setup_col->col_capacity + 1;
							size_t new_length = new_column_length;
							size_t old_length = setup_col->col_capacity;
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
							setup_col->col_capacity = new_length;
						}
						setup_col->rowId[setup_col->col_size] = rlv;
					}
					else {
						int slv = atoi(slvle);
						setup_col->data[setup_col->col_size] = slv;
						setup_col->col_size++;
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

int set_column_idx_cls(Column* slcol, char* idx_type, char* cls_type) {
    if (strcmp(idx_type,"unidx") == 0) {
        slcol->idx_type = UNIDX;
        slcol->cls_type = UNCLSR;
    }
    else if (strcmp(idx_type,"btree") == 0 && strcmp(cls_type,"priclsr") == 0) {
        slcol->idx_type = BTREE;
        slcol->cls_type = PRICLSR;
		Table* tbl_aff = get_tbl(slcol->tbl_name_aff);
        tbl_aff->hasCls = 1;
		tbl_aff->pricls_col_name = malloc((strlen(slcol->col_name)+1)* sizeof(char));
		strcpy(tbl_aff->pricls_col_name,slcol->col_name);
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
		Table* tbl_aff = get_tbl(slcol->tbl_name_aff);
		tbl_aff->hasCls = 1;
		tbl_aff->pricls_col_name = malloc((strlen(slcol->col_name)+1)* sizeof(char));
		strcpy(tbl_aff->pricls_col_name,slcol->col_name);
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
