#include <utils.h>
#include <memory.h>
#include <assert.h>
#include <zconf.h>
#include <sys/stat.h>
#include <dirent.h>

#include "db_cluster.h"
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
	Db* db = get_db(db_name);
	if(db != NULL) {
		log_info("database %s exists, open database %s\n", db_name, db_name);
		return current_db;
	}
	db = malloc(sizeof(Db));
	db->db_name = malloc((strlen(db_name)+1)* sizeof(char));
	strcpy(db->db_name,db_name);
	db->db_capacity = 0;
	db->db_size = 0;
	db->tables = NULL;
	put_db(db_name,db);
	log_info("create db %s successfully.\n",db_name);
	return db;
}

Table* create_table(char* db_name, char* tbl_name, char* pricls_col_name, size_t num_columns) {
	if(strcmp(current_db->db_name,db_name) != 0) {
		log_err("the current active database is not database %s\n", db_name);
		return NULL;
	}
	Table* tbl = get_tbl(tbl_name);
	if(tbl != NULL) {
		log_info("the table %s exists.\n", tbl_name);
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
		tbl->pricls_col_name = malloc((strlen(pricls_col_name)+1)* sizeof(char));
        strcpy(tbl->pricls_col_name,pricls_col_name);
        if(strcmp(pricls_col_name,"NULL") == 0) {
            tbl->hasCls = 0;
        }
        else {
            tbl->hasCls = 1;
        }
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
        log_err("[db_manager.c:create_column()] the associated table doesn't exist, create table %s.\n", tbl_name);
        return NULL;
	}
	if(cur_tbl->tbl_size < cur_tbl->tbl_capacity) {
        Column* col = get_col(col_name);
        if(col != NULL) {
            log_info("[db_manager.c:create_column()] column %s exists.\n", col_name);
            return col;
        }
        col = malloc(sizeof(Column));
        col->col_name = malloc((strlen(col_name)+1)* sizeof(char));
        strcpy(col->col_name, col_name);
		col->tbl_name_aff = malloc((strlen(tbl_name)+1)* sizeof(char));
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
	    log_err("[db_manager.c:create_column()] the associated table is full, cannot create new column %s", col_name);
	    return NULL;
	}
}

int tbl_psylayout_cls(Table* tbl_aff, IndexType idx_type) {
	return 0;
}

int load_data_csv(char* data_path) {
	message_status mes_status = OK_DONE;
	FILE *fp;
	if((fp=fopen(data_path,"r"))==NULL) {
		log_err("[db_manager.c:load_data_csv()] cannot load data %s\n", data_path);
		return 1;
	}
	char *line = NULL;
	size_t len = 0;
	int read = getline(&line, &len, fp);
	if (read == -1) {
		log_err("[db_manager.c:load_data_csv()] read file header failed.\n");
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

    //only 1 column to load
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
        else if(lcol->cls_type == PRICLSR) {
            ClusterLink* clink = clusterlink_init();
            int rowId_load = 0;
            while ((getline(&line, &len, fp)) != -1) {
                char *va = line;
                int lv = atoi(va);
                clusterlink_insert(lv,rowId_load,clink);
                rowId_load++;
            }
            if(lcol->idx_type == BTREE) {
				//TODO
            }
            else if(lcol->idx_type == SORTED) {
				clink = clusterlink_sort(clink);
				clusterlink_load(clink,lcol);
				free(line_copy);
				fclose(fp);
            }
        }
        //TODO: Mutiple clustered indices
    }
    //multiple columns to load
    else {
		char* header = trim_newline(line);
        Column** colSet = calloc(headerCount, sizeof(Column*));
		ClusterLink** slSet = calloc(headerCount, sizeof(ClusterLink*));
		for(int i = 0; i < headerCount; ++i) {
			char* col_name = next_token_comma(&header, &mes_status);
			colSet[i] = get_col(col_name);
			if (colSet[i] == NULL) {
				log_err("[db_manager.c:load_data_csv] cannot find column %s in database\n", col_name);
				free(line_copy);
				fclose(fp);
				return 1;
			}
			slSet[i] = clusterlink_init();
		}
		int rowId_load = 0;
		while ((getline(&line, &len, fp)) != -1) {
			for (int i = 0; i < headerCount; ++i) {
				char *va = next_token_comma(&line, &mes_status);
				int lv = atoi(va);
				clusterlink_insert(lv,rowId_load,slSet[i]);
			}
			rowId_load++;
		}
		for(int j = 0; j < headerCount; ++j) {
			if(colSet[j]->cls_type == UNCLSR) {
				if(clusterlink_load(slSet[j],colSet[j]) != 0) {
					log_err("[db_manager.c:load_data_csv] load column %s in database failed.\n", colSet[j]->col_name);
					free(line_copy);
					fclose(fp);
					return 1;
				}
			}
			else if(colSet[j]->cls_type == PRICLSR) {
				if(colSet[j]->idx_type == BTREE) {
					//TODO
				}
				else if(colSet[j]->idx_type == SORTED) {
					slSet[j] = clusterlink_sort(slSet[j]);
					if(clusterlink_load(slSet[j],colSet[j]) != 0) {
						log_err("[db_manager.c:load_data_csv] load column %s with sorted index in database failed.\n", colSet[j]->col_name);
						free(line_copy);
						fclose(fp);
						return 1;
					}
				}
			}
		}
		//TODO: Mutiple clustered indices
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
        log_err("Current working dir path is too long");
        return 1;
    }
	strcat(cwd,"/");
	mkdir("db",0777);
	strcat(cwd,"db/");
    strcat(cwd,current_db->db_name);
    strcat(cwd,".csv");
	log_info("Current database is stored at: %s\n", cwd);
	FILE *fp = NULL;
	fp = fopen(cwd, "w+");
	if(fp == NULL) {
		log_err("cannot open the database store");
		return 1;
	}
	for(int i = 0; i < current_db->db_size; ++i) {
	    Table* stbl = get_tbl(current_db->tables[i]->tbl_name);

	    for(int j = 0; j < stbl->tbl_size; ++j) {
	        fprintf(fp, "%s", current_db->db_name);
		    fprintf(fp, ",%s", stbl->tbl_name);
            fprintf(fp, ",%s", stbl->pricls_col_name);
		    fprintf(fp, ",%zu", stbl->tbl_capacity);
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
    log_info("The database is successfully stored at: %s\n", cwd);
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
                Column* scol = create_column(tbl_name,col_name);
            	if(scol == NULL) {
					log_err("[db_manager.c:setup_db_csv()] setup column failed.\n");
					return 1;
            	}
            	log_info("table size:%d\n",setup_tbl->tbl_size);
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

int insert_data_tbl(Table* itbl, int* row_values) {
	for(int i = 0; i < itbl->tbl_size; ++i) {
		Column* icol = get_col(itbl->columns[i]->col_name);
		if(insert_data_col(icol,row_values[i],icol->col_size+i) != 0) {
			log_err("[db_manager.c:insert_data_tbl()] insert table failed.\n");
			return 1;
		}
	}
	return 0;
}

int select_data_col_unidx(Column* scol, char* handle, char* pre_range, char* post_range) {
	Result* rsl = malloc(sizeof(Result));
	if (strncmp(pre_range,"null",4) == 0) {
		int post = atoi(post_range);
		int* scol_data = scol->data;
		int* rsl_data = calloc(scol->col_size, sizeof(int));
		unsigned int count = 0;
		for(int i = 0; i < scol->col_size; ++i) {
			if(scol_data[i] < post) {
				rsl_data[count] = i;
				count++;
			}
		}
		rsl->num_tuples = count;
		rsl->data_type = INT;
		rsl->payload = calloc(count, sizeof(int));
		memcpy(rsl->payload,rsl_data,count* sizeof(int));
        put_rsl_replace(handle,rsl);
	}
	else if (strncmp(post_range,"null",4) == 0) {
		int pre = atoi(pre_range);
		int* scol_data = scol->data;
		int* rsl_data = calloc(scol->col_size, sizeof(int));
		unsigned int count = 0;
		for(int i = 0; i < scol->col_size; ++i) {
			if(scol_data[i] >= pre) {
				rsl_data[count] = i;
				count++;
			}
		}
		rsl->num_tuples = count;
		rsl->data_type = INT;
		rsl->payload = calloc(count, sizeof(int));
		memcpy(rsl->payload,rsl_data,count* sizeof(int));
        put_rsl_replace(handle,rsl);
	}
	else {
		int pre = atoi(pre_range);
		int post = atoi(post_range);
		int* scol_data = scol->data;
		int* rsl_data = calloc(scol->col_size, sizeof(int));
		unsigned int count = 0;
		for(int i = 0; i < scol->col_size; ++i) {
			if(scol_data[i] >= pre && scol_data[i] < post) {
				rsl_data[count] = i;
				count++;
			}
		}
		rsl->num_tuples = count;
		rsl->data_type = INT;
		rsl->payload = calloc(count, sizeof(int));
		memcpy(rsl->payload,rsl_data,count* sizeof(int));
		put_rsl_replace(handle,rsl);
	}
	return 0;
}

int select_data_rsl(Result* srsl_pos, Result* srsl_val, char* handle, char* pre_range, char* post_range) {
	size_t rsl_size = srsl_pos->num_tuples;
	int* srsl_pos_payload = srsl_pos->payload;
	int* srsl_val_payload = srsl_val->payload;
	int* rsl_payload = calloc(rsl_size, sizeof(int));
	Result* rsl = malloc(sizeof(Result));
	if (strncmp(pre_range,"null",4) == 0) {
		int post = atoi(post_range);
		size_t size = 0;
		for(int i = 0; i < rsl_size; ++i) {
			if(srsl_val_payload[i] < post) {
				rsl_payload[size] = srsl_pos_payload[i];
				size++;
			}
		}
		rsl->num_tuples = size;
		rsl->data_type = INT;
		rsl->payload = calloc(size, sizeof(int));
		memcpy(rsl->payload, rsl_payload, size*sizeof(int));
		put_rsl_replace(handle,rsl);
	}
	else if (strncmp(post_range,"null",4) == 0) {
		int pre = atoi(pre_range);
		size_t size = 0;
		for(int i = 0; i < rsl_size; ++i) {
			if(srsl_val_payload[i] >= pre) {
				rsl_payload[size] = srsl_pos_payload[i];
				size++;
			}
		}
		rsl->num_tuples = size;
		rsl->data_type = INT;
		rsl->payload = calloc(size, sizeof(int));
		memcpy(rsl->payload, rsl_payload, size*sizeof(int));
		put_rsl_replace(handle,rsl);
	}
	else {
		int pre = atoi(pre_range);
		int post = atoi(post_range);
		size_t size = 0;
		for(int i = 0; i < rsl_size; ++i) {
			if(srsl_val_payload[i] < post && srsl_val_payload[i] >= pre) {
				rsl_payload[size] = srsl_pos_payload[i];
				size++;
			}
		}
		rsl->num_tuples = size;
		rsl->data_type = INT;
		rsl->payload = calloc(size, sizeof(int));
		memcpy(rsl->payload, rsl_payload, size*sizeof(int));
		put_rsl_replace(handle,rsl);
	}
	return 0;
}

int fetch_col_data(char* col_val_name, char* rsl_vec_pos, char* handle) {
	Result* rsl_pos = get_rsl(rsl_vec_pos);
	if(rsl_pos == NULL) {
		log_err("[db_manager.c:fetch_col_data] fetch position didn't exist.\n");
		return 1;
	}
	Column* col_val = get_col(col_val_name);
	if(col_val == NULL) {
		log_err("[db_manager.c:fetch_col_data] fetch col didn't exist.\n");
		return 1;
	}
	size_t rsl_size = rsl_pos->num_tuples;
	Result* rsl = malloc(sizeof(Result));
	rsl->data_type = INT;
	rsl->num_tuples = rsl_size;
	rsl->payload = calloc(rsl_size, sizeof(int));
	int* row_id = rsl_pos->payload;
	int* fetch_payload = calloc(rsl_size, sizeof(int));
	for(int i = 0; i < rsl_size; ++i) {
		fetch_payload[i] = col_val->data[row_id[i]];
	}
	memcpy(rsl->payload, fetch_payload, rsl_size*sizeof(int));
	put_rsl_replace(handle, rsl);
	return 0;
}

char* generate_print_result(size_t print_num, char** print_name) {
	size_t rsl_total_tuples = 0;
	for(size_t i = 0; i< print_num; ++i) {
		Result* rsl = get_rsl(print_name[i]);
		rsl_total_tuples += rsl->num_tuples;
	}
	char* print_rsl = malloc(rsl_total_tuples * (sizeof(long)+1));
	sprintf(print_rsl,"");
	log_info("[Server results]\n");
	for(size_t i = 0; i< print_num; ++i) {
		Result* rsl = get_rsl(print_name[i]);
		if(rsl->data_type == INT) {
			for(size_t j = 0; j < rsl->num_tuples; ++j) {
				log_info("%d\n",((int *)rsl->payload)[j]);
				//we allocate 1 one to avoid overflow
				char* tmp_payload_data = malloc(sizeof(int)+1);
				sprintf(tmp_payload_data, "%d\n", ((int *)rsl->payload)[j]);
				strcat(print_rsl,tmp_payload_data);
			}
		}
		else if(rsl->data_type == FLOAT) {
			for(size_t j = 0; j < rsl->num_tuples; ++j) {
				log_info("%0.2f\n",((double *)rsl->payload)[j]);
				char* tmp_payload_data = malloc(sizeof(double)+1);
				sprintf(tmp_payload_data, "%0.2f\n", ((double *)rsl->payload)[j]);
				strcat(print_rsl,tmp_payload_data);
			}
		}
		else if(rsl->data_type == LONG) {
			for(size_t j = 0; j < rsl->num_tuples; ++j) {
				log_info("%ld\n",((double *)rsl->payload)[j]);
				char* tmp_payload_data = malloc(sizeof(long)+1);
				sprintf(tmp_payload_data, "%ld\n", ((long *)rsl->payload)[j]);
				strcat(print_rsl,tmp_payload_data);
			}
		}
	}
	log_info("\n");
	return print_rsl;
	/*
	int* data_payload_id = malloc(print_num* sizeof(int));
	void** data_payload_set = malloc(print_num * sizeof(void*));
	size_t row_total_length = 0;
	size_t row_total_num = 0;
	for(int i = 0; i < print_num; ++i) {
		Result* rsl = get_rsl(print_name[i]);
		size_t row_num = rsl->num_tuples;
		row_total_num += row_num;
		if(rsl->data_type == INT) {
			row_total_length += row_num * sizeof(int);
			data_payload_id[i] = 1;
			data_payload_set[i] = calloc(row_num, sizeof(int));
			memcpy(data_payload_set[i], rsl->payload, row_num* sizeof(int));
		}
		else if (rsl->data_type == FLOAT) {
			row_total_length += row_num * sizeof(double);
			data_payload_id[i] = 2;
			data_payload_set[i] = calloc(row_num, sizeof(double)+1);
			memcpy(data_payload_set[i], rsl->payload, row_num* sizeof(double));
		}
		else if (rsl->data_type == LONG) {
			row_total_length += row_num * sizeof(long);
			data_payload_id[i] = 3;
			data_payload_set[i] = calloc(row_num, sizeof(long));
			memcpy(data_payload_set[i], rsl->payload, row_num* sizeof(long));
		}
	}
	char* print_rsl = malloc((row_total_length+1) * sizeof(char));
	void* data_payload;
	for(int j = 0; j < row_total_num; ++j) {
		char* payload_tmp = (char*) malloc((row_total_length+1) * sizeof(char));
        int data_payload_len = 0;
		for(int k = 0; k < print_num; ++k) {
			switch (data_payload_id[k]) {
				case 1:
					data_payload = data_payload_set[k];
                    data_payload_len += sprintf(payload_tmp+data_payload_len, "%d,", ((int *)data_payload)[j]);
					break;
				case 2:
					data_payload = data_payload_set[k];
                    data_payload_len += sprintf(payload_tmp+data_payload_len, "%0.2f,", ((double *)data_payload)[j]);
					break;
				case 3:
					data_payload = data_payload_set[k];
                    data_payload_len += sprintf(payload_tmp+data_payload_len, "%ld,", ((long *)data_payload)[j]);
					break;
			}
		}
		payload_tmp[data_payload_len-1] = '\n';
		strcat(print_rsl,payload_tmp);
		free(payload_tmp);
	}
	size_t lastchar = strlen(print_rsl);
	print_rsl[lastchar] = '\0';

	return print_rsl;
	*/
}

int avg_col_data(char* avg_col_name, char* handle) {
	Column* acol = get_col(avg_col_name);
	if (acol == NULL) {
		log_err("[db_manager.c:avg_col_data()]: column didn't exist in the database.\n");
		return 1;
	}
	int sum = 0;
	int* acol_data = acol->data;
	for(int i = 0; i < acol->col_size; ++i) {
		sum += acol_data[i];
	}
	double avg = (double) sum / (double) acol->col_size;
	Result* arsl = malloc(sizeof(Result));
	if(arsl == NULL) {
		log_err("[db_manager.c:avg_col_data()]: init new result failed.\n");
		return 1;
	}
	arsl->data_type = FLOAT;
	arsl->num_tuples = 1;
	arsl->payload = calloc(1, sizeof(double));
	memcpy(arsl->payload, &avg, sizeof(double));
	put_rsl_replace(handle,arsl);
	return 0;
}

int avg_rsl_data(char* avg_rsl_name, char* handle) {
	Result* avg_rsl = get_rsl(avg_rsl_name);
	double avg = 0;
	if (avg_rsl == NULL) {
		log_err("[db_manager.c:avg_col_data()]: result didn't exist in the database.\n");
		return 1;
	}
	if(avg_rsl->data_type == INT) {
		int* int_avg_payload = avg_rsl->payload;
		int sum = 0;
		for(int i = 0; i < avg_rsl->num_tuples; ++i) {
			sum += int_avg_payload[i];
		}
		avg = (double) sum / (double) avg_rsl->num_tuples;
	}
	else if(avg_rsl->data_type == FLOAT) {
		float* float_avg_payload = avg_rsl->payload;
		float sum = 0;
		for(int i = 0; i < avg_rsl->num_tuples; ++i) {
			sum += float_avg_payload[i];
		}
		avg = (double) sum / (double) avg_rsl->num_tuples;
	}
	else if(avg_rsl->data_type == LONG) {
		long* long_avg_payload = avg_rsl->payload;
		long sum = 0;
		for(int i = 0; i < avg_rsl->num_tuples; ++i) {
			log_info("%d item:%d\n",i,long_avg_payload[i]);
			sum += long_avg_payload[i];
		}
		avg = (double) sum / (double) avg_rsl->num_tuples;
	}
	Result* rsl = malloc(sizeof(Result));
	if(rsl == NULL) {
		log_err("[db_manager.c:avg_col_data()]: init new result failed.\n");
		return 1;
	}
	rsl->data_type = FLOAT;
	rsl->num_tuples = 1;
	rsl->payload = calloc(1, sizeof(double));
	memcpy(rsl->payload, &avg, sizeof(double));
	put_rsl_replace(handle,rsl);
	return 0;
}

int sum_col_data(char* sum_col_name, char* handle) {
	Column* sum_col = get_col(sum_col_name);
	if (sum_col == NULL) {
		log_err("[db_manager.c:sum_col_data()]: column didn't exist in the database.\n");
		return 1;
	}
	long sum = 0;
	int* sum_payload = sum_col->data;
	for(int i = 0; i < sum_col->col_size; ++i) {
		sum += sum_payload[i];
	}
	Result* rsl = malloc(sizeof(Result));
	if(rsl == NULL) {
		log_err("[db_manager.c:avg_col_data()]: init new result failed.\n");
		return 1;
	}
	rsl->data_type = LONG;
	rsl->num_tuples = 1;
	rsl->payload = calloc(1, sizeof(long));
	memcpy(rsl->payload, &sum, sizeof(long));
	put_rsl_replace(handle,rsl);
	return 0;
}

int sum_rsl_data(char* sum_rsl_name, char* handle) {
	Result* sum_rsl = get_rsl(sum_rsl_name);
	if (sum_rsl == NULL) {
		log_err("[db_manager.c:sum_rsl_data()]: result didn't exist.\n");
		return 1;
	}
	if(sum_rsl->data_type == FLOAT) {
		double sum = 0;
		double* float_sum_payload = sum_rsl->payload;
		for(int i = 0; i < sum_rsl->num_tuples; ++i) {
			sum += float_sum_payload[i];
		}
		Result* rsl = malloc(sizeof(Result));
		if(rsl == NULL) {
			log_err("[db_manager.c:avg_col_data()]: init new result failed.\n");
			return 1;
		}
		rsl->data_type = FLOAT;
		rsl->num_tuples = 1;
		rsl->payload = calloc(1, sizeof(double));
		memcpy(rsl->payload, &sum, sizeof(double));
		put_rsl_replace(handle,rsl);
	}
	else if (sum_rsl->data_type == INT) {
		long sum = 0;
		int* int_sum_payload = sum_rsl->payload;
		for(int i = 0; i < sum_rsl->num_tuples; ++i) {
			sum += int_sum_payload[i];
		}
		Result* rsl = malloc(sizeof(Result));
		if(rsl == NULL) {
			log_err("[db_manager.c:avg_col_data()]: init new result failed.\n");
			return 1;
		}
		rsl->data_type = LONG;
		rsl->num_tuples = 1;
		rsl->payload = calloc(1, sizeof(long));
		memcpy(rsl->payload, &sum, sizeof(long));
		put_rsl_replace(handle,rsl);
	}
	else {
		long sum = 0;
		long* int_sum_payload = sum_rsl->payload;
		for(int i = 0; i < sum_rsl->num_tuples; ++i) {
			sum += int_sum_payload[i];
		}
		Result* rsl = malloc(sizeof(Result));
		if(rsl == NULL) {
			log_err("[db_manager.c:avg_col_data()]: init new result failed.\n");
			return 1;
		}
		rsl->data_type = LONG;
		rsl->num_tuples = 1;
		rsl->payload = calloc(1, sizeof(long));
		memcpy(rsl->payload, &sum, sizeof(long));
		put_rsl_replace(handle,rsl);
	}
	return 0;
}

int add_col_col(char* add_name1, char* add_name2, char* handle) {
	Column* add1 = get_col(add_name1);
	Column* add2 = get_col(add_name2);
	if(add1->col_size != add2->col_size) {
		log_err("two items in add operation have different number records.\n");
		return 1;
	}
	size_t count = add1->col_size;
	long* add_sum = calloc(count, sizeof(long));
	for(int i = 0; i < count; ++i) {
		add_sum[i] = add1->data[i] + add2->data[2];
	}
	Result* rsl = malloc(sizeof(Result));
	rsl->num_tuples = count;
	rsl->data_type = LONG;
	rsl->payload = calloc(count, sizeof(long));
	memcpy(rsl->payload, add_sum, count* sizeof(long));
	put_rsl_replace(handle,rsl);
	return 0;
}

int add_rsl_rsl(char* add_name1, char* add_name2, char* handle) {
	Result* add1 = get_rsl(add_name1);
	Result* add2 = get_rsl(add_name2);
	if(add1->num_tuples != add2->num_tuples) {
		log_err("two items in add operation have different number records.\n");
		return 1;
	}
	size_t count = add1->num_tuples;
	if(add1->data_type == INT && add2->data_type == INT) {
		long* add_sum = calloc(count, sizeof(long));
		int* add1_payload = add1->payload;
		int* add2_payload = add2->payload;
		for(int i = 0; i < count; ++i) {
			add_sum[i] = add1_payload[i] + add2_payload[i];
		}
		Result* rsl = malloc(sizeof(Result));
		rsl->num_tuples = count;
		rsl->data_type = LONG;
		rsl->payload = calloc(count, sizeof(long));
		memcpy(rsl->payload, add_sum, count* sizeof(long));
		put_rsl_replace(handle,rsl);
		return 0;
	}
	else if (add1->data_type == FLOAT || add2->data_type == FLOAT) {
		double* add_sum = calloc(count, sizeof(double));
		double* add1_payload = add1->payload;
		double* add2_payload = add2->payload;
		for(int i = 0; i < count; ++i) {
			add_sum[i] = add1_payload[i] + add2_payload[i];
		}
		Result* rsl = malloc(sizeof(Result));
		rsl->num_tuples = count;
		rsl->data_type = FLOAT;
		rsl->payload = calloc(count, sizeof(double));
		memcpy(rsl->payload, add_sum, count* sizeof(double));
		put_rsl_replace(handle,rsl);
		return 0;
	}
}

int add_col_rsl(char* add_name1, char* add_name2, char* handle) {
	Column* add1 = get_col(add_name1);
	Result* add2 = get_rsl(add_name2);
	if(add1->col_size != add2->num_tuples) {
		log_err("two items in add operation have different number records.\n");
		return 1;
	}
	size_t count = add1->col_size;
	if (add2->data_type == INT) {
		long* add_sum = calloc(count, sizeof(long));
		int* add1_payload = add1->data;
		int* add2_payload = add2->payload;
		for(int i = 0; i < count; ++i) {
			add_sum[i] = add1_payload[i] + add2_payload[i];
		}
		Result* rsl = malloc(sizeof(Result));
		rsl->num_tuples = count;
		rsl->data_type = LONG;
		rsl->payload = calloc(count, sizeof(long));
		memcpy(rsl->payload, add_sum, count* sizeof(long));
		put_rsl_replace(handle,rsl);
		return 0;
	}
	else if (add2->data_type == FLOAT) {
		double* add_sum = calloc(count, sizeof(double));
		double* add1_payload = add1->data;
		double* add2_payload = add2->payload;
		for(int i = 0; i < count; ++i) {
			add_sum[i] = add1_payload[i] + add2_payload[i];
		}
		Result* rsl = malloc(sizeof(Result));
		rsl->num_tuples = count;
		rsl->data_type = FLOAT;
		rsl->payload = calloc(count, sizeof(double));
		memcpy(rsl->payload, add_sum, count* sizeof(double));
		put_rsl_replace(handle,rsl);
		return 0;
	}
}

int add_rsl_col(char* add_name1, char* add_name2, char* handle) {
	Result* add1 = get_rsl(add_name1);
	Column* add2 = get_col(add_name2);
	if(add1->num_tuples != add2->col_size) {
		log_err("two items in add operation have different number records.\n");
		return 1;
	}
	size_t count = add1->num_tuples;
	if (add1->data_type == INT) {
		long* add_sum = calloc(count, sizeof(long));
		int* add1_payload = add1->payload;
		int* add2_payload = add2->data;
		for(int i = 0; i < count; ++i) {
			add_sum[i] = add1_payload[i] + add2_payload[i];
		}
		Result* rsl = malloc(sizeof(Result));
		rsl->num_tuples = count;
		rsl->data_type = LONG;
		rsl->payload = calloc(count, sizeof(long));
		memcpy(rsl->payload, add_sum, count* sizeof(long));
		put_rsl_replace(handle,rsl);
		return 0;
	}
	else if (add1->data_type == FLOAT) {
		double* add_sum = calloc(count, sizeof(double));
		double* add1_payload = add1->payload;
		double* add2_payload = (double *) add2->data;
		for(int i = 0; i < count; ++i) {
			add_sum[i] = add1_payload[i] + add2_payload[i];
		}
		Result* rsl = malloc(sizeof(Result));
		rsl->num_tuples = count;
		rsl->data_type = FLOAT;
		rsl->payload = calloc(count, sizeof(double));
		memcpy(rsl->payload, add_sum, count* sizeof(double));
		put_rsl_replace(handle,rsl);
		return 0;
	}
	return 0;
}

int sub_col_col(char* sub_name1, char* sub_name2, char* handle) {
	Column* sub1 = get_col(sub_name1);
	Column* sub2 = get_col(sub_name2);
	if(sub1->col_size != sub2->col_size) {
		log_err("two items in sub operation have different number records.\n");
		return 1;
	}
	size_t count = sub1->col_size;
	long* sub_sum = calloc(count, sizeof(long));
	for(int i = 0; i < count; ++i) {
		sub_sum[i] = sub1->data[i] - sub2->data[i];
	}
	Result* rsl = malloc(sizeof(Result));
	rsl->num_tuples = count;
	rsl->data_type = LONG;
	rsl->payload = calloc(count, sizeof(long));
	memcpy(rsl->payload, sub_sum, count* sizeof(long));
	put_rsl_replace(handle,rsl);
	return 0;
}

int sub_rsl_rsl(char* sub_name1, char* sub_name2, char* handle) {
	Result* sub1 = get_rsl(sub_name1);
	Result* sub2 = get_rsl(sub_name2);
	if(sub1->num_tuples != sub2->num_tuples) {
		log_err("two items in add operation have different number records.\n");
		return 1;
	}
	size_t count = sub1->num_tuples;
	if(sub1->data_type == INT && sub2->data_type == INT) {
		long* sub_sum = calloc(count, sizeof(long));
		int* sub1_payload = sub1->payload;
		int* sub2_payload = sub2->payload;
		for(int i = 0; i < count; ++i) {
			sub_sum[i] = sub1_payload[i] - sub2_payload[i];
		}
		Result* rsl = malloc(sizeof(Result));
		rsl->num_tuples = count;
		rsl->data_type = LONG;
		rsl->payload = calloc(count, sizeof(long));
		memcpy(rsl->payload, sub_sum, count* sizeof(long));
		put_rsl_replace(handle,rsl);
		return 0;
	}
	else if (sub1->data_type == FLOAT || sub2->data_type == FLOAT) {
		double* sub_sum = calloc(count, sizeof(double));
		double* sub1_payload = sub1->payload;
		double* sub2_payload = sub2->payload;
		for(int i = 0; i < count; ++i) {
			sub_sum[i] = sub1_payload[i] - sub2_payload[i];
		}
		Result* rsl = malloc(sizeof(Result));
		rsl->num_tuples = count;
		rsl->data_type = FLOAT;
		rsl->payload = calloc(count, sizeof(double));
		memcpy(rsl->payload, sub_sum, count* sizeof(double));
		put_rsl_replace(handle,rsl);
		return 0;
	}
}

int max_rsl_value(char* max_vec, char* handle) {
    Result* vrsl = get_rsl(max_vec);
    if(vrsl == NULL) {
        log_err("the result %s for max didn't exist in the database", max_vec);
        return 1;
    }
    if(vrsl->data_type == INT) {
        int* vrsl_payload = vrsl->payload;
        int max = vrsl_payload[0];
        size_t max_cnt = vrsl->num_tuples;
        for(int i = 1; i < max_cnt; ++i) {
            if(max < vrsl_payload[i]) {
                max = vrsl_payload[i];
            }
        }
        Result* rsl = malloc(sizeof(Result));
        rsl->data_type = INT;
        rsl->num_tuples = 1;
        rsl->payload = calloc(1, sizeof(int));
        memcpy(rsl->payload, &max, sizeof(int));
        put_rsl_replace(handle,rsl);
        return 0;
    }
    else if (vrsl->data_type == FLOAT) {
        double* vrsl_payload = vrsl->payload;
        double max = vrsl_payload[0];
        size_t max_cnt = vrsl->num_tuples;
        for(int i = 1; i < max_cnt; ++i) {
            if(max < vrsl_payload[i]) {
                max = vrsl_payload[i];
            }
        }
        Result* rsl = malloc(sizeof(Result));
        rsl->data_type = FLOAT;
        rsl->num_tuples = 1;
        rsl->payload = calloc(1, sizeof(double));
        memcpy(rsl->payload, &max, sizeof(double));
        put_rsl_replace(handle,rsl);
        return 0;
    }
    else {
        long* vrsl_payload = vrsl->payload;
        long max = vrsl_payload[0];
        size_t max_cnt = vrsl->num_tuples;
        for(int i = 1; i < max_cnt; ++i) {
            if(max < vrsl_payload[i]) {
                max = vrsl_payload[i];
            }
        }
        Result* rsl = malloc(sizeof(Result));
        rsl->data_type = LONG;
        rsl->num_tuples = 1;
        rsl->payload = calloc(1, sizeof(long));
        memcpy(rsl->payload, &max, sizeof(long));
        put_rsl_replace(handle,rsl);
        return 0;
    }
}

int max_rsl_value_pos(char* max_vec_pos, char* max_vec_value, char* handle_pos, char* handle_value) {
    Result* rsl_pos = get_rsl(max_vec_pos);
    Result* rsl_value = get_rsl(max_vec_value);
    if(rsl_pos == NULL || rsl_value == NULL) {
        log_err("the result %s/%s for max didn't exist in the database", max_vec_pos, max_vec_value);
        return 1;
    }
    if (rsl_value->data_type == INT) {
        int* rsl_value_payload = rsl_value->payload;
        int max = rsl_value_payload[0];
        size_t max_cnt = rsl_value->num_tuples;
        int pmax;
        for(int i = 1; i < max_cnt; ++i) {
            if(max < rsl_value_payload[i]) {
                max = rsl_value_payload[i];
                pmax = i;
            }
        }
        int* max_pos_payload = rsl_pos->payload;
        int max_pos = max_pos_payload[pmax];

        Result* vrsl = malloc(sizeof(Result));
        vrsl->data_type = INT;
        vrsl->num_tuples = 1;
        vrsl->payload = calloc(1, sizeof(int));
        memcpy(vrsl->payload, &max, sizeof(int));
        put_rsl_replace(handle_value,vrsl);

        Result* prsl = malloc(sizeof(Result));
        prsl->data_type = INT;
        prsl->num_tuples = 1;
        prsl->payload = calloc(1, sizeof(int));
        memcpy(prsl->payload, &max_pos, sizeof(int));
        put_rsl_replace(handle_pos,prsl);
        return 0;
    }
    else if (rsl_value->data_type == FLOAT) {
        double* rsl_value_payload = rsl_value->payload;
        double max = rsl_value_payload[0];
        size_t max_cnt = rsl_value->num_tuples;
        int pmax;
        for(int i = 1; i < max_cnt; ++i) {
            if(max < rsl_value_payload[i]) {
                max = rsl_value_payload[i];
                pmax = i;
            }
        }
        int* max_pos_payload = rsl_pos->payload;
        int max_pos = max_pos_payload[pmax];

        Result* vrsl = malloc(sizeof(Result));
        vrsl->data_type = FLOAT;
        vrsl->num_tuples = 1;
        vrsl->payload = calloc(1, sizeof(double));
        memcpy(vrsl->payload, &max, sizeof(double));
        put_rsl_replace(handle_value,vrsl);

        Result* prsl = malloc(sizeof(Result));
        prsl->data_type = INT;
        prsl->num_tuples = 1;
        prsl->payload = calloc(1, sizeof(int));
        memcpy(prsl->payload, &max_pos, sizeof(int));
        put_rsl_replace(handle_pos,prsl);
        return 0;
    }
    else {
        long* rsl_value_payload = rsl_value->payload;
        long max = rsl_value_payload[0];
        size_t max_cnt = rsl_value->num_tuples;
        int pmax;
        for(int i = 1; i < max_cnt; ++i) {
            if(max < rsl_value_payload[i]) {
                max = rsl_value_payload[i];
                pmax = i;
            }
        }
        int* max_pos_payload = rsl_pos->payload;
        int max_pos = max_pos_payload[pmax];

        Result* vrsl = malloc(sizeof(Result));
        vrsl->data_type = LONG;
        vrsl->num_tuples = 1;
        vrsl->payload = calloc(1, sizeof(long));
        memcpy(vrsl->payload, &max, sizeof(long));
        put_rsl_replace(handle_value,vrsl);

        Result* prsl = malloc(sizeof(Result));
        prsl->data_type = INT;
        prsl->num_tuples = 1;
        prsl->payload = calloc(1, sizeof(int));
        memcpy(prsl->payload, &max_pos, sizeof(int));
        put_rsl_replace(handle_pos,prsl);
        return 0;
    }
}

int min_rsl_value(char* min_vec,char* handle) {
	Result* vrsl = get_rsl(min_vec);
	if(vrsl == NULL) {
		log_err("the result %s for min didn't exist in the database", min_vec);
		return 1;
	}
	if(vrsl->data_type == INT) {
		int* vrsl_payload = vrsl->payload;
		int min = vrsl_payload[0];
		size_t min_cnt = vrsl->num_tuples;
		for(int i = 1; i < min_cnt; ++i) {
			if(min > vrsl_payload[i]) {
				min = vrsl_payload[i];
			}
		}
		Result* rsl = malloc(sizeof(Result));
		rsl->data_type = INT;
		rsl->num_tuples = 1;
		rsl->payload = calloc(1, sizeof(int));
		memcpy(rsl->payload, &min, sizeof(int));
		put_rsl_replace(handle,rsl);
		return 0;
	}
	else if (vrsl->data_type == FLOAT) {
		double* vrsl_payload = vrsl->payload;
		double min = vrsl_payload[0];
		size_t min_cnt = vrsl->num_tuples;
		for(int i = 1; i < min_cnt; ++i) {
			if(min > vrsl_payload[i]) {
				min = vrsl_payload[i];
			}
		}
		Result* rsl = malloc(sizeof(Result));
		rsl->data_type = FLOAT;
		rsl->num_tuples = 1;
		rsl->payload = calloc(1, sizeof(double));
		memcpy(rsl->payload, &min, sizeof(double));
		put_rsl_replace(handle,rsl);
		return 0;
	}
	else {
		long* vrsl_payload = vrsl->payload;
		long min = vrsl_payload[0];
		size_t min_cnt = vrsl->num_tuples;
		for(int i = 1; i < min_cnt; ++i) {
			if(min > vrsl_payload[i]) {
				min = vrsl_payload[i];
			}
		}
		Result* rsl = malloc(sizeof(Result));
		rsl->data_type = LONG;
		rsl->num_tuples = 1;
		rsl->payload = calloc(1, sizeof(long));
		memcpy(rsl->payload, &min, sizeof(long));
		put_rsl_replace(handle,rsl);
		return 0;
	}
}

int min_rsl_value_pos(char* min_vec_pos, char* min_vec_value, char* handle_pos, char* handle_value) {
	Result* rsl_pos = get_rsl(min_vec_pos);
	Result* rsl_value = get_rsl(min_vec_value);
	if(rsl_pos == NULL || rsl_value == NULL) {
		log_err("the result %s/%s for min didn't exist in the database", min_vec_pos, min_vec_value);
		return 1;
	}
	if (rsl_value->data_type == INT) {
		int* rsl_value_payload = rsl_value->payload;
		int min = rsl_value_payload[0];
		size_t min_cnt = rsl_value->num_tuples;
		int pmin;
		for(int i = 1; i < min_cnt; ++i) {
			if(min > rsl_value_payload[i]) {
				min = rsl_value_payload[i];
				pmin = i;
			}
		}
		int* min_pos_payload = rsl_pos->payload;
		int min_pos = min_pos_payload[pmin];

		Result* vrsl = malloc(sizeof(Result));
		vrsl->data_type = INT;
		vrsl->num_tuples = 1;
		vrsl->payload = calloc(1, sizeof(int));
		memcpy(vrsl->payload, &min, sizeof(int));
		put_rsl_replace(handle_value,vrsl);

		Result* prsl = malloc(sizeof(Result));
		prsl->data_type = INT;
		prsl->num_tuples = 1;
		prsl->payload = calloc(1, sizeof(int));
		memcpy(prsl->payload, &min_pos, sizeof(int));
		put_rsl_replace(handle_pos,prsl);
		return 0;
	}
	else if (rsl_value->data_type == FLOAT) {
		double* rsl_value_payload = rsl_value->payload;
		double min = rsl_value_payload[0];
		size_t min_cnt = rsl_value->num_tuples;
		int pmin;
		for(int i = 1; i < min_cnt; ++i) {
			if(min > rsl_value_payload[i]) {
				min = rsl_value_payload[i];
				pmin = i;
			}
		}
		int* min_pos_payload = rsl_pos->payload;
		int min_pos = min_pos_payload[pmin];

		Result* vrsl = malloc(sizeof(Result));
		vrsl->data_type = FLOAT;
		vrsl->num_tuples = 1;
		vrsl->payload = calloc(1, sizeof(double));
		memcpy(vrsl->payload, &min, sizeof(double));
		put_rsl_replace(handle_value,vrsl);

		Result* prsl = malloc(sizeof(Result));
		prsl->data_type = INT;
		prsl->num_tuples = 1;
		prsl->payload = calloc(1, sizeof(int));
		memcpy(prsl->payload, &min_pos, sizeof(int));
		put_rsl_replace(handle_pos,prsl);
		return 0;
	}
	else {
		long* rsl_value_payload = rsl_value->payload;
		long min = rsl_value_payload[0];
		size_t min_cnt = rsl_value->num_tuples;
		int pmin;
		for(int i = 1; i < min_cnt; ++i) {
			if(min > rsl_value_payload[i]) {
				min = rsl_value_payload[i];
				pmin = i;
			}
		}
		int* min_pos_payload = rsl_pos->payload;
		int min_pos = min_pos_payload[pmin];

		Result* vrsl = malloc(sizeof(Result));
		vrsl->data_type = LONG;
		vrsl->num_tuples = 1;
		vrsl->payload = calloc(1, sizeof(long));
		memcpy(vrsl->payload, &min, sizeof(long));
		put_rsl_replace(handle_value,vrsl);

		Result* prsl = malloc(sizeof(Result));
		prsl->data_type = INT;
		prsl->num_tuples = 1;
		prsl->payload = calloc(1, sizeof(int));
		memcpy(prsl->payload, &min_pos, sizeof(int));
		put_rsl_replace(handle_pos,prsl);
		return 0;
	}
}