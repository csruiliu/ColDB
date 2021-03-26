# ColDB #

ColDB: A main-memory optimized column-store. 

This is an academic column-based database. It is implemented by C [only with Berkeley BSC].

## Dependencies ##

The ColDB requires libexplain. You can install this package via:

> `sudo apt install libexplain-dev`

Sometimes, there will be a fatal error: zconf.h: No such file or directory, it can be fixed via:

> `sudo apt install build-essential zlib1g-dev`

## Client-Server Code ##

We have included a simple unix socket implementation of an interactive client-server database implementation. You are recommended to use it as a foundation for your own database implementation. We have also provided a sample makefile that should be compatible with most machines. You are free to use your own makefile as well.

You can build both the client and server using:

> `make all`

You should spin up the server first before trying to connect the client.

> `./server`
> `./client`

A high-level explanation of what happens is:

1. The server creates a socket to listen for an incoming connection.

2. The client attempts to connect to the server.

3. When the client has successfully connected, it waits for input from stdin. Once received, it will create a message struct containing the input and then send it to the server.  It immediately waits for a response to determine if the server is willing to process the command.

4. When the server notices that a client has connected, it waits for a message from the client.  When it receives the message from the client, it parses the input and decides whether it is a valid/invalid query. It immediately returns the response indicating whether it was valid or not.

5. Once the client receives the response, three things are possible: 1) if the query was invalid, then just go back to waiting for input from stdin. 2) if the query was valid and the server indicates that it will send back the result, then wait to receive another message from the server. 3) if the query was valid but the server indicates that it will not send back the result, then go back to waiting for input on stdin.

6. Back on the server side, if the query is a valid query then it should process it, and then send back the result if it was asked to.

## Logging ##

We have included a couple useful logging functions in utils.c. These logging functions depend on #ifdef located within the code. There are multiple ways to enable logging. One way is by adding your own definition at the top of the file:

> `#define LOG 1`

The other way is to add it during the compilation process. Instead of running just `make`, you can run:

> `make CFLAGS+="-DLOG -DLOG_ERR -DLOG_INFO"`

## Database Schema ##

Each database is stored as a csv file (e.g., `coldb.csv`), located in `./src/db` folder, of course you can redirect it. 

In the csv file, each line (record) describes a single column with the following schema: 

| db | tbl | pricls_col | tbl_cap | col | index | cls | row_id | value | ... |
|---|---|---|---|---|---|---|---|---|---|
| db1 | table1 | col3 | 4 | col1 | btree | uncls | 0 | 192 | ... |

+ `db`: The database name that the column belongs to
+ `tbl`: The table name that the column belongs to
+ `pricls_col`: The principle cluster column name in the table. The first declared clustered index will be the principal copy of the data. This copy of the data will support unclustered indices.
+ `tbl_cap`: how many columns in the table
+ `col`: The column name for this record
+ `index`: The index supported by the current column 
+ `cls`: Whether the current index is clustered index or not
+ `row_id`: The row id
+ `value`: data stored in the corresponding row.

The schema design is based on the [instructions](Instructions.md). 

## Index ##

The supported index are B Tree and Sort.

Primary Clustered Index: the first declared clustered index, and only this principal copy of the data support support unclustered indices, which means all unclustered indices are built on top that.

Clustered Index: each clustered index will have a copy of data, so each cluster index for a column will lead a new column that has the name [db_name.table_name.col_name.idx_name].

Unclustered Index: the uncluster index is not persisted, so the unclustered index will be discarded when the database is closed.  


## Test ## 

The naive test files are located in `./project_tests` folder. In this folder, `csv` files are dataset, `dsl` files are workload, `exp` files are expected results (some exp are empty since the regarding workload don't have a result) 

**Milestone 1: test01 through test09**

| Test no. | Pass/Fail |
|:-----:|:-----:|
| Test 01 | **Pass** |
| Test 02 | **Pass** |
| Test 03 | **Pass** |
| Test 04 | **Pass** |
| Test 05 | **Pass** |
| Test 06 | **Pass** |
| Test 07 | **Pass** |
| Test 08 | **Pass** |
| Test 09 | **Pass** |

**Milestone 2: test10 through test17**

| Test no. | Pass/Fail |
|:-----:|:-----:|
| Test 10 | **Pass** |
| Test 11 | **Pass** |
| Test 12 | **Pass** |
| Test 13 | **Pass** |
| Test 14 | **Pass** |
| Test 15 | **Pass** |
| Test 16 | **Pass** |
| Test 17 | **Pass** |

**Milestone 3: test18 through test29**

| Test no. | Pass/Fail |
|:-----:|:-----:|
| Test 18 | **Pass** |
| Test 19 | **Pass** |
| Test 20 | **Pass** |
| Test 21 | **Pass** |
| Test 22 | **Pass** |
| Test 23 | **Pass** |
| Test 24 | **Pass** |
| Test 25 | **Pass** |
| Test 26 | **Pass** |
| Test 27 | **Pass** |
| Test 28 | **Pass** |
| Test 29 | **Pass** |

**Milestone 4: test30 through test35**

| Test no. | Pass/Fail |
|:-----:|:-----:|
| Test 30 | **Pass** |
| Test 31 | **Pass** |
| Test 32 | **Pass** |
| Test 33 | **Pass** |
| Test 34 | **Pass** |
| Test 35 | **Pass** |

---------

Reference:
---
Note that this project originates from Stratos Idreos's CS165 course at Harvard. He and his TAs have put a tremendous amount of effort into making this project. Some of the documentation has been updated to reflect our course number UChicago CMSC33500. 
