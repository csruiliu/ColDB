# ColDB #

UChicago CMSC33500 Project: A main-memory optimized column-store, [using c only with Berkeley BSC]

**Milestone 1**: design and implement the basic functionality of a column-store with the ability to run single-table queries

**Milestone 2**: add fast scans, using scan sharing & multi-cores

**Milestone 3**: add indexing support to boost select operators

**Milestone 4**: add hash-join/nested-join support to ColDB system

**TODO 1: Fine-grained query partition for batch**

**TODO 2: Multiple clustered and unclustered indices (Done!)**

## Instructions ##

### Getting Started ###

Please follow the instructions in the README in the project root for 
setting up your git repository.

We recommend that throughout the quarter, you make git tags at each of
the checkpoints so that it's easier to manage the progress of your project.

### Client-Server code ###
We have included a simple unix socket implementation of an interactive
client-server database implementation. You are recommended to use it
as a foundation for your own database implementation. We have also
provided a sample makefile that should be compatible with most machines.
You are free to use your own makefile as well.

You can build both the client and server using:

> `make all`

You should spin up the server first before trying to connect the client.

> `./server`
> `./client`

A high-level explanation of what happens is:

1. The server creates a socket to listen for an incoming connection.

2. The client attempts to connect to the server.

3. When the client has successfully connected, it waits for input from stdin.
Once received, it will create a message struct containing the input and
then send it to the server.  It immediately waits for a response to determine
if the server is willing to process the command.

4. When the server notices that a client has connected, it waits for a message
from the client.  When it receives the message from the client, it parses the
input and decides whether it is a valid/invalid query.
It immediately returns the response indicating whether it was valid or not.

5. Once the client receives the response, three things are possible:
1) if the query was invalid, then just go back to waiting for input from stdin.
2) if the query was valid and the server indicates that it will send back the
result, then wait to receive another message from the server.
3) if the query was valid but the server indicates that it will not send back
the result, then go back to waiting for input on stdin.

6. Back on the server side, if the query is a valid query then it should
process it, and then send back the result if it was asked to.

### Dependencies
Right now the starter code requires that you use libexplain. You can install this 
package via:

> `sudo apt-get install libexplain-dev`

Sometimes, there will be a fatal error: zconf.h: No such file or directory, it can be fixed via:

> `sudo apt-get install build-essential zlib1g-dev`

### Logging ###

We have included a couple useful logging functions in utils.c.
These logging functions depend on #ifdef located within the code.
There are multiple ways to enable logging. One way is by adding your own
definition at the top of the file:

> `#define LOG 1`

The other way is to add it during the compilation process. Instead of running
just `make`, you can run:

> `make CFLAGS+="-DLOG -DLOG_ERR -DLOG_INFO"


### Persistence CSV ###

Each line/record describes each column: 

| aff_db_name | tbl_name | pricls_col_name | tbl_capacity | col_name | index_type | cls_type | row_id | value | ... | row_id | value |
|---|---|---|---|---|---|---|---|---|---|---|---|
|   |   |   |   |   |   |   |   |   |   |   |   |


---
Note that this project originates from Stratos Idreos’s CS165 course at Harvard. He and his TAs have put a tremendous amount of effort into making this project. Some of the documentation has been updated to reflect our course number, but you may see references to 165. 
