cmake_minimum_required(VERSION 3.17)
project(coldb C)

set(CMAKE_C_STANDARD 11)

include_directories(src/include)

add_executable(coldb
        src/include/common.h
        src/include/db_element.h
        src/include/db_manager.h
        src/include/kv_store.h
        src/include/message.h
        src/include/operator.h
        src/include/parse.h
        src/include/utils_func.h
        src/client.c
        src/db_element.c
        src/db_manager.c
        src/kv_store.c
        src/parse.c
        src/server.c
        src/utils_func.c)
