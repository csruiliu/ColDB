cmake_minimum_required(VERSION 3.17)
project(coldb C)

set(CMAKE_C_STANDARD 11)

include_directories(src/include)

add_executable(coldb
        src/include/common.h
        src/include/db_element.h
        src/include/db_manager.h
        src/include/message.h
        src/include/operator.h
        src/include/parse.h
        src/include/utils.h
        src/client.c
        src/db_manager.c
        src/parse.c
        src/server.c
        src/utils.c)
