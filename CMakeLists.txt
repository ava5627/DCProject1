cmake_minimum_required(VERSION 3.23)
project(Project1 C)

set(CMAKE_C_STANDARD 11)

add_executable(Project1 main.c)

target_link_libraries(Project1 pthread)