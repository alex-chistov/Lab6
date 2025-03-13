cmake_minimum_required(VERSION 3.28)
project(Lab6)

set(CMAKE_CXX_STANDARD 20)

set(POSTGRESQL_DIR "C:/Program Files/PostgreSQL/17")
include_directories(${POSTGRESQL_DIR}/include)
link_directories(${POSTGRESQL_DIR}/lib)

add_executable(Lab6 main.cpp)

target_link_libraries(Lab6 libpq)