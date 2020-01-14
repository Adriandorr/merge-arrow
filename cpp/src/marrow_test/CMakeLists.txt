cmake_minimum_required(VERSION 3.10)
project(cpp)

set(CMAKE_CXX_STANDARD 17)

add_executable(marrow_test compare_test.cpp index_test.cpp sort_test.cpp left_test.cpp)
add_test(NAME marrow_test
        COMMAND marrow_test)

target_link_libraries(marrow_test libarrow.so gtest gmock gtest_main)