cmake_minimum_required(VERSION 3.10)
project(marrow)

set(CMAKE_CXX_STANDARD 17)

#find_package(arrow)
#if(Arrow_FOUND)
#    message("Arrow not found")
#endif()

list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

enable_testing() # Must be in main file

include(AddGoogleTest.cmake)

include_directories(src)

add_subdirectory(src/marrow)
add_subdirectory(src/marrow_test)