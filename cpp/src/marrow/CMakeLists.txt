cmake_minimum_required(VERSION 3.10)
project(marrow)

add_library(marrow INTERFACE)
target_sources(marrow INTERFACE compare.h string_array.h index.h)