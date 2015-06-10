cmake_minimum_required (VERSION 2.8)
project (iotools)

set (CMAKE_BUILD_TYPE "Debug")

find_package(Protobuf REQUIRED)

if (NOT Protobuf_FOUND)
    message(STATUS "Google Protobuf not found. Cannot compile.")
endif()

include_directories(proto)

add_subdirectory(proto)
add_subdirectory(test)

if(UNIX)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -pedantic -std=c++11 -g")
endif()

add_library(logger Logger.cc)
