cmake_minimum_required (VERSION 2.8)
project (iotools)

set (CMAKE_BUILD_TYPE "Debug")

find_package(Protobuf REQUIRED)
find_package(Threads REQUIRED)

if (NOT Protobuf_FOUND)
    message(STATUS "Google Protobuf not found. Cannot compile.")
endif()

if(NOT CMAKE_THREAD_LIBS_INIT)
    message(STATUS "Threads library not found. Cannot compile.")
endif()

include_directories(proto)

add_subdirectory(proto)
add_subdirectory(test)

if(UNIX)
    set (CMAKE_CXX_FLAGS "-Wall -Wextra -pedantic -std=c++11 -g")
endif()

add_library(logger Logging.cc Logger.cc)
target_link_libraries(logger "${CMAKE_THREAD_LIBS_INIT}")
