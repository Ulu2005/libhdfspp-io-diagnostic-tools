cmake_minimum_required (VERSION 2.8)
project (iotools)

set (CMAKE_BUILD_TYPE "Debug")

find_package(Protobuf REQUIRED)
find_package(Threads REQUIRED)

include_directories(utility)
include_directories(proto)
include_directories(logger)

if(UNIX)
    set (CMAKE_CXX_FLAGS "-Wall -Wextra -pedantic -std=c++11 -g")
endif()

if(APPLE)
    set(CMAKE_CXX_FLAGS "-Wall -Wextra -std=c++11 -stdlib=libc++ -Wno-deprecated-declarations -g")
endif()

add_subdirectory(proto)
add_subdirectory(logger)
add_subdirectory(replayer)
