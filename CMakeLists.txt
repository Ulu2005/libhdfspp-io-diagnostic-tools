cmake_minimum_required (VERSION 2.8)
project (iotools)

set (CMAKE_BUILD_TYPE "Debug")

find_package(Protobuf REQUIRED)

include_directories(proto)
include_directories(${PROTOBUF_INCLUDE_DIRS})

add_subdirectory(proto)

if(UNIX)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -pedantic -std=c++11 -g")
endif()

add_executable(main Logger.cc)
add_executable(loader TinyLoader.cc)

target_link_libraries(main protobuf ${PROTOBUF_LIBRARIES})
target_link_libraries(loader protobuf ${PROTOBUF_LIBRARIES})
