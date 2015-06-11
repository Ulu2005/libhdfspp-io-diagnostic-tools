include_directories(..)

if(UNIX)
    set (CMAKE_CXX_FLAGS "-Wall -Wextra -pedantic -std=c++11 -g")
endif()

add_executable(store_test store_test.cc)
add_executable(cstore_test concurrent_store_test.cc)
add_executable(reader TinyReader.cc)

target_link_libraries(store_test logger protobuf)
target_link_libraries(cstore_test logger protobuf)
target_link_libraries(reader protobuf)
