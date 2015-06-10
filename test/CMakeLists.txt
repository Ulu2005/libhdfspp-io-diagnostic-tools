include_directories(..)

add_executable(store_test store_test.cc)
add_executable(reader TinyReader.cc)

target_link_libraries(store_test logger protobuf)
target_link_libraries(reader protobuf)
