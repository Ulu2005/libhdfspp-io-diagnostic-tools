add_library(reader LogReader.cc)
add_dependencies(reader protobuf)
add_executable(treader TinyReader.cc)
add_executable(tmerger TinyMerger.cc)

target_link_libraries(treader reader protobuf)
target_link_libraries(tmerger reader protobuf)
