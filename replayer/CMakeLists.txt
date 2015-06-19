if(UNIX)
    set (CMAKE_CXX_FLAGS "-Wall -Wextra -pedantic -std=c++11 -g")
endif()

add_library(reader LogReader.cc)
add_executable(treader TinyReader.cc)
add_executable(tmerger TinyMerger.cc)

target_link_libraries(treader reader protobuf)
target_link_libraries(tmerger reader protobuf)
