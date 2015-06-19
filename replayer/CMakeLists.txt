if(UNIX)
    set (CMAKE_CXX_FLAGS "-Wall -Wextra -pedantic -std=c++11 -g")
endif()

add_executable(reader TinyReader.cc)

target_link_libraries(reader protobuf)
