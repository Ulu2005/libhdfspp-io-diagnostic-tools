add_library(logger Logging.cc Logger.cc)
add_dependencies(logger protobuf)
target_link_libraries(logger ${CMAKE_THREAD_LIBS_INIT} rt)
