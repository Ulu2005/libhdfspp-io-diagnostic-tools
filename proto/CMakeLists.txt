protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS log.proto)
add_library(protobuf ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(protobuf ${PROTOBUF_LIBRARIES})
