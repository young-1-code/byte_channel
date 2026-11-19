cmake_minimum_required(VERSION 3.16)
project(byte_channel LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

add_library(byte_channel INTERFACE)
target_include_directories(byte_channel INTERFACE
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}/core
  ${CMAKE_CURRENT_SOURCE_DIR}/core/ipc)
target_link_libraries(byte_channel INTERFACE Threads::Threads)

function(add_byte_channel_target target source)
  add_executable(${target} ${source})
  target_link_libraries(${target} PRIVATE byte_channel)
  target_compile_options(${target} PRIVATE -Wall -Wextra -Werror
    -Wno-stringop-truncation)
endfunction()

add_byte_channel_target(channel_example example/channel_example.cpp)
add_byte_channel_target(channel_benchmark benchmark/channel_benchmark.cpp)
add_byte_channel_target(ipc_channel_benchmark benchmark/ipc_channel_benchmark.cpp)
add_byte_channel_target(latency_benchmark benchmark/latency_benchmark.cpp)
add_byte_channel_target(spsc_channel_stress benchmark/spsc_channel_stress.cpp)
add_byte_channel_target(spsc_pop_lock_benchmark benchmark/spsc_pop_lock_benchmark.cpp)

set(UDP_SOURCES
  core/udp/proto.cpp
  core/udp/engine/system/udp_socket.cpp
  core/udp/engine/system/udp_endpoint.cpp
  core/udp/engine/system/udp_event_loop.cpp
  core/udp/udp_pub.cpp
  core/udp/udp_sub.cpp)

add_executable(udp_nak_example example/udp_nak_example.cpp ${UDP_SOURCES})
target_link_libraries(udp_nak_example PRIVATE byte_channel)
target_compile_options(udp_nak_example PRIVATE -Wall -Wextra -Werror
  -Wno-stringop-truncation)

add_executable(udp_nak_stress benchmark/udp_nak_stress.cpp ${UDP_SOURCES})
target_link_libraries(udp_nak_stress PRIVATE byte_channel)
target_compile_options(udp_nak_stress PRIVATE -Wall -Wextra -Werror
  -Wno-stringop-truncation)

