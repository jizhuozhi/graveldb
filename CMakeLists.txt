cmake_minimum_required(VERSION 3.14)
project(graveldb C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Compiler flags
add_compile_options(-Wall -Wextra -O2 -g)

# Platform detection
if(APPLE)
    add_compile_definitions(_DARWIN_C_SOURCE)
endif()

# Library
add_library(graveldb_lib STATIC
    src/hash_index.c
    src/dirty_tracker.c
    src/overlay.c
    src/dimbin.c
    src/slab_alloc.c
    src/dim_registry.c
    src/checkpoint.c
    src/io_uring_flush.c
    src/graveldb.c
)
target_include_directories(graveldb_lib PUBLIC include)
target_include_directories(graveldb_lib PRIVATE src)

# On Linux, link liburing for io_uring support
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    find_library(LIBURING uring)
    if(LIBURING)
        target_link_libraries(graveldb_lib PUBLIC ${LIBURING})
        target_compile_definitions(graveldb_lib PUBLIC GRAVELDB_USE_IO_URING=1)
        message(STATUS "Found liburing: ${LIBURING} - io_uring flush enabled")
    else()
        target_compile_definitions(graveldb_lib PUBLIC GRAVELDB_USE_IO_URING=0)
        message(STATUS "liburing not found - using pwrite+fdatasync fallback")
    endif()
endif()

# Server library (reusable without main)
add_library(graveldb_server_lib STATIC
    example/server/server.c
    example/server/io_poller.c
)
target_link_libraries(graveldb_server_lib graveldb_lib pthread)
target_include_directories(graveldb_server_lib PUBLIC example/server)
target_include_directories(graveldb_server_lib PRIVATE include example/protocol)

# Server executable
add_executable(graveldb-server example/server/main.c)
target_link_libraries(graveldb-server graveldb_server_lib)
target_include_directories(graveldb-server PRIVATE include example/server example/protocol)

# Client Library
add_library(graveldb_client STATIC
    example/client/client.c
)
target_include_directories(graveldb_client PUBLIC example/client)
target_include_directories(graveldb_client PRIVATE include example/protocol)

# Tests
add_executable(test-basic tests/test_basic.c)
target_link_libraries(test-basic graveldb_lib pthread)
target_include_directories(test-basic PRIVATE src)

add_executable(test-hash-index tests/test_hash_index.c)
target_link_libraries(test-hash-index graveldb_lib pthread)
target_include_directories(test-hash-index PRIVATE src)

add_executable(test-slab-alloc tests/test_slab_alloc.c)
target_link_libraries(test-slab-alloc graveldb_lib pthread)
target_include_directories(test-slab-alloc PRIVATE src)

add_executable(test-dim-registry tests/test_dim_registry.c)
target_link_libraries(test-dim-registry graveldb_lib pthread)
target_include_directories(test-dim-registry PRIVATE src)

add_executable(test-integration tests/test_integration.c)
target_link_libraries(test-integration graveldb_lib pthread)
target_include_directories(test-integration PRIVATE src)

add_executable(test-checkpoint tests/test_checkpoint.c)
target_link_libraries(test-checkpoint graveldb_lib pthread)
target_include_directories(test-checkpoint PRIVATE src)

# Benchmarks
add_executable(bench-main tests/bench.c)
target_link_libraries(bench-main graveldb_lib pthread)
target_include_directories(bench-main PRIVATE src)

add_executable(bench-hash tests/bench_hash_compare.c)
target_include_directories(bench-hash PRIVATE src)

add_executable(bench-dirty-tracker tests/bench_dirty_tracker.c)
target_link_libraries(bench-dirty-tracker graveldb_lib pthread)
target_include_directories(bench-dirty-tracker PRIVATE src)

add_executable(bench-cache tests/bench_cache.c)
target_link_libraries(bench-cache graveldb_lib pthread m)
target_include_directories(bench-cache PRIVATE src)

add_executable(bench-hash-index tests/bench_hash_index.c)
target_link_libraries(bench-hash-index graveldb_lib pthread)
target_include_directories(bench-hash-index PRIVATE src)

add_executable(bench-slab-alloc tests/bench_slab_alloc.c)
target_link_libraries(bench-slab-alloc graveldb_lib pthread)
target_include_directories(bench-slab-alloc PRIVATE src)

add_executable(bench-latency tests/bench_latency.c)
target_link_libraries(bench-latency graveldb_lib pthread)
target_include_directories(bench-latency PRIVATE src)

add_executable(bench-write-buffer tests/bench_write_buffer.c)
target_link_libraries(bench-write-buffer graveldb_lib pthread)
target_include_directories(bench-write-buffer PRIVATE src)

add_executable(bench-memory-scaling tests/bench_memory_scaling.c)
target_link_libraries(bench-memory-scaling graveldb_lib pthread m)
target_include_directories(bench-memory-scaling PRIVATE src)

add_executable(bench-network example/bench/bench_network.c)
target_link_libraries(bench-network graveldb_client graveldb_server_lib m)
target_include_directories(bench-network PRIVATE example/client example/server include example/protocol)

add_executable(bench-network-scale example/bench/bench_network_scale.c)
target_link_libraries(bench-network-scale graveldb_client pthread m)
target_include_directories(bench-network-scale PRIVATE example/client)

# CTest
enable_testing()
add_test(NAME basic         COMMAND test-basic)
add_test(NAME hash-index    COMMAND test-hash-index)
add_test(NAME slab-alloc    COMMAND test-slab-alloc)
add_test(NAME dim-registry  COMMAND test-dim-registry)
add_test(NAME integration   COMMAND test-integration)
add_test(NAME checkpoint    COMMAND test-checkpoint)

# Install
install(TARGETS graveldb-server DESTINATION bin)
install(TARGETS graveldb_lib graveldb_client DESTINATION lib)
install(FILES include/graveldb.h DESTINATION include/graveldb)
install(FILES example/protocol/graveldb_wire.h DESTINATION include/graveldb)
install(FILES example/client/client.h DESTINATION include/graveldb)
