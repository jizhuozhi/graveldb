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

# Options
option(GRAVELDB_BUILD_BENCHMARKS "Build performance benchmarks" OFF)

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

# ---- Tests ----
# Single test program covering the public API (include/graveldb.h).
# Internal modules are deliberately not unit-tested here: their behavior is
# observable through the public surface, and unit tests for internals would
# couple to implementation details and slow refactoring.
add_executable(test-api tests/test_api.c)
target_link_libraries(test-api graveldb_lib pthread m)
target_include_directories(test-api PRIVATE tests)

enable_testing()
add_test(NAME api COMMAND test-api)

# ---- Benchmarks ----
# Off by default; opt in with -DGRAVELDB_BUILD_BENCHMARKS=ON.
# Not registered with ctest -- benches are run manually on perf hardware.
if(GRAVELDB_BUILD_BENCHMARKS)
    foreach(bench_name read_latency write_throughput checkpoint_impact)
        add_executable(bench-${bench_name} bench/bench_${bench_name}.c)
        target_link_libraries(bench-${bench_name} graveldb_lib pthread m)
        target_include_directories(bench-${bench_name} PRIVATE bench include)
    endforeach()
endif()

# Install
install(TARGETS graveldb-server DESTINATION bin)
install(TARGETS graveldb_lib graveldb_client DESTINATION lib)
install(FILES include/graveldb.h DESTINATION include/graveldb)
install(FILES example/protocol/graveldb_wire.h DESTINATION include/graveldb)
install(FILES example/client/client.h DESTINATION include/graveldb)
