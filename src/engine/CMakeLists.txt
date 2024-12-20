cmake_minimum_required(VERSION 3.13)
find_program(MAKE_EXE NAMES gmake nmake make)

project(vdb LANGUAGES CXX C)

# https://www.cmake.org/cmake/help/latest/policy/CMP0025.html
#
# Compiler id for Apple Clang is now AppleClang.
cmake_policy(SET CMP0025 NEW)

# https://cmake.org/cmake/help/latest/policy/CMP0042.html
#
# Enable MACOSX_RPATH by default. @rpath in a target's install name is
# a more flexible and powerful mechanism than @executable_path or
# @loader_path for locating shared libraries.
cmake_policy(SET CMP0042 NEW)

# https://www.cmake.org/cmake/help/latest/policy/CMP0054.html
#
# Only interpret if() arguments as variables or keywords when unquoted.
cmake_policy(SET CMP0054 NEW)

# https://www.cmake.org/cmake/help/latest/policy/CMP0057.html
#
# Support new if() IN_LIST operator.
cmake_policy(SET CMP0057 NEW)

# https://www.cmake.org/cmake/help/latest/policy/CMP0063.html
#
# Adapted from Apache Kudu: https://github.com/apache/kudu/commit/bd549e13743a51013585
# Honor visibility properties for all target types.
cmake_policy(SET CMP0063 NEW)

# https://cmake.org/cmake/help/latest/policy/CMP0068.html
#
# RPATH settings on macOS do not affect install_name.
cmake_policy(SET CMP0068 NEW)

# https://cmake.org/cmake/help/latest/policy/CMP0074.html
#
# find_package() uses <PackageName>_ROOT variables.
cmake_policy(SET CMP0074 NEW)

# https://cmake.org/cmake/help/latest/policy/CMP0091.html
#
# MSVC runtime library flags are selected by an abstraction.
cmake_policy(SET CMP0091 NEW)

# https://cmake.org/cmake/help/latest/policy/CMP0135.html
#
# CMP0135 is for solving re-building and re-downloading.
# We don't have a real problem with the OLD behavior for now
# but we use the NEW behavior explicitly to suppress CMP0135
# warnings.
if(POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
endif()

if(NOT DEFINED WITH_TESTS)
  set(WITH_TESTS "OFF")
endif()

if(NOT DEFINED APPLY_CLANG_FORMAT)
  option(APPLY_CLANG_FORMAT "execute clang-format before building redis-server" ON)
endif()

set(CMAKE_C_STANDARD 99)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_INSTALL_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/output")

# Set default build type to Debug
if(NOT DEFINED CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

# Set compiler flags based on build type
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O2")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2")
elseif(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0")
endif()

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -g -ggdb -D_GNU_SOURCE -Werror=implicit-function-declaration -march=native")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -g -ggdb --std=c++17 -march=native")

message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
message(STATUS "CMAKE_C_STANDARD: ${CMAKE_C_STANDARD}")
message(STATUS "CMAKE_C_FLAGS: ${CMAKE_C_FLAGS}")
message(STATUS "CMAKE_CXX_STANDARD: ${CMAKE_CXX_STANDARD}")
message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")
message(STATUS "CMAKE_INSTALL_PREFIX: ${CMAKE_INSTALL_PREFIX}")
message(STATUS "CMAKE_C_COMPILER_")
message(STATUS " ID       \t: ${CMAKE_C_COMPILER_ID}")
message(STATUS " Version  \t: ${CMAKE_C_COMPILER_VERSION}")
message(STATUS " Path     \t: ${CMAKE_C_COMPILER}")
message(STATUS "CMAKE_CXX_COMPILER_")
message(STATUS " ID       \t: ${CMAKE_CXX_COMPILER_ID}")
message(STATUS " Version  \t: ${CMAKE_CXX_COMPILER_VERSION}")
message(STATUS " Path     \t: ${CMAKE_CXX_COMPILER}")
message(STATUS "WIN32 : ${WIN32}")
message(STATUS "APPLE : ${APPLE}")
message(STATUS "UNIX : ${UNIX}")
message(STATUS "CMAKE_SYSTEM_NAME : ${CMAKE_SYSTEM_NAME}")

include(deps/CMakeLists.txt)

# Create release header files(release.c/.h)
execute_process(COMMAND sh mkreleasehdr.sh WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/src)

# List of source files for redis-server, derived from the object files mentioned
set(REDIS_SERVER_SOURCES
    src/acl.c
    src/adlist.c
    src/ae.c
    src/anet.c
    src/aof.c
    src/bio.c
    src/bitops.c
    src/blocked.c
    src/call_reply.c
    src/childinfo.c
    src/cluster.c
    src/commands.c
    src/config.c
    src/connection.c
    src/crc16.c
    src/crc64.c
    src/crcspeed.c
    src/db.c
    src/debug.c
    src/defrag.c
    src/dict.c
    src/endianconv.c
    src/eval.c
    src/evict.c
    src/expire.c
    src/function_lua.c
    src/functions.c
    src/geo.c
    src/geohash.c
    src/geohash_helper.c
    src/hyperloglog.c
    src/intset.c
    src/latency.c
    src/lazyfree.c
    src/listpack.c
    src/localtime.c
    src/logreqres.c
    src/lolwut.c
    src/lolwut5.c
    src/lolwut6.c
    src/lzf_c.c
    src/lzf_d.c
    src/memtest.c
    src/module.c
    src/monotonic.c
    src/mt19937-64.c
    src/multi.c
    src/networking.c
    src/notify.c
    src/object.c
    src/pqsort.c
    src/pubsub.c
    src/quicklist.c
    src/rand.c
    src/rax.c
    src/rdb.c
    src/redis-check-aof.c
    src/redis-check-rdb.c
    src/release.c
    src/release.c
    src/release.h
    src/replication.c
    src/resp_parser.c
    src/rio.c
    src/script.c
    src/script_lua.c
    src/sds.c
    src/sentinel.c
    src/server.c
    src/setcpuaffinity.c
    src/setproctitle.c
    src/sha1.c
    src/sha256.c
    src/siphash.c
    src/slowlog.c
    src/socket.c
    src/sort.c
    src/sparkline.c
    src/strl.c
    src/syncio.c
    src/syscheck.c
    src/t_hash.c
    src/t_list.c
    src/t_set.c
    src/t_stream.c
    src/t_string.c
    src/t_zset.c
    src/timeout.c
    src/tls.c
    src/tracking.c
    src/unix.c
    src/util.c
    src/ziplist.c
    src/zipmap.c
    src/zmalloc.c
)

set(VDB_SOURCES
  src/vdb/vdb.hh
  src/vdb/vdb_api.cc
  src/vdb/vdb_api.hh
  src/vdb/vdb_command_impl.cc
  src/vdb/data/checker.cc
  src/vdb/data/checker.hh
  src/vdb/common/defs.hh
  src/vdb/common/memory_allocator.cc
  src/vdb/common/memory_allocator.hh
  src/vdb/common/spinlock.cc
  src/vdb/common/spinlock.hh
  src/vdb/common/status.hh
  src/vdb/common/status.cc
  src/vdb/common/system_log.hh
  src/vdb/common/util.cc
  src/vdb/common/util.hh
  src/vdb/compute/execution.cc
  src/vdb/compute/execution.hh
  src/vdb/data/expression.cc
  src/vdb/data/expression.hh
  src/vdb/data/filter.cc
  src/vdb/data/filter.hh
  src/vdb/data/index_handler.cc
  src/vdb/data/index_handler.hh
  src/vdb/data/label_info.hh
  src/vdb/data/metadata.hh
  src/vdb/data/mutable_array.hh
  src/vdb/data/table.cc
  src/vdb/data/table.hh
  src/vdb/tests/util_for_test.hh
  src/vdb/tests/util_for_test.cc
)

set(HSNWLIB_SOURCES
  deps/hnswlib/hnswlib/bruteforce.h
  deps/hnswlib/hnswlib/hnswalg.h
  deps/hnswlib/hnswlib/hnswlib.h
  deps/hnswlib/hnswlib/space_ip.h
  deps/hnswlib/hnswlib/space_l2.h
  deps/hnswlib/hnswlib/stop_condition.h
  deps/hnswlib/hnswlib/visitied_list_pool.h
)

include_directories(${CMAKE_INSTALL_PREFIX}/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src)

# Add executable target for redis-server
add_executable(redis-server ${REDIS_SERVER_SOURCES} ${VDB_SOURCES} ${HNSWLIB_SOURCES})

# `add_dependencies()` enforces the order of compilations
add_dependencies(redis-server jemalloc_lib)

# add pthread into redis-server using 'target_compile_options'
target_compile_options(redis-server PRIVATE -pthread)

# Link necessary libraries for redis-server
target_link_libraries(redis-server pthread hiredis lua hdr_histogram linenoise fpconv jemalloc_lib Arrow::arrow_shared ArrowDataset::arrow_dataset_shared)

add_library(redis-lib ${REDIS_SERVER_SOURCES} ${VDB_SOURCES} ${HNSWLIB_SOURCES})
target_link_libraries(redis-lib hiredis lua hdr_histogram linenoise fpconv jemalloc_lib Arrow::arrow_shared ArrowDataset::arrow_dataset_shared)

# List of source files for redis-cli, derived from the object files mentioned
set(REDIS_CLI_SOURCES
    src/adlist.c
    src/ae.c
    src/anet.c
    src/cli_commands.c
    src/cli_common.c
    src/crc16.c
    src/crc64.c
    src/crcspeed.c
    src/dict.c
    src/monotonic.c
    src/mt19937-64.c
    src/redis-cli.c
    src/redisassert.c
    src/release.c
    src/release.h
    src/siphash.c
    src/strl.c
    src/zmalloc.c
)

# Add executable target for redis-cli
add_executable(redis-cli ${REDIS_CLI_SOURCES})
# Link necessary libraries for redis-cli
target_link_libraries(redis-cli hiredis linenoise m)

if (NOT WITH_TESTS STREQUAL "OFF")
  function(add_test test_name test_path)
    add_executable(${test_name} ${test_path})
    if (WITH_TESTS STREQUAL "DEBUG")
      target_compile_definitions(${test_name} PRIVATE _DEBUG_GTEST)
    endif()
    target_link_libraries(${test_name} GTest::gtest_main redis-lib hiredis lua hdr_histogram linenoise fpconv jemalloc_lib Arrow::arrow_shared ArrowDataset::arrow_dataset_shared)
    set_target_properties(${test_name} PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/tests")
    add_custom_command(
        TARGET ${test_name} POST_BUILD
        COMMENT "Running ${test_name}, ignore failures"
        COMMAND ${CMAKE_BINARY_DIR}/tests/${test_name} || true
    )
  endfunction()

  add_test("data_structure_tests" src/vdb/tests/data_structure_tests.cc)
  add_test("command_tests" src/vdb/tests/command_tests.cc)
  add_test("util_tests" src/vdb/tests/util_tests.cc)
  add_test("ann_tests" src/vdb/tests/ann_tests.cc)
  add_test("snapshot_tests" src/vdb/tests/snapshot_tests.cc)
  add_test("allocator_tests" src/vdb/tests/allocator_tests.cc)
endif()

if (APPLY_CLANG_FORMAT)
  add_custom_command(
      TARGET redis-server PRE_BUILD
      COMMENT "Executing clang-format before building"
      COMMAND clang-format -i --style=file `find ../src/vdb -type f`
  )
endif()
