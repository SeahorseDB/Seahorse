include(ExternalProject)

add_subdirectory(deps/fpconv)
add_subdirectory(deps/hdr_histogram)
add_subdirectory(deps/hiredis)
add_subdirectory(deps/linenoise)
add_subdirectory(deps/lua)

# Add directories for dependency headers
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/deps/fpconv)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/deps/hdr_histogram)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/deps/hiredis)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/deps/linenoise)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/deps/lua/src)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/deps/hnswlib)

# Jemalloc
ExternalProject_Add(jemalloc_ep
  SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/deps/jemalloc
  CONFIGURE_COMMAND ./autogen.sh COMMAND ./configure --disable-cxx --with-version=5.3.0-0-g0 --with-lg-quantum=3 --disable-cache-oblivious --with-jemalloc-prefix=je_ --prefix=${CMAKE_INSTALL_PREFIX}
  BUILD_IN_SOURCE 1
  INSTALL_COMMAND ${MAKE_EXE} install_lib
)
add_library(jemalloc_lib STATIC IMPORTED)
add_dependencies(jemalloc_lib jemalloc_ep)
ExternalProject_Get_Property(jemalloc_ep binary_dir)
set_target_properties(jemalloc_lib PROPERTIES
  IMPORTED_LOCATION ${CMAKE_INSTALL_PREFIX}/lib/libjemalloc.a)

find_package(Arrow REQUIRED)
find_package(ArrowDataset REQUIRED)

if (WITH_TESTS)
# GoogleTest requires at least C++14
  include(FetchContent)
  FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
  )
# For Windows: Prevent overriding the parent project's compiler/linker settings
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)
endif()
