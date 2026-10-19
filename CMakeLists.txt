cmake_minimum_required(VERSION 3.16)
project(netcache LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()

# Sanitizers: cmake -DNETCACHE_SANITIZE=address,undefined   (or: thread)
set(NETCACHE_SANITIZE "" CACHE STRING "Comma-separated -fsanitize= list")
add_compile_options(-Wall -Wextra -Wpedantic -Wshadow)
if(NETCACHE_SANITIZE)
  add_compile_options(-fsanitize=${NETCACHE_SANITIZE} -fno-omit-frame-pointer)
  add_link_options(-fsanitize=${NETCACHE_SANITIZE})
endif()

find_package(Threads REQUIRED)

# Phase 1: pure HTTP subset (no sockets), shared by the proxy and the tests.
add_library(netcache_http STATIC src/http.cpp)
target_include_directories(netcache_http PUBLIC src)

add_executable(netcache src/main.cpp)
target_link_libraries(netcache PRIVATE netcache_http Threads::Threads)

enable_testing()
add_executable(http_test tests/http_test.cpp)
target_link_libraries(http_test PRIVATE netcache_http)
add_test(NAME http_unit COMMAND http_test)

add_test(NAME integration
         COMMAND python3 ${CMAKE_SOURCE_DIR}/tests/integration_test.py $<TARGET_FILE:netcache>)
