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

# Phase 2A/2B: bounded LRU cache + single-flight coalescing (no HTTP, no sockets).
add_library(netcache_cache STATIC src/cache.cpp src/coalescing_cache.cpp)
target_include_directories(netcache_cache PUBLIC src)
target_link_libraries(netcache_cache PUBLIC Threads::Threads)

# Phase 2B: cache eligibility rules (pure functions over the Phase 1 HTTP types).
add_library(netcache_policy STATIC src/cache_policy.cpp)
target_link_libraries(netcache_policy PUBLIC netcache_http)

add_executable(netcache src/main.cpp)
target_link_libraries(netcache PRIVATE netcache_http netcache_cache netcache_policy Threads::Threads)

enable_testing()
add_executable(http_test tests/http_test.cpp)
target_link_libraries(http_test PRIVATE netcache_http)
add_test(NAME http_unit COMMAND http_test)

add_executable(cache_test tests/cache_test.cpp)
target_link_libraries(cache_test PRIVATE netcache_cache)
add_test(NAME cache_unit COMMAND cache_test)

add_executable(coalescing_test tests/coalescing_test.cpp)
target_link_libraries(coalescing_test PRIVATE netcache_cache)
add_test(NAME coalescing_unit COMMAND coalescing_test)

add_executable(cache_policy_test tests/cache_policy_test.cpp)
target_link_libraries(cache_policy_test PRIVATE netcache_policy)
add_test(NAME cache_policy_unit COMMAND cache_policy_test)

add_test(NAME integration
         COMMAND python3 ${CMAKE_SOURCE_DIR}/tests/integration_test.py $<TARGET_FILE:netcache>)
