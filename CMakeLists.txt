# Copyright (c) 2019 Harikrishnan (harikrishnan.prabakaran@gmail.com) Distributed under the MIT
# License. See accompanying file LICENSE.md or copy at http://opensource.org/licenses/MIT

cmake_minimum_required(VERSION 3.10)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)
set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(PROJECT_NAME InMemIndexes)

project(${PROJECT_NAME} C CXX)
set(PROJECT_PATH ${CMAKE_CURRENT_SOURCE_DIR})
set(${CMAKE_PROJECT_NAME}_VERSION_MAJOR 0)
set(${CMAKE_PROJECT_NAME}_VERSION_MINOR 0)
set(${CMAKE_PROJECT_NAME}_VERSION_PATCH 1)
set(
    PROJECT_VERSION
    "${${CMAKE_PROJECT_NAME}_VERSION_MAJOR}.${${CMAKE_PROJECT_NAME}_VERSION_MINOR}.${${CMAKE_PROJECT_NAME}_VERSION_PATCH}"
    )

set(LIB "${PROJECT_NAME}")
set(TEST "test_${PROJECT_NAME}")
set(BENCHMARK "bench_${PROJECT_NAME}")
set(RAND_INT_BENCH "bench_${PROJECT_NAME}_rand_int")
set(YCSB_BENCH "ycsb")

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

message(STATUS "Building ${CMAKE_BUILD_TYPE} version!")

include(AdditionalBuildTypes.cmake)
include(ProjectFiles.cmake)
include_directories(AFTER "include")

find_package(Threads)
find_package(benchmark CONFIG REQUIRED)
find_path(VCPKG_INCLUDE_DIR NAMES tsl gsl catch.hpp)

include_directories(AFTER ${VCPKG_INCLUDE_DIR})

if(NOT MSVC)
    add_compile_options("-Wall" "-pedantic")
endif(NOT MSVC)

add_library(${LIB} SHARED ${SRC})

add_subdirectory(src/benchmark)
add_subdirectory(test)

if(BUILD_COVERAGE_ANALYSIS)
    include(CodeCoverage.cmake)
    message(STATUS "Enabling Code Coverage")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fprofile-arcs -ftest-coverage")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
    set(COVERAGE_EXTRACT
        '${CMAKE_CURRENT_SOURCE_DIR}/include/*'
        '${CMAKE_CURRENT_SOURCE_DIR}/src/*'
        '${CMAKE_CURRENT_SOURCE_DIR}/test/*')
    setup_target_for_coverage(coverage "${TEST}" coverage)
endif(BUILD_COVERAGE_ANALYSIS)
