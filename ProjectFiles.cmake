# Copyright (c) 2018 Harikrishnan (harikrishnan.prabakaran@gmail.com) Distributed under the MIT
# License. See accompanying file LICENSE.md or copy at http://opensource.org/licenses/MIT

set(SRC_PATH "${PROJECT_PATH}/src")
set(BENCH_SRC_PATH "${SRC_PATH}/benchmark")
set(YCSB_SRC_PATH "${BENCH_SRC_PATH}/ycsb")
set(TEST_SRC_PATH "${PROJECT_PATH}/test")

# Set library source files.
set(SRC "${SRC_PATH}/btree.cpp")

# Set benchmark source files.
set(BENCH_SRC "${BENCH_SRC_PATH}/bench.cpp")
set(YCSB_SRC
    "${YCSB_SRC_PATH}/core/core_workload.cpp"
    "${YCSB_SRC_PATH}/db/db_factory.cpp"
    "${YCSB_SRC_PATH}/ycsb.cpp")

# Set project test source files.
set(TEST_SRC
    # "${TEST_SRC_PATH}/*.cpp"
    "${TEST_SRC_PATH}/testBtreeMap.cpp" "${TEST_SRC_PATH}/testBase.cpp"
    "${TEST_SRC_PATH}/sha512.cpp")
