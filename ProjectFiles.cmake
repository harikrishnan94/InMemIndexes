# Copyright (c) 2018 Harikrishnan (harikrishnan.prabakaran@gmail.com) Distributed under the MIT
# License. See accompanying file LICENSE.md or copy at http://opensource.org/licenses/MIT

set(SRC_PATH "${PROJECT_PATH}/src")
set(SRC_UTILS_PATH "${SRC_PATH}/utils")
set(BENCH_SRC_PATH "${SRC_PATH}/benchmark")
set(YCSB_SRC_PATH "${BENCH_SRC_PATH}/ycsb")
set(TEST_SRC_PATH "${PROJECT_PATH}/test")

# Set library source files.
set(SRC
    "${SRC_UTILS_PATH}/Mutex.cpp"
    "${SRC_UTILS_PATH}/ParkingLot.cpp"
    "${SRC_UTILS_PATH}/ThreadRegistry.cpp"
    "${SRC_UTILS_PATH}/TraceLog.cpp")

# Set benchmark source files.
set(BENCH_SRC
    "${BENCH_SRC_PATH}/benchBtree.cpp"
    "${BENCH_SRC_PATH}/benchHashTable.cpp"
    "${BENCH_SRC_PATH}/benchART.cpp")
set(RAND_INT_BENCH_SRC "${BENCH_SRC_PATH}/randIntBench.cpp")
set(YCSB_SRC
    "${YCSB_SRC_PATH}/core/core_workload.cpp"
    "${YCSB_SRC_PATH}/db/db_factory.cpp"
    "${YCSB_SRC_PATH}/ycsb.cpp")

# Set project test source files.
set(TEST_SRC
    "${TEST_SRC_PATH}/testConcurrentART.cpp"
    "${TEST_SRC_PATH}/testART.cpp"
    "${TEST_SRC_PATH}/testHashMap.cpp"
    "${TEST_SRC_PATH}/testConcurrentMapUtils.cpp"
    "${TEST_SRC_PATH}/testBtreeConcurrentMap.cpp"
    "${TEST_SRC_PATH}/testBtreeMap.cpp"
    "${TEST_SRC_PATH}/testBase.cpp"
    "${TEST_SRC_PATH}/sha512.cpp")
