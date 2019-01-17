add_executable(${BENCHMARK} ${BENCH_SRC})
target_link_libraries(${BENCHMARK} PRIVATE ${LIB})
target_link_libraries(${BENCHMARK} PRIVATE benchmark::benchmark_main jemalloc)

find_package(cxxopts CONFIG REQUIRED)

add_executable(${RAND_INT_BENCH} ${RAND_INT_BENCH_SRC})
target_include_directories(${RAND_INT_BENCH} PRIVATE ${BENCH_SRC_PATH})
target_link_libraries(${RAND_INT_BENCH}
                      PRIVATE ${LIB}
                              ${CMAKE_THREAD_LIBS_INIT}
                              cxxopts::cxxopts
                              jemalloc)

add_executable(${YCSB_BENCH} ${YCSB_SRC})
target_include_directories(${YCSB_BENCH} PRIVATE ${YCSB_SRC_PATH} ${BENCH_SRC_PATH})
target_link_libraries(${YCSB_BENCH} PRIVATE ${LIB} ${CMAKE_THREAD_LIBS_INIT} jemalloc)
