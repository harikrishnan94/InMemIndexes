add_executable(${BENCHMARK} ${BENCH_SRC})
target_link_libraries(${BENCHMARK} PRIVATE ${LIB})
target_link_libraries(${BENCHMARK} PRIVATE benchmark::benchmark_main)

add_executable(${YCSB_BENCH} ${YCSB_SRC})
target_include_directories(${YCSB_BENCH} PRIVATE ${YCSB_SRC_PATH})
target_link_libraries(${YCSB_BENCH} PRIVATE ${LIB})
