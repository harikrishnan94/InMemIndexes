add_executable(${BENCHMARK} ${BENCH_SRC})
target_link_libraries(${BENCHMARK} PRIVATE ${LIB})
target_link_libraries(${BENCHMARK} PRIVATE benchmark::benchmark_main unofficial::abseil::hash)

find_package(Boost REQUIRED COMPONENTS system thread program_options)

message(STATUS "${Boost_INCLUDE_DIR}")

add_executable(${RAND_INT_BENCH} ${RAND_INT_BENCH_SRC})
target_include_directories(${RAND_INT_BENCH} PRIVATE ${BENCH_SRC_PATH} ${Boost_INCLUDE_DIR})
target_link_libraries(${RAND_INT_BENCH}
                      PRIVATE ${LIB}
                              ${CMAKE_THREAD_LIBS_INIT}
                              unofficial::abseil::hash
                              ${Boost_LIBRARIES})

add_executable(${YCSB_BENCH} ${YCSB_SRC})
target_include_directories(${YCSB_BENCH} PRIVATE ${YCSB_SRC_PATH} ${BENCH_SRC_PATH})
target_link_libraries(${YCSB_BENCH} PRIVATE ${LIB} ${CMAKE_THREAD_LIBS_INIT})
