add_executable(${TEST} ${TEST_SRC})
target_link_libraries(${TEST} PRIVATE ${LIB} ${CMAKE_THREAD_LIBS_INIT} unofficial::abseil::hash)

enable_testing()
add_test(NAME ${TEST} COMMAND ${TEST})
