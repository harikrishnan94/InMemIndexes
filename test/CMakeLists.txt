add_executable(${TEST} ${TEST_SRC})
target_link_libraries(${TEST} PRIVATE ${LIB})

enable_testing()
add_test(NAME ${TEST} COMMAND ${TEST})
