# DebWithAsan Build Type
set(CMAKE_CXX_FLAGS_DEBWITHASAN "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address"
    CACHE STRING "Flags used by the C++ compiler during DebWithAsan builds."
    FORCE)
set(CMAKE_C_FLAGS_DEBWITHASAN "${CMAKE_C_FLAGS_DEBUG} -fsanitize=address"
    CACHE STRING "Flags used by the C compiler during DebWithAsan builds."
    FORCE)
set(CMAKE_EXE_LINKER_FLAGS_DEBWITHASAN "${CMAKE_EXE_LINKER_FLAGS_DEBUG}"
    CACHE STRING "Flags used for linking binaries during DebWithAsan builds."
    FORCE)
set(CMAKE_SHARED_LINKER_FLAGS_DEBWITHASAN "${CMAKE_SHARED_LINKER_FLAGS_DEBUG}"
    CACHE STRING "Flags used by the shared libraries linker during DebWithAsan builds."
    FORCE)
mark_as_advanced(CMAKE_CXX_FLAGS_DEBWITHASAN
                 CMAKE_C_FLAGS_DEBWITHASAN
                 CMAKE_EXE_LINKER_FLAGS_DEBWITHASAN
                 CMAKE_SHARED_LINKER_FLAGS_DEBWITHASAN)

# DebWithTsan Build Type
set(CMAKE_CXX_FLAGS_DEBWITHTSAN "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=thread"
    CACHE STRING "Flags used by the C++ compiler during DebWithTsan builds."
    FORCE)
set(CMAKE_C_FLAGS_DEBWITHTSAN "${CMAKE_C_FLAGS_DEBUG} -fsanitize=thread"
    CACHE STRING "Flags used by the C compiler during DebWithTsan builds."
    FORCE)
set(CMAKE_EXE_LINKER_FLAGS_DEBWITHTSAN "${CMAKE_EXE_LINKER_FLAGS_DEBUG}"
    CACHE STRING "Flags used for linking binaries during DebWithTsan builds."
    FORCE)
set(CMAKE_SHARED_LINKER_FLAGS_DEBWITHTSAN "${CMAKE_SHARED_LINKER_FLAGS_DEBUG}"
    CACHE STRING "Flags used by the shared libraries linker during DebWithTsan builds."
    FORCE)
mark_as_advanced(CMAKE_CXX_FLAGS_DEBWITHTSAN
                 CMAKE_C_FLAGS_DEBWITHTSAN
                 CMAKE_EXE_LINKER_FLAGS_DEBWITHTSAN
                 CMAKE_SHARED_LINKER_FLAGS_DEBWITHTSAN)

# DebWithUBsan Build Type
set(CMAKE_CXX_FLAGS_DEBWITHUBSAN "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=undefined"
    CACHE STRING "Flags used by the C++ compiler during DebWithUBsan builds."
    FORCE)
set(CMAKE_C_FLAGS_DEBWITHUBSAN "${CMAKE_C_FLAGS_DEBUG} -fsanitize=undefined"
    CACHE STRING "Flags used by the C compiler during DebWithUBsan builds."
    FORCE)
set(CMAKE_EXE_LINKER_FLAGS_DEBWITHUBSAN "${CMAKE_EXE_LINKER_FLAGS_DEBUG}"
    CACHE STRING "Flags used for linking binaries during DebWithUBsan builds."
    FORCE)
set(CMAKE_SHARED_LINKER_FLAGS_DEBWITHUBSAN "${CMAKE_SHARED_LINKER_FLAGS_DEBUG}"
    CACHE STRING "Flags used by the shared libraries linker during DebWithUBsan builds."
    FORCE)
mark_as_advanced(CMAKE_CXX_FLAGS_DEBWITHUBSAN
                 CMAKE_C_FLAGS_DEBWITHUBSAN
                 CMAKE_EXE_LINKER_FLAGS_DEBWITHUBSAN
                 CMAKE_SHARED_LINKER_FLAGS_DEBWITHUBSAN)

# RelWithAsan Build Type
set(CMAKE_CXX_FLAGS_RELWITHASAN "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -fsanitize=address"
    CACHE STRING "Flags used by the C++ compiler during RelWithAsan builds."
    FORCE)
set(CMAKE_C_FLAGS_RELWITHASAN "${CMAKE_C_FLAGS_RELWITHDEBINFO} -fsanitize=address"
    CACHE STRING "Flags used by the C compiler during RelWithAsan builds."
    FORCE)
set(CMAKE_EXE_LINKER_FLAGS_RELWITHASAN "${CMAKE_EXE_LINKER_FLAGS_RELWITHDEBINFO}"
    CACHE STRING "Flags used for linking binaries during RelWithAsan builds."
    FORCE)
set(CMAKE_SHARED_LINKER_FLAGS_RELWITHASAN "${CMAKE_SHARED_LINKER_FLAGS_RELWITHDEBINFO}"
    CACHE STRING "Flags used by the shared libraries linker during RelWithAsan builds."
    FORCE)
mark_as_advanced(CMAKE_CXX_FLAGS_RELWITHASAN
                 CMAKE_C_FLAGS_RELWITHASAN
                 CMAKE_EXE_LINKER_FLAGS_RELWITHASAN
                 CMAKE_SHARED_LINKER_FLAGS_RELWITHASAN)

# RelWithTsan Build Type
set(CMAKE_CXX_FLAGS_RELWITHTSAN "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -fsanitize=thread"
    CACHE STRING "Flags used by the C++ compiler during RelWithTsan builds."
    FORCE)
set(CMAKE_C_FLAGS_RELWITHTSAN "${CMAKE_C_FLAGS_RELWITHDEBINFO} -fsanitize=thread"
    CACHE STRING "Flags used by the C compiler during RelWithTsan builds."
    FORCE)
set(CMAKE_EXE_LINKER_FLAGS_RELWITHTSAN "${CMAKE_EXE_LINKER_FLAGS_RELWITHDEBINFO}"
    CACHE STRING "Flags used for linking binaries during RelWithTsan builds."
    FORCE)
set(CMAKE_SHARED_LINKER_FLAGS_RELWITHTSAN "${CMAKE_SHARED_LINKER_FLAGS_RELWITHDEBINFO}"
    CACHE STRING "Flags used by the shared libraries linker during RelWithTsan builds."
    FORCE)
mark_as_advanced(CMAKE_CXX_FLAGS_RELWITHTSAN
                 CMAKE_C_FLAGS_RELWITHTSAN
                 CMAKE_EXE_LINKER_FLAGS_RELWITHTSAN
                 CMAKE_SHARED_LINKER_FLAGS_RELWITHTSAN)

# RelWithUBsan Build Type
set(CMAKE_CXX_FLAGS_RELWITHUBSAN "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -fsanitize=undefined"
    CACHE STRING "Flags used by the C++ compiler during RelWithUBsan builds."
    FORCE)
set(CMAKE_C_FLAGS_RELWITHUBSAN "${CMAKE_C_FLAGS_RELWITHDEBINFO} -fsanitize=undefined"
    CACHE STRING "Flags used by the C compiler during RelWithUBsan builds."
    FORCE)
set(CMAKE_EXE_LINKER_FLAGS_RELWITHUBSAN "${CMAKE_EXE_LINKER_FLAGS_RELWITHDEBINFO}"
    CACHE STRING "Flags used for linking binaries during RelWithUBsan builds."
    FORCE)
set(CMAKE_SHARED_LINKER_FLAGS_RELWITHUBSAN "${CMAKE_SHARED_LINKER_FLAGS_RELWITHDEBINFO}"
    CACHE STRING "Flags used by the shared libraries linker during RelWithUBsan builds."
    FORCE)
mark_as_advanced(CMAKE_CXX_FLAGS_RELWITHUBSAN
                 CMAKE_C_FLAGS_RELWITHUBSAN
                 CMAKE_EXE_LINKER_FLAGS_RELWITHUBSAN
                 CMAKE_SHARED_LINKER_FLAGS_RELWITHUBSAN)

# Update the documentation string of CMAKE_BUILD_TYPE for GUIs
set(
    CMAKE_BUILD_TYPE "${CMAKE_BUILD_TYPE}"
    CACHE
        STRING
        "Choose the type of build, options are: None Debug DebWithAsan DebWithTsan DebWithUBsan Release RelWithDebInfo MinSizeRel RelWithAsan RelWithTsan RelWithUBsan."
    FORCE)
