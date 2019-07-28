#pragma once

#include <folly/portability/Builtins.h>
#include <inttypes.h>

#include "sync_prim/ThreadRegistry.h"

#if defined(__APPLE__) || defined(__linux__)
#define _RESTRICT __restrict__
#endif
#if _WIN32
#define _RESTRICT __restrict
#endif

#ifndef CHAR_BIT
// Lets hope this is true
#define CHAR_BIT 8
#endif

namespace indexes::utils {
static inline int leading_zeroes(uint64_t val) { return __builtin_clzl(val); }
static inline int leading_zeroes(uint32_t val) { return __builtin_clz(val); }
using ThreadRegistry = sync_prim::ThreadRegistry;
} // namespace indexes::utils
