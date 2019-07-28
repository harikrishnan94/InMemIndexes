#pragma once

#include <inttypes.h>

#include "sync_prim/ThreadRegistry.h"

#if defined(_WIN32) && defined(_MSC_VER) && !defined(__clang__)
#include <intrin.h>
#pragma intrinsic(_BitScanReverse64)
#pragma intrinsic(_BitScanReverse)

static inline int __builtin_ctz(uint32_t x) {
  unsigned long ret;
  _BitScanForward(&ret, x);
  return (int)ret;
}

static inline int __builtin_ctzll(unsigned long long x) {
  unsigned long ret;
  _BitScanForward64(&ret, x);
  return (int)ret;
}

static inline int __builtin_ctzl(unsigned long x) {
  return sizeof(x) == 8 ? __builtin_ctzll(x) : __builtin_ctz((uint32_t)x);
}

static inline int __builtin_clz(uint32_t x) {
  unsigned long ret;
  _BitScanReverse(&ret, x);
  return (int)(31 ^ ret);
  // return (int)__lzcnt(x);
}

static inline int __builtin_clzll(unsigned long long x) {
  unsigned long ret;
  _BitScanReverse64(&ret, x);
  return (int)(63 ^ ret);
  // return (int)__lzcnt64(x);
}

static inline int __builtin_clzl(unsigned long x) {
  return sizeof(x) == 8 ? __builtin_clzll(x) : __builtin_clz((uint32_t)x);
}

#ifdef __cplusplus
static inline int __builtin_ctzl(unsigned long long x) {
  return __builtin_ctzll(x);
}

static inline int __builtin_clzl(unsigned long long x) {
  return __builtin_clzll(x);
}
#endif
#endif

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
