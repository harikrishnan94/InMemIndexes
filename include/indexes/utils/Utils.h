#pragma once

#include <inttypes.h>

#if defined(__APPLE__) || defined(__linux__)
#define _RESTRICT __restrict__
#endif
#if _WIN32
#define _RESTRICT __restrict
#endif

namespace indexes::utils {
static constexpr int leading_zeroes(uint64_t val) {
#if defined(__LINUX__) || defined(__APPLE__)
#define leading_zeroes_impl(x) __builtin_clzl(x)
#endif /* __LINUX__ */

#ifdef _WIN32
#include <intrin.h>
#define leading_zeroes_impl(x) __lzcnt64(x)
#endif /* _WIN32 */

  return leading_zeroes_impl(val);

#undef leading_zeroes_impl
}

static constexpr int leading_zeroes(uint32_t val) {
#if defined(__LINUX__) || defined(__APPLE__)
#define leading_zeroes_impl(x) __builtin_clz(x)
#endif /* __LINUX__ */

#ifdef _WIN32
#include <intrin.h>
#define leading_zeroes_impl(x) __lzcnt(x)
#endif /* _WIN32 */

  return leading_zeroes_impl(val);

#undef leading_zeroes_impl
}
} // namespace indexes::utils
