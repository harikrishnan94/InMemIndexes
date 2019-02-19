/*
 * include/indexes/hashtable/common.h
 * 	Contains common includes and macros b/w map and concurrent_map
 */

#pragma once

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cinttypes>
#include <cstddef>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <ostream>
#include <type_traits>
#include <utility>

#define HT_DEBUG(expr)     \
	do                     \
	{                      \
		if (Traits::DEBUG) \
		{                  \
			expr;          \
		}                  \
	} while (0)

#define HT_DEBUG_ASSERT(expr) HT_DEBUG(assert(expr))
#define HT_DEBUG_ONLY(expr) ((void) (expr))

namespace indexes::hashtable
{
struct hashtable_traits_default
{
	static constexpr bool DEBUG                  = false;
	static constexpr uint8_t LINEAR_SEARCH_LIMIT = std::numeric_limits<uint8_t>::max();
};

struct hashtable_traits_debug : hashtable_traits_default
{
	static constexpr bool DEBUG = true;
};

} // namespace indexes::hashtable