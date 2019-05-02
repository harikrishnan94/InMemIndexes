#pragma once

#include "art_dump.h"
#include "indexes/utils/Utils.h"

#include <algorithm>
#include <cassert>
#include <cinttypes>
#include <deque>
#include <optional>
#include <utility>

#define ART_DEBUG(expr)                                                        \
  do {                                                                         \
    if (Traits::DEBUG) {                                                       \
      expr;                                                                    \
    }                                                                          \
  } while (0)

#define ART_DEBUG_ASSERT(expr) ART_DEBUG(assert(expr))
#define ART_DEBUG_ONLY(expr) ((void)(expr))

namespace indexes::art {
struct art_traits_default {
  static constexpr bool DEBUG = false;
};

struct art_traits_debug : art_traits_default {
  static constexpr bool DEBUG = true;
};
}