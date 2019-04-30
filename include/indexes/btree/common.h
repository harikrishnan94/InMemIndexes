/*
 * include/indexes/btree/common.h
 * 	Contains common includes and macros b/w map and concurrent_map
 */

#pragma once

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cinttypes>
#include <cstddef>
#include <deque>
#include <functional>
#include <iterator>
#include <memory>
#include <optional>
#include <ostream>
#include <tuple>
#include <type_traits>

#include "btree_dump.h"

#define BTREE_DEBUG(expr)                                                      \
  do {                                                                         \
    if (Traits::DEBUG) {                                                       \
      expr;                                                                    \
    }                                                                          \
  } while (0)

#define BTREE_DEBUG_ASSERT(expr) BTREE_DEBUG(assert(expr))
#define BTREE_DEBUG_ONLY(expr) ((void)(expr))
#define PASTER(x, y) x##y
#define CONCAT(x, y) PASTER(x, y)
#define BTREE_UPDATE_STAT_NODE_BASED(stat)                                     \
  do {                                                                         \
    if constexpr (Node::IsInner())                                             \
      BTREE_UPDATE_STAT(CONCAT(inner_, stat), ++);                             \
    else                                                                       \
      BTREE_UPDATE_STAT(CONCAT(leaf_, stat), ++);                              \
  } while (0)

#define BTREE_UPDATE_STAT(stat, op)                                            \
  do {                                                                         \
    if constexpr (Traits::STAT) {                                              \
      this->m_stats->CONCAT(CONCAT(num_, stat), s) op;                         \
    }                                                                          \
  } while (0)

#define ASLEAF(n) static_cast<leaf_node_t *>(n)
#define ASINNER(n) static_cast<inner_node_t *>(n)
#define INNER_ONLY                                                             \
  template <typename Dummy = void,                                             \
            typename = typename std::enable_if_t<IsInner(), Dummy>>
#define LEAF_ONLY                                                              \
  template <typename Dummy = void,                                             \
            typename = typename std::enable_if_t<IsLeaf(), Dummy>>

namespace indexes::btree {
struct btree_traits_default {
  static constexpr int NODE_SIZE = 8 * 1024;
  static constexpr int NODE_MERGE_THRESHOLD = 20;
  static constexpr bool DEBUG = false;
  static constexpr bool STAT = false;
};

struct btree_traits_debug : btree_traits_default {
  static constexpr bool DEBUG = true;
  static constexpr bool STAT = true;
};

struct btree_stats_t {
  std::atomic<size_t> num_elements;
  std::atomic<size_t> num_leaf_splits;
  std::atomic<size_t> num_inner_splits;
  std::atomic<size_t> num_leaf_trims;
  std::atomic<size_t> num_inner_trims;
  std::atomic<size_t> num_leaf_merges;
  std::atomic<size_t> num_inner_merges;

  std::atomic<size_t> num_pessimistic_reads;
  std::atomic<size_t> num_optimistic_fails;
  std::atomic<size_t> num_retrys;

  void dump(std::ostream &ostr) const {
    ostr << "Num Leaf Splits = " << num_leaf_splits << "\n";
    ostr << "Num Inner Splits = " << num_inner_splits << "\n";
    ostr << "Num Leaf Trims = " << num_leaf_trims << "\n";
    ostr << "Num Inner Trims = " << num_inner_trims << "\n";
    ostr << "Num Leaf Merges = " << num_leaf_merges << "\n";
    ostr << "Num Inner Merges = " << num_inner_merges << "\n";

    ostr << "Num Pessimistic Reads = " << num_pessimistic_reads << "\n";
    ostr << "Num Optimistic Fails = " << num_optimistic_fails << "\n";
    ostr << "Num Retries = " << num_retrys << "\n";
  }
};

struct btree_empty_stats_t {};
} // namespace indexes::btree
