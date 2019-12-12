#include "indexes/btree/concurrent_map.h"
#include "indexes/btree/key.h"
#include "sha512.h"
#include "testConcurrentMapUtils.h"

#include <doctest/doctest.h>
#include <gsl/span>
#include <tsl/robin_set.h>

#include <limits>
#include <map>
#include <random>
#include <string>
#include <thread>
#include <vector>

struct btree_small_page_traits : indexes::btree::btree_traits_debug {
  static constexpr int NODE_SIZE = 256;
  static constexpr int NODE_MERGE_THRESHOLD = 80;
};

struct btree_traits_string_key : indexes::btree::btree_traits_debug {
  static constexpr int NODE_SIZE = 448;
  static constexpr int NODE_MERGE_THRESHOLD = 80;
};

struct btree_medium_page_traits : indexes::btree::btree_traits_default {
  static constexpr int NODE_SIZE = 384;
  static constexpr int NODE_MERGE_THRESHOLD = 50;
  static constexpr bool STAT = true;
};

using Key = indexes::btree::compound_key<int, int, int>;
using PartKey = indexes::btree::compound_key<int>;
using range_kind = indexes::btree::range_kind;

template class indexes::btree::concurrent_map<Key, int,
                                              btree_small_page_traits>;
template class indexes::btree::concurrent_map<std::string, int,
                                              btree_traits_string_key>;

static Key gen_key(std::uniform_int_distribution<int> &dist,
                   std::mt19937 &rnd) {
  return Key{dist(rnd), dist(rnd), dist(rnd)};
}

static PartKey gen_part_key(std::uniform_int_distribution<int> &dist,
                            std::mt19937 &rnd) {
  return PartKey{dist(rnd)};
};

TEST_SUITE_BEGIN("concurrent_btree");

TEST_CASE("BtreeConcurrentMapBasic") {
  indexes::utils::ThreadRegistry::RegisterThread();

  indexes::btree::concurrent_map<Key, int, btree_small_page_traits> map;

  constexpr auto num_keys = 100000;

  std::uniform_int_distribution<int> udist{1, num_keys};
  std::random_device r;
  std::seed_seq seed{r(), r(), r(), r(), r(), r(), r(), r()};
  std::mt19937 rnd(seed);

  auto min_key = [] {
    auto min = std::numeric_limits<int>::min();
    return Key{min, min, min};
  };
  auto max_key = [] {
    auto max = std::numeric_limits<int>::max();
    return Key{max, max, max};
  };

  std::map<Key, int> key_values;

  for (int i = 0; i < num_keys; i++) {
    auto key = gen_key(udist, rnd);

    map.Upsert(key, i);
    key_values[key] = i;
  }

  REQUIRE(map.size() == key_values.size());

  // Forward iterator
  {
    auto map_iter = map.lower_bound(min_key());
    auto kv_iter = key_values.lower_bound(min_key());
    auto map_iter_end = map.upper_bound(max_key());
    auto kv_iter_end = key_values.upper_bound(max_key());

    while (map_iter != map_iter_end) {
      REQUIRE(*map_iter++ == *kv_iter++);
    }

    REQUIRE(kv_iter == kv_iter_end);

    auto iter = map.begin();
    auto iter_copy = iter++;

    REQUIRE(--iter == iter_copy);
    REQUIRE(iter != ++iter_copy);
    REQUIRE(iter != map.end());
  }

  // reverse iterator
  {
    auto map_iter = map.rbegin();
    auto kv_iter = key_values.rbegin();

    while (map_iter != map.rend()) {
      REQUIRE(*map_iter++ == *kv_iter++);
    }

    REQUIRE(kv_iter == key_values.rend());

    auto iter = map.rbegin();
    auto iter_copy = iter++;

    REQUIRE(--iter == iter_copy);
    REQUIRE(iter != ++iter_copy);
    REQUIRE(iter != map.rend());
  }

  for (int i = 0; i < num_keys; i++) {
    auto key = gen_part_key(udist, rnd);
    auto map_lower = map.lower_bound(key);
    auto map_upper = map.upper_bound(key);

    if (map_lower == map.end() || map_upper == map.end()) {
      REQUIRE(key >= map.crbegin()->first);
      continue;
    }

    auto kv_lower = key_values.find(map_lower->first);
    auto kv_upper = key_values.find(map_upper->first);

    REQUIRE(kv_lower != key_values.end());
    REQUIRE(kv_upper != key_values.end());

    while (map_lower != map_upper) {
      REQUIRE(*map_lower++ == *kv_lower++);
    }
    REQUIRE(kv_lower == kv_upper);
  }

  for (const auto &kv : key_values) {
    REQUIRE(*map.Delete(kv.first) == kv.second);
  }

  REQUIRE(map.size() == 0);
  REQUIRE(map.Search(Key{0, 0, 0}).has_value() == false);

  // lower and upper bound
  {
    auto key = gen_key(udist, rnd);

    map.Insert(key, std::get<0>(key));
    REQUIRE(map.lower_bound(key) != map.upper_bound(key));

    map.Delete(key);
    REQUIRE(map.lower_bound(key) == map.upper_bound(key));

    key = min_key();
    REQUIRE(map.lower_bound(key) == map.upper_bound(key));

    key = max_key();
    REQUIRE(map.lower_bound(key) == map.upper_bound(key));
  }

  // Iterator operator
  {
    auto key = gen_key(udist, rnd);

    map.Insert(key, std::get<0>(key));
    REQUIRE((*map.lower_bound(key)).first == map.lower_bound(key)->first);
  }

  indexes::utils::ThreadRegistry::UnregisterThread();
}

class RangeIterTester {
public:
  enum { FORWARD, REVERSE };
  void Prep() {
    for (int i = 0; i < NUMKEYS; i++) {
      auto key = gen_key(udist, rnd);

      map.Upsert(key, i);
      key_values[key] = i;
    }
  }

  template <int Dir, range_kind LRK, range_kind RRK> void Run() {
    for (int i = 0; i < NUM_RANGES; i++) {
      Key min, max;
      std::tie(min, max) = gen_range();

      auto range_it = [&] {
        if constexpr (Dir == FORWARD)
          return map.range_iter<LRK, RRK>(min, max);
        else
          return map.rrange_iter<LRK, RRK>(min, max);
      }();

      if (!range_it) {
        auto last = map.rbegin()->first;
        REQUIRE((last < max || last > min) == true);
        continue;
      }

      auto kv_it = key_values.find(range_it->first);
      check_iter<Dir, LRK, RRK>(range_it, kv_it, min, max);
    }
  }

private:
  template <int Dir, range_kind LRK, range_kind RRK, typename RangeIT,
            typename KVIT, typename Key1T, typename Key2T>
  static void check_iter(RangeIT map_it, KVIT kv_iter, const Key1T &min,
                         const Key2T &max) {
    if (!map_it)
      return;

    check_iter_start<Dir, LRK, RRK>(map_it->first, min, max);

    Key last_key;
    do {
      auto &map_p = *map_it;
      auto &kv_p = *kv_iter;

      REQUIRE(map_p == kv_p);
      last_key = map_p.first;

      if constexpr (Dir == FORWARD)
        ++kv_iter;
      else
        --kv_iter;
    } while (++map_it);

    check_iter_end<Dir, LRK, RRK>(last_key, min, max);
  }

  template <int Dir, range_kind LRK, range_kind RRK, typename Key1T,
            typename Key2T>
  static void check_iter_start(const Key &itkey, const Key1T &min,
                               const Key2T &max) {
    if constexpr (Dir == FORWARD) {
      if constexpr (LRK == range_kind::INCLUSIVE)
        REQUIRE(itkey == min);
      else
        REQUIRE(itkey > min);
    } else {
      if constexpr (RRK == range_kind::INCLUSIVE)
        REQUIRE(itkey == max);
      else
        REQUIRE(itkey < max);
    }
  }
  template <int Dir, range_kind LRK, range_kind RRK, typename Key1T,
            typename Key2T>
  static void check_iter_end(const Key &itkey, const Key1T &min,
                             const Key2T &max) {
    if constexpr (Dir == FORWARD) {
      if constexpr (RRK == range_kind::INCLUSIVE)
        REQUIRE(itkey == max);
      else
        REQUIRE(itkey < max);
    } else {
      if constexpr (LRK == range_kind::INCLUSIVE)
        REQUIRE(itkey == min);
      else
        REQUIRE(itkey > min);
    }
  }

  std::pair<Key, Key> gen_range() {
    while (true) {
      auto min = gen_part_key(udist, rnd);
      auto min_it = map.lower_bound(min);

      if (min_it == map.end())
        continue;

      auto max = min;
      auto range = rdist(rnd);
      std::get<0>(max) += range;
      auto max_it = map.lower_bound(max);

      if (max_it == map.end())
        continue;

      return {min_it->first, max_it->first};
    }
  }

  static constexpr auto NUMKEYS = 100000;
  static constexpr auto ITER_RANGE = 1000;
  static constexpr auto NUM_RANGES = 1000;
  std::uniform_int_distribution<int> udist{1, NUMKEYS};
  std::uniform_int_distribution<int> rdist{1, ITER_RANGE};
  std::random_device r;
  std::seed_seq seed{r(), r(), r(), r(), r(), r(), r(), r()};
  std::mt19937 rnd{seed};

  indexes::btree::concurrent_map<Key, int, btree_small_page_traits> map;
  std::map<Key, int> key_values;
};

TEST_CASE("BtreeConcurrentMapRangeIter") {
  indexes::utils::ThreadRegistry::RegisterThread();

  RangeIterTester test;

  test.Prep();

  // FORWARD ITERATION
  SUBCASE("Forward - INCLUSIVE, INCLUSIVE") {
    test.Run<RangeIterTester::FORWARD, range_kind::INCLUSIVE,
             range_kind::INCLUSIVE>();
  }
  SUBCASE("Forward - INCLUSIVE, EXCLUSIVE") {
    test.Run<RangeIterTester::FORWARD, range_kind::INCLUSIVE,
             range_kind::EXCLUSIVE>();
  }
  SUBCASE("Forward - EXCLUSIVE, INCLUSIVE") {
    test.Run<RangeIterTester::FORWARD, range_kind::EXCLUSIVE,
             range_kind::INCLUSIVE>();
  }
  SUBCASE("Forward - EXCLUSIVE, EXCLUSIVE") {
    test.Run<RangeIterTester::FORWARD, range_kind::EXCLUSIVE,
             range_kind::EXCLUSIVE>();
  }

  // REVERSE ITERATION
  SUBCASE("Reverse - INCLUSIVE, INCLUSIVE") {
    test.Run<RangeIterTester::REVERSE, range_kind::INCLUSIVE,
             range_kind::INCLUSIVE>();
  }
  SUBCASE("Reverse - INCLUSIVE, EXCLUSIVE") {
    test.Run<RangeIterTester::REVERSE, range_kind::INCLUSIVE,
             range_kind::EXCLUSIVE>();
  }
  SUBCASE("Reverse - EXCLUSIVE, INCLUSIVE") {
    test.Run<RangeIterTester::REVERSE, range_kind::EXCLUSIVE,
             range_kind::INCLUSIVE>();
  }
  SUBCASE("Reverse - EXCLUSIVE, EXCLUSIVE") {
    test.Run<RangeIterTester::REVERSE, range_kind::EXCLUSIVE,
             range_kind::EXCLUSIVE>();
  }

  indexes::utils::ThreadRegistry::UnregisterThread();
}

TEST_CASE("BtreeConcurrentMapString") {
  indexes::utils::ThreadRegistry::RegisterThread();
  indexes::btree::concurrent_map<std::string, int, btree_traits_string_key> map;

  int num_keys = 100000;

  std::uniform_int_distribution<int> udist;
  std::random_device r;
  std::seed_seq seed{r(), r(), r(), r(), r(), r(), r(), r()};
  std::mt19937 rnd(seed);

  std::map<std::string, int> key_values;

  for (int i = 0; i < num_keys; i++) {
    std::string key = sha512(std::to_string(udist(rnd)));

    map.Upsert(key, i);
    key_values[key] = i;
  }

  REQUIRE(map.size() == key_values.size());

  // Forward iterator
  {
    auto map_iter = map.begin();
    auto kv_iter = key_values.begin();
    auto map_iter_end = map.end();
    auto kv_iter_end = key_values.end();

    while (map_iter != map_iter_end) {
      REQUIRE(*map_iter++ == *kv_iter++);
    }

    REQUIRE(kv_iter == kv_iter_end);

    auto iter = map.begin();
    auto iter_copy = iter++;

    REQUIRE(--iter == iter_copy);
    REQUIRE(iter != ++iter_copy);
    REQUIRE(iter != map.end());
  }

  // reverse iterator
  {
    auto map_iter = map.rbegin();
    auto kv_iter = key_values.rbegin();

    while (map_iter != map.rend()) {
      REQUIRE(*map_iter++ == *kv_iter++);
    }

    REQUIRE(kv_iter == key_values.rend());

    auto iter = map.rbegin();
    auto iter_copy = iter++;

    REQUIRE(--iter == iter_copy);
    REQUIRE(iter != ++iter_copy);
    REQUIRE(iter != map.rend());
  }

  for (int i = 0; i < num_keys; i++) {
    std::string key = sha512(std::to_string(udist(rnd)));
    auto map_lower = map.lower_bound(key);
    auto map_upper = map.upper_bound(key);
    auto kv_lower = key_values.lower_bound(key);
    auto kv_upper = key_values.upper_bound(key);

    if (kv_lower != key_values.end())
      REQUIRE(*map_lower == *kv_lower);
    else
      REQUIRE(map_lower == map.end());

    if (kv_upper != key_values.end())
      REQUIRE(*map_upper == *kv_upper);
    else
      REQUIRE(map_upper == map.end());
  }

  for (const auto &kv : key_values) {
    REQUIRE(*map.Delete(kv.first) == kv.second);
  }

  REQUIRE(map.size() == 0);
  REQUIRE(map.Search("").has_value() == false);

  indexes::utils::ThreadRegistry::UnregisterThread();
}

TEST_CASE("BtreeConcurrentMapMixed") {
  MixedMapTest<
      indexes::btree::concurrent_map<int, int, btree_small_page_traits>>();
}

using Btree =
    indexes::btree::concurrent_map<int64_t, int64_t, btree_medium_page_traits>;
static void range_scan(Btree &map, int64_t min, int64_t max, size_t count) {
  constexpr auto RANGE = 100;
  std::uniform_int_distribution<int64_t> vdist{min, max};
  std::uniform_int_distribution<int> rdist{1, RANGE};
  std::random_device r;
  std::seed_seq seed{r(), r(), r(), r(), r(), r(), r(), r()};
  std::mt19937 rnd{seed};
  std::vector<int64_t> vals;

  for (size_t i = 0; i < count / RANGE; i++) {
    auto min_val = vdist(rnd);
    auto max_val = min_val + rdist(rnd);
    auto it = map.range_iter<range_kind::INCLUSIVE, range_kind::INCLUSIVE>(
        min_val, max_val);
    while (it) {
      vals.push_back(it->first);
      ++it;
    }
    REQUIRE(std::is_sorted(vals.begin(), vals.end()));
    vals.clear();
  }
}

TEST_CASE("BtreeConcurrentMapConcurrencyRandom") {
  using Btree = indexes::btree::concurrent_map<int64_t, int64_t,
                                               btree_medium_page_traits>;
  ConcurrentMapTest<Btree, LookupType::LT_CUSTOM>(
      ConcurrentMapTestWorkload::WL_RANDOM, range_scan);
}

TEST_CASE("BtreeConcurrentMapConcurrencyContented") {
  using Btree = indexes::btree::concurrent_map<int64_t, int64_t,
                                               btree_medium_page_traits>;
  ConcurrentMapTest<Btree, LookupType::LT_CUSTOM>(
      ConcurrentMapTestWorkload::WL_CONTENTED, range_scan);
}

TEST_SUITE_END();