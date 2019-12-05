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
template class indexes::btree::concurrent_map<Key, int,
                                              btree_small_page_traits>;
template class indexes::btree::concurrent_map<std::string, int,
                                              btree_traits_string_key>;

TEST_SUITE_BEGIN("concurrent_btree");

TEST_CASE("BtreeConcurrentMapBasic") {
  indexes::utils::ThreadRegistry::RegisterThread();

  using PartKey = indexes::btree::compound_key<int>;
  indexes::btree::concurrent_map<Key, int, btree_small_page_traits> map;

  constexpr auto num_keys = 100000;

  std::uniform_int_distribution<int> udist{1, num_keys};
  std::random_device r;
  std::seed_seq seed{r(), r(), r(), r(), r(), r(), r(), r()};
  std::mt19937 rnd(seed);

  auto gen_key = [&] { return Key{udist(rnd), udist(rnd), udist(rnd)}; };
  auto gen_part_key = [&] { return PartKey{udist(rnd)}; };
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
    auto key = gen_key();

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
    auto key = gen_part_key();
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
    auto key = gen_key();

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
    auto key = gen_key();

    map.Insert(key, std::get<0>(key));
    REQUIRE((*map.lower_bound(key)).first == map.lower_bound(key)->first);
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

TEST_CASE("BtreeConcurrentMapConcurrencyRandom") {
  ConcurrentMapTest<indexes::btree::concurrent_map<int64_t, int64_t,
                                                   btree_medium_page_traits>>(
      ConcurrentMapTestWorkload::WL_RANDOM);
}

TEST_CASE("BtreeConcurrentMapConcurrencyContented") {
  ConcurrentMapTest<indexes::btree::concurrent_map<int64_t, int64_t,
                                                   btree_medium_page_traits>>(
      ConcurrentMapTestWorkload::WL_CONTENTED);
}

TEST_SUITE_END();