#include "indexes/btree/key.h"
#include "indexes/btree/map.h"
#include "sha512.h"

#include <doctest/doctest.h>

#include <limits>
#include <map>
#include <random>
#include <string>

struct btree_small_page_traits : indexes::btree::btree_traits_debug {
  static constexpr int NODE_SIZE = 192;
  static constexpr int NODE_MERGE_THRESHOLD = 80;
};

struct btree_traits_string_key : indexes::btree::btree_traits_debug {
  static constexpr int NODE_SIZE = 448;
};

template class indexes::btree::map<int, int, btree_small_page_traits>;
template class indexes::btree::map<std::string, int, btree_traits_string_key>;

TEST_SUITE_BEGIN("btree");

TEST_CASE("BtreeMapBasic") {
  using Key = indexes::btree::compound_key<int, int, int>;
  using PartKey = indexes::btree::compound_key<int>;
  indexes::btree::map<Key, int, btree_small_page_traits> map;

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

    map[key] = i;
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
    auto it = map.find(kv.first);

    REQUIRE(it != map.end());
    REQUIRE(it.data() == kv.second);
    map.erase(it);
    REQUIRE(map.find(kv.first) == map.end());
  }

  REQUIRE(map.size() == 0);
  REQUIRE(map.find(Key{0, 0, 0}) == map.end());

  // lower and upper bound
  {
    auto key = gen_key();

    map[key] = std::get<0>(key);
    REQUIRE(map.lower_bound(key) != map.upper_bound(key));

    map.erase(key);
    REQUIRE(map.lower_bound(key) == map.upper_bound(key));

    key = min_key();
    REQUIRE(map.lower_bound(key) == map.upper_bound(key));

    key = max_key();
    REQUIRE(map.lower_bound(key) == map.upper_bound(key));
  }

  // Iterator operator
  {
    auto key = gen_key();

    map[key] = std::get<0>(key);
    REQUIRE((*map.lower_bound(key)).first == map.lower_bound(key)->first);
  }
}

TEST_CASE("BtreeMapString") {
  indexes::btree::map<std::string, int, btree_traits_string_key> map;

  int num_keys = 100000;

  std::uniform_int_distribution<int> udist;
  std::random_device r;
  std::seed_seq seed{r(), r(), r(), r(), r(), r(), r(), r()};
  std::mt19937 rnd(seed);

  std::map<std::string, int> key_values;

  for (int i = 0; i < num_keys; i++) {
    std::string key = sha512(std::to_string(udist(rnd)));

    map[key] = i;
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
    auto it = map.find(kv.first);

    REQUIRE(it != map.end());
    REQUIRE(it.data() == kv.second);
    map.erase(it);
    REQUIRE(map.find(kv.first) == map.end());
  }

  REQUIRE(map.size() == 0);
  REQUIRE(map.find("") == map.end());
}

TEST_CASE("BtreeMapMixed") {
  indexes::btree::map<int, int, btree_small_page_traits> map;

  int num_operations = 1024 * 1024;
  int cardinality = num_operations * 0.1;

  constexpr auto INSERT_OP = 1;
  constexpr auto LOOKUP_OP = 2;
  constexpr auto DELETE_OP = 3;

  std::random_device r;
  std::seed_seq seed{r(), r(), r(), r(), r(), r(), r(), r()};
  std::mt19937 rnd(seed);

  std::uniform_int_distribution<int> key_dist{1, cardinality};
  std::uniform_int_distribution<int> val_dist;
  std::uniform_int_distribution<int> op_dist{INSERT_OP, DELETE_OP};

  std::map<int, int> key_values;

  for (int i = 0; i < num_operations; i++) {
    const auto key = key_dist(rnd);
    const auto op = op_dist(rnd);
    int val;

    switch (op) {
    case INSERT_OP: {
      if (key_values.count(key)) {
        auto it = map.find(key);

        REQUIRE(it != map.end());
        REQUIRE(it.data() == key_values[key]);

        map.erase(it);
        key_values.erase(key);
      }

      val = val_dist(rnd);
      map[key] = val;

      REQUIRE(map.find(key) != map.end());
      key_values[key] = val;
      break;
    }

    case LOOKUP_OP: {
      auto it = map.find(key);

      if (key_values.count(key)) {
        REQUIRE(it != map.end());
        REQUIRE(it.data() == key_values[key]);
      } else {
        REQUIRE(it == map.end());
      }

      break;
    }

    case DELETE_OP: {
      auto it = map.find(key);

      if (key_values.count(key)) {
        REQUIRE(it != map.end());
        REQUIRE(it.data() == key_values[key]);

        map.erase(it);
        key_values.erase(key);
      } else {
        REQUIRE(it == map.end());
      }
      break;
    }

    default:
      continue;
    }
  }

  REQUIRE(map.size() == key_values.size());

  for (const auto &kv : key_values) {
    auto it = map.find(kv.first);

    REQUIRE(it != map.end());
    REQUIRE(it.data() == kv.second);
    map.erase(it);
    REQUIRE(map.find(kv.first) == map.end());
  }

  REQUIRE(map.size() == 0);
}

TEST_SUITE_END();