#include "indexes/art/map.h"
#include "testConcurrentMapUtils.h"

#include <doctest/doctest.h>

#include <limits>
#include <random>
#include <string>
#include <unordered_map>

TEST_SUITE_BEGIN("art");

TEST_CASE("ARTBasic") {
  indexes::utils::ThreadRegistry::RegisterThread();
  indexes::art::map<int, indexes::art::art_traits_debug> map;

  int num_keys = 1000 * 1000;

  std::uniform_int_distribution<uint64_t> udist;
  std::random_device r;
  std::seed_seq seed{r(), r(), r(), r(), r(), r(), r(), r()};
  std::mt19937 rnd(seed);

  std::unordered_map<uint64_t, int> key_values;

  for (int i = 0; i < num_keys; i++) {
    auto key = udist(rnd);

    map.Upsert(key, i);
    key_values[key] = i;
  }

  REQUIRE(map.size() == key_values.size());

  for (const auto &kv : key_values) {
    REQUIRE(*map.Search(kv.first) == kv.second);
  }

  for (const auto &kv : key_values) {
    REQUIRE(*map.Delete(kv.first) == kv.second);
    REQUIRE(map.Insert(kv.first, kv.second) == true);
  }

  REQUIRE(map.size() == key_values.size());

  for (const auto &kv : key_values) {
    REQUIRE(*map.Delete(kv.first) == kv.second);
  }

  indexes::utils::ThreadRegistry::UnregisterThread();
}

TEST_CASE("ARTMixed") {
  MixedMapTest<indexes::art::map<int, indexes::art::art_traits_debug>>();
}

TEST_SUITE_END();