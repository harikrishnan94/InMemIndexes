#include "indexes/utils/ThreadRegistry.h"

#include <doctest/doctest.h>
#include <gsl/span>

#include <inttypes.h>
#include <limits>
#include <map>
#include <random>
#include <string>
#include <thread>
#include <vector>

enum class ConcurrentMapTestWorkload {
  WL_CONTENTED,
  WL_CONTENTED_SWAP,
  WL_RANDOM,
};

std::vector<int64_t> generateUniqueValues(int num_threads, int perthread_count,
                                          ConcurrentMapTestWorkload workload);

template <typename MapType> void MixedMapTest() {
  indexes::utils::ThreadRegistry::RegisterThread();

  MapType map;

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
  std::uniform_int_distribution<> op_dist{INSERT_OP, DELETE_OP};

  std::map<int, int> key_values;

  for (int i = 0; i < num_operations; i++) {
    const auto key = key_dist(rnd);
    const auto op = op_dist(rnd);
    int val;

    switch (op) {
    case INSERT_OP: {
      val = val_dist(rnd);

      if (key_values.count(key)) {
        REQUIRE(*map.Search(key) == key_values[key]);
        REQUIRE(*map.Update(key, val) == key_values[key]);
      } else {
        REQUIRE(map.Insert(key, val) == true);
        REQUIRE(*map.Search(key) == val);
      }

      key_values[key] = val;
      break;
    }

    case LOOKUP_OP: {
      if (key_values.count(key))
        REQUIRE(*map.Search(key) == key_values[key]);
      else
        REQUIRE(map.Search(key).has_value() == false);

      break;
    }

    case DELETE_OP: {
      if (key_values.count(key)) {
        REQUIRE(*map.Search(key) == key_values[key]);
        REQUIRE(*map.Delete(key) == key_values[key]);
        key_values.erase(key);
      } else {
        REQUIRE(map.Delete(key).has_value() == false);
      }
      break;
    }

    default:
      continue;
    }
  }

  REQUIRE(map.size() == key_values.size());

  for (const auto &kv : key_values) {
    REQUIRE(*map.Delete(kv.first) == kv.second);
  }

  REQUIRE(map.size() == 0);

  indexes::utils::ThreadRegistry::UnregisterThread();
}

template <typename MapType>
static void lookup_worker(MapType &map, gsl::span<int64_t> vals) {
  indexes::utils::ThreadRegistry::RegisterThread();

  for (auto val : vals) {
    map.Search(val);
  }

  indexes::utils::ThreadRegistry::UnregisterThread();
}

template <typename MapType>
static void insert_worker(MapType &map, gsl::span<int64_t> vals) {
  indexes::utils::ThreadRegistry::RegisterThread();

  for (auto val : vals) {
    REQUIRE(map.Insert(val, val) == true);
    REQUIRE(*map.Search(val) == val);
  }

  indexes::utils::ThreadRegistry::UnregisterThread();
}

enum class OpType { INSERT, DELETE, DELETE_AND_INSERT };

template <typename MapType>
static void delete_worker(MapType &map, gsl::span<int64_t> vals, OpType op) {
  indexes::utils::ThreadRegistry::RegisterThread();

  for (auto val : vals) {
    switch (op) {
    case OpType::DELETE:
      REQUIRE(*map.Delete(val) == val);
      REQUIRE(map.Search(val).has_value() == false);
      break;

    case OpType::DELETE_AND_INSERT:
      REQUIRE(*map.Delete(val) == val);
      REQUIRE(map.Insert(val, val) == true);
      break;

    case OpType::INSERT:
      break;
    }
  }

  indexes::utils::ThreadRegistry::UnregisterThread();
}

template <typename MapType>
void ConcurrentMapTest(ConcurrentMapTestWorkload workload) {
  MapType map;
  constexpr int PER_THREAD_OP_COUNT = 256 * 1024;
  const int NUM_THREADS = std::thread::hardware_concurrency();
  std::vector<int64_t> vals =
      generateUniqueValues(NUM_THREADS, PER_THREAD_OP_COUNT, workload);

  indexes::utils::ThreadRegistry::RegisterThread();

  auto run_test = [&](OpType op) {
    std::vector<std::thread> workers;
    int startval = 0;
    int quantum = vals.size() / NUM_THREADS;

    for (int i = 0; i < NUM_THREADS; i++) {
      if (op == OpType::INSERT) {
        workers.emplace_back(
            insert_worker<MapType>, std::ref(map),
            gsl::span<int64_t>{vals.data() + startval, quantum});
      } else {
        workers.emplace_back(
            delete_worker<MapType>, std::ref(map),
            gsl::span<int64_t>{vals.data() + startval, quantum}, op);
      }

      startval += quantum;
    }

    workers.emplace_back(lookup_worker<MapType>, std::ref(map),
                         gsl::span<int64_t>{vals});

    for (auto &worker : workers) {
      worker.join();
    }
  };

  // Insert check
  {
    run_test(OpType::INSERT);

    REQUIRE(map.size() == vals.size());

    for (auto val : vals) {
      REQUIRE(*map.Search(val) == val);
    }
  }

  // Delete And Insert check
  {
    run_test(OpType::DELETE_AND_INSERT);

    for (auto val : vals) {
      REQUIRE(*map.Search(val) == val);
    }

    REQUIRE(map.size() == vals.size());
  }

  // Delete check
  {
    run_test(OpType::DELETE);

    REQUIRE(map.size() == 0);
  }

  indexes::utils::ThreadRegistry::UnregisterThread();
}
