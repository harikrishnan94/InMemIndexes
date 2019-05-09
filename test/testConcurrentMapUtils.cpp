#include "testConcurrentMapUtils.h"

#include <tsl/robin_set.h>

static inline int64_t swap_int64(int64_t val) {
  val = ((val << 8) & 0xFF00FF00FF00FF00ULL) |
        ((val >> 8) & 0x00FF00FF00FF00FFULL);
  val = ((val << 16) & 0xFFFF0000FFFF0000ULL) |
        ((val >> 16) & 0x0000FFFF0000FFFFULL);
  return (val << 32) | ((val >> 32) & 0xFFFFFFFFULL);
}

std::vector<int64_t> generateUniqueValues(int num_threads, int perthread_count,
                                          ConcurrentMapTestWorkload workload) {
  std::random_device rd;
  std::mt19937_64 gen{rd()};
  int size = perthread_count * num_threads;
  std::vector<int64_t> vals(size);

  switch (workload) {
  case ConcurrentMapTestWorkload::WL_CONTENTED:
    for (int i = 0, k = 0; i < size; k++) {
      for (int j = 0; j < num_threads; j++) {
        vals[j * perthread_count + k] = i++;
      }
    }
    break;

  case ConcurrentMapTestWorkload::WL_CONTENTED_SWAP:
    for (int i = 0, k = 0; i < size; k++) {
      for (int j = 0; j < num_threads; j++) {
        vals[j * perthread_count + k] = swap_int64(i++);
      }
    }
    break;

  case ConcurrentMapTestWorkload::WL_RANDOM: {
    std::uniform_int_distribution<int64_t> dist(0, size - 1);
    tsl::robin_set<int64_t> used_vals;

    used_vals.reserve(size);

    for (int i = 0; i < size; i++) {
      int64_t val;

      do {
        val = dist(gen);
      } while (used_vals.count(val));

      used_vals.insert(val);
      vals[i] = val;
    }
  }
  }

  return vals;
}
