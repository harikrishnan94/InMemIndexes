//
//  uniform_generator.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/6/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_UNIFORM_GENERATOR_H_
#define YCSB_C_UNIFORM_GENERATOR_H_

#include "generator.h"

#include <atomic>
#include <mutex>
#include <random>

namespace ycsbc {
class UniformGenerator : public Generator<uint64_t> {
public:
  // Both min and max are inclusive
  UniformGenerator(uint64_t min, uint64_t max) : dist_(min, max) { Next(); }

  uint64_t Next();
  uint64_t Last();
  uint64_t NextUnlocked();
  uint64_t LastUnlocked();

private:
  std::mt19937_64 generator_;
  std::uniform_int_distribution<uint64_t> dist_;
  uint64_t last_int_;
  std::mutex mutex_;
};

inline uint64_t UniformGenerator::NextUnlocked() {
  return last_int_ = dist_(generator_);
}

inline uint64_t UniformGenerator::LastUnlocked() { return last_int_; }

inline uint64_t UniformGenerator::Next() {
  std::lock_guard<std::mutex> lock(mutex_);
  return NextUnlocked();
}

inline uint64_t UniformGenerator::Last() {
  std::lock_guard<std::mutex> lock(mutex_);
  return LastUnlocked();
}

} // namespace ycsbc

#endif // YCSB_C_UNIFORM_GENERATOR_H_
