/*
 * Copyright 2017-present Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "Unit.h"

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <mutex>
#include <utility>

namespace indexes::utils::parking_lot {
namespace detail {
struct WaitNodeBase {
  const uint64_t m_key;
  const uint64_t m_lotid;
  WaitNodeBase *m_next{nullptr};
  WaitNodeBase *m_prev{nullptr};

  // tricky: hold both bucket and node mutex to write, either to read
  bool m_signaled;
  std::mutex m_mutex;
  std::condition_variable m_cond;

  WaitNodeBase(uint64_t key, uint64_t lotid)
      : m_key(key), m_lotid(lotid), m_signaled(false) {}

  template <typename Clock, typename Duration>
  std::cv_status wait(std::chrono::time_point<Clock, Duration> deadline) {
    std::cv_status status = std::cv_status::no_timeout;
    std::unique_lock<std::mutex> nodeLock(m_mutex);

    while (!m_signaled && status != std::cv_status::timeout) {
      if (deadline != std::chrono::time_point<Clock, Duration>::max()) {
        status = m_cond.wait_until(nodeLock, deadline);
      } else {
        m_cond.wait(nodeLock);
      }
    }
    return status;
  }

  void wake() {
    std::lock_guard<std::mutex> nodeLock(m_mutex);

    m_signaled = true;
    m_cond.notify_one();
  }

  bool signaled() { return m_signaled; }
};

extern std::atomic<uint64_t> idallocator;

// Our emulated futex uses 4096 lists of wait nodes.  There are two levels
// of locking: a per-list mutex that controls access to the list and a
// per-node mutex, condvar, and bool that are used for the actual wakeups.
// The per-node mutex allows us to do precise wakeups without thundering
// herds.
struct Bucket {
  std::mutex m_mutex;
  WaitNodeBase *m_head;
  WaitNodeBase *m_tail;
  std::atomic<uint64_t> m_count;

  static Bucket &bucketFor(size_t key);

  void push_back(WaitNodeBase *node) {
    if (m_tail) {
      assert(m_head);

      node->m_prev = m_tail;
      m_tail->m_next = node;
      m_tail = node;
    } else {
      m_tail = node;
      m_head = node;
    }
  }

  void erase(WaitNodeBase *node) {
    assert(m_count.load(std::memory_order_relaxed) >= 1);

    if (m_head == node && m_tail == node) {
      assert(node->m_prev == nullptr);
      assert(node->m_next == nullptr);

      m_head = nullptr;
      m_tail = nullptr;
    } else if (m_head == node) {
      assert(node->m_prev == nullptr);
      assert(node->m_next);

      m_head = node->m_next;
      m_head->m_prev = nullptr;
    } else if (m_tail == node) {
      assert(node->m_next == nullptr);
      assert(node->m_prev);

      m_tail = node->m_prev;
      m_tail->m_next = nullptr;
    } else {
      assert(node->m_next);
      assert(node->m_prev);

      node->m_next->m_prev = node->m_prev;
      node->m_prev->m_next = node->m_next;
    }
    m_count.fetch_sub(1, std::memory_order_relaxed);
  }
};

} // namespace detail

enum class UnparkControl {
  RetainContinue,
  RemoveContinue,
  RetainBreak,
  RemoveBreak,
};

enum class ParkResult {
  Skip,
  Unpark,
  Timeout,
};

/*
 * ParkingLot provides an interface that is similar to Linux's futex
 * system call, but with additional functionality.  It is implemented
 * in a portable way on top of std::mutex and std::condition_variable.
 *
 * Additional reading:
 * https://webkit.org/blog/6161/locking-in-webkit/
 * https://github.com/WebKit/webkit/blob/master/Source/WTF/wtf/ParkingLot.h
 * https://locklessinc.com/articles/futex_cheat_sheet/
 *
 * The main difference from futex is that park/unpark take lambdas,
 * such that nearly anything can be done while holding the bucket
 * lock.  Unpark() lambda can also be used to wake up any number of
 * waiters.
 *
 * ParkingLot is templated on the data type, however, all ParkingLot
 * implementations are backed by a single static array of buckets to
 * avoid large memory overhead.  Lambdas will only ever be called on
 * the specific ParkingLot's nodes.
 */
template <typename Data = Unit> class ParkingLot {
  const uint64_t m_lotid;
  ParkingLot(const ParkingLot &) = delete;

  struct WaitNode : public parking_lot::detail::WaitNodeBase {
    const Data m_data;

    template <typename D>
    WaitNode(uint64_t key, uint64_t lotid, D &&data)
        : WaitNodeBase(key, lotid), m_data(std::forward<D>(data)) {}
  };

public:
  ParkingLot() : m_lotid(parking_lot::detail::idallocator++) {}

  /* Park API
   *
   * Key is almost always the address of a variable.
   *
   * ToPark runs while holding the bucket lock: usually this
   * is a check to see if we can sleep, by checking waiter bits.
   *
   * PreWait is usually used to implement condition variable like
   * things, such that you can unlock the condition variable's lock at
   * the appropriate time.
   */
  template <typename Key, typename D, typename ToPark, typename PreWait>
  ParkResult park(const Key key, D &&data, ToPark &&toPark, PreWait &&preWait) {
    return park_until(key, std::forward<D>(data), std::forward<ToPark>(toPark),
                      std::forward<PreWait>(preWait),
                      std::chrono::steady_clock::time_point::max());
  }

  template <typename Key, typename D, typename ToPark, typename PreWait,
            typename Clock, typename Duration>
  ParkResult park_until(const Key key, D &&data, ToPark &&toPark,
                        PreWait &&preWait,
                        std::chrono::time_point<Clock, Duration> deadline);

  template <typename Key, typename D, typename ToPark, typename PreWait,
            typename Rep, typename Period>
  ParkResult park_for(const Key key, D &&data, ToPark &&toPark,
                      PreWait &&preWait,
                      std::chrono::duration<Rep, Period> &timeout) {
    return park_until(key, std::forward<D>(data), std::forward<ToPark>(toPark),
                      std::forward<PreWait>(preWait),
                      timeout + std::chrono::steady_clock::now());
  }

  /*
   * Unpark API
   *
   * Key is the same uniqueaddress used in park(), and is used as a
   * hash key for lookup of waiters.
   *
   * Unparker is a function that is given the Data parameter, and
   * returns an UnparkControl.  The Remove* results will remove and
   * wake the waiter, the Ignore/Stop results will not, while stopping
   * or continuing iteration of the waiter list.
   */
  template <typename Key, typename Unparker>
  void unpark(const Key key, Unparker &&func);
};

template <typename Data>
template <typename Key, typename D, typename ToPark, typename PreWait,
          typename Clock, typename Duration>
ParkResult ParkingLot<Data>::park_until(
    const Key bits, D &&data, ToPark &&toPark, PreWait &&preWait,
    std::chrono::time_point<Clock, Duration> deadline) {
  auto key = std::hash<Key>{}(bits);
  auto &bucket = parking_lot::detail::Bucket::bucketFor(key);

  WaitNode node(key, m_lotid, std::forward<D>(data));

  {
    // A: Must be seq_cst.  Matches B.
    bucket.m_count.fetch_add(1, std::memory_order_seq_cst);

    std::unique_lock<std::mutex> bucketLock(bucket.m_mutex);

    if (!std::forward<ToPark>(toPark)()) {
      bucketLock.unlock();
      bucket.m_count.fetch_sub(1, std::memory_order_relaxed);
      return ParkResult::Skip;
    }

    bucket.push_back(&node);
  } // bucketLock scope

  std::forward<PreWait>(preWait)();

  auto status = node.wait(deadline);

  if (status == std::cv_status::timeout) {
    // it's not really a timeout until we unlink the unsignaled node
    std::lock_guard<std::mutex> bucketLock(bucket.m_mutex);

    if (!node.signaled()) {
      bucket.erase(&node);

      return ParkResult::Timeout;
    }
  }

  return ParkResult::Unpark;
}

template <typename Data>
template <typename Key, typename Func>
void ParkingLot<Data>::unpark(const Key bits, Func &&func) {
  auto key = std::hash<Key>{}(bits);
  auto &bucket = parking_lot::detail::Bucket::bucketFor(key);

  // B: Must be seq_cst.  Matches A.  If true, A *must* see in seq_cst
  // order any atomic updates in toPark() (and matching updates that
  // happen before unpark is called)
  if (bucket.m_count.load(std::memory_order_seq_cst) == 0)
    return;

  std::lock_guard<std::mutex> bucketLock(bucket.m_mutex);

  for (auto iter = bucket.m_head; iter != nullptr;) {
    auto node = static_cast<WaitNode *>(iter);
    iter = iter->m_next;

    if (node->m_key == key && node->m_lotid == m_lotid) {
      auto result = std::forward<Func>(func)(node->m_data);
      if (result == UnparkControl::RemoveBreak ||
          result == UnparkControl::RemoveContinue) {
        // we unlink, but waiter destroys the node
        bucket.erase(node);

        node->wake();
      }

      if (result == UnparkControl::RemoveBreak ||
          result == UnparkControl::RetainBreak)
        return;
    }
  }
}
} // namespace indexes::utils::parking_lot