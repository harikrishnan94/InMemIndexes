#pragma once

namespace indexes::utils {
class ThreadLocal {
public:
  // Maximum # active threads supported by EpochGC.
  static constexpr int MAX_THREADS = 1 << 16;

  // Register thread for participating in mvcc.
  static bool RegisterThread();

  // Unregister thread from participating in mvcc.
  static void UnregisterThread();

  // Returns thread id allocated for this thread.
  // NOTE: RegisterThread must be called, before using this function.
  static int ThreadID();

  // Returns # active (registered) threads.
  static int NumRegisteredThreads();

  // Returns Max tid allocated for among all active threads.
  // This is always >= NumRegisterdThreads()
  static int MaxThreadID();
};
} // namespace indexes::utils
