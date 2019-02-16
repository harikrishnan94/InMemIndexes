#pragma once

#include "ParkingLot.h"
#include "ThreadLocal.h"

#include <algorithm>
#include <chrono>
#include <immintrin.h>
#include <limits.h>
#include <mutex>
#include <thread>
#include <unordered_map>

namespace indexes::utils
{
template <bool EnableDetectDetection>
class MutexImpl;

using Mutex             = MutexImpl<false>;
using DeadlockSafeMutex = MutexImpl<true>;

namespace detail
{
	constexpr int M_CONTENDED_MASK = 1 << (sizeof(int) * CHAR_BIT - 1);
	constexpr int M_UNLOCKED       = -1 & ~M_CONTENDED_MASK;

	extern parking_lot::ParkingLot<std::nullptr_t> parkinglot;
	extern std::unique_ptr<std::atomic<const DeadlockSafeMutex *>[]> thread_waiting_on;
	extern std::mutex dead_lock_verify_mutex;
} // namespace detail

enum class MutexLockResult
{
	LOCKED,
	DEADLOCKED
};

template <bool EnableDetectDetection>
class MutexImpl
{
private:
	std::atomic<int> word{ detail::M_UNLOCKED };

	bool
	check_deadlock() const
	{
		std::unordered_map<int, const DeadlockSafeMutex *> waiters;

		auto detect_deadlock = [&]() {
			const DeadlockSafeMutex *waiting_on = this;

			waiters[ThreadLocal::ThreadID()] = waiting_on;

			while (true)
			{
				int lock_holder = waiting_on->word & ~detail::M_CONTENDED_MASK;

				/* Lock was just released.. */
				if (lock_holder == detail::M_UNLOCKED)
					return false;

				waiting_on = detail::thread_waiting_on[lock_holder];

				/* lock holder is live, so not a dead lock */
				if (waiting_on == nullptr)
					return false;

				/* Found a cycle, so deadlock */
				if (waiters.count(lock_holder) != 0)
					return true;

				waiters[lock_holder] = waiting_on;
			}
		};

		auto verify_deadlock = [&]() {
			std::lock_guard<std::mutex> dead_lock_verify_lock{ detail::dead_lock_verify_mutex };

			for (const auto &waiter : waiters)
			{
				if (waiter.second != detail::thread_waiting_on[waiter.first])
					return false;
			}

			denounce_wait();

			return true;
		};

		return detect_deadlock() && verify_deadlock();
	}

	bool
	park() const
	{
		if constexpr (EnableDetectDetection)
		{
			using namespace std::chrono_literals;
			static constexpr auto DEADLOCK_DETECT_TIMEOUT = 1s;

			announce_wait();

			auto res = detail::parkinglot.park_for(this,
			                                       nullptr,
			                                       [&]() { return is_lock_contented(); },
			                                       []() {},
			                                       DEADLOCK_DETECT_TIMEOUT);

			if (res == parking_lot::ParkResult::Timeout && check_deadlock())
				return true;

			denounce_wait();
		}
		else
		{
			detail::parkinglot.park(this, nullptr, [&]() { return is_lock_contented(); }, []() {});
		}

		return false;
	}

	void
	announce_wait() const
	{
		detail::thread_waiting_on[ThreadLocal::ThreadID()] = this;
	}

	void
	denounce_wait() const
	{
		detail::thread_waiting_on[ThreadLocal::ThreadID()] = nullptr;
	}

	bool
	is_lock_contented() const
	{
		return word & detail::M_CONTENDED_MASK;
	}

	bool
	uncontended_path_available()
	{
		while (true)
		{
			int old = word;

			if (old == detail::M_UNLOCKED)
				return true;

			if (old & detail::M_CONTENDED_MASK
			    || word.compare_exchange_strong(old, old | detail::M_CONTENDED_MASK))
			{
				return false;
			}

			_mm_pause();
		}
	}

	bool
	try_lock_contended()
	{
		auto old = detail::M_UNLOCKED;

		return word.compare_exchange_strong(old,
		                                    ThreadLocal::ThreadID() | detail::M_CONTENDED_MASK);
	}

	MutexLockResult
	lock_contended()
	{
		while (!try_lock_contended())
		{
			if (park())
				return MutexLockResult::DEADLOCKED;
		};

		return MutexLockResult::LOCKED;
	}

public:
	static constexpr bool DEADLOCK_SAFE = EnableDetectDetection;

	MutexImpl()                  = default;
	MutexImpl(MutexImpl &&)      = delete;
	MutexImpl(const MutexImpl &) = delete;

	bool
	try_lock()
	{
		auto old = detail::M_UNLOCKED;

		return word.compare_exchange_strong(old, ThreadLocal::ThreadID());
	}

	bool
	is_locked() const
	{
		return word != detail::M_UNLOCKED;
	}

	MutexLockResult
	lock()
	{
		while (!try_lock())
		{
			if (!uncontended_path_available())
				return lock_contended();

			_mm_pause();
		}

		assert(is_locked());

		return MutexLockResult::LOCKED;
	}

	void
	unlock()
	{
		int old = word.exchange(detail::M_UNLOCKED);

		if (old & detail::M_CONTENDED_MASK)
		{
			detail::parkinglot.unpark(this,
			                          [](auto) { return parking_lot::UnparkControl::RemoveBreak; });
		}
	}
};

} // namespace indexes::utils
