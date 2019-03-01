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
template <bool EnableDeadlockDetection>
class MutexImpl;

using Mutex             = MutexImpl<false>;
using DeadlockSafeMutex = MutexImpl<true>;

namespace detail
{
	extern parking_lot::ParkingLot<std::nullptr_t> parkinglot;
	extern std::unique_ptr<std::atomic<const DeadlockSafeMutex *>[]> thread_waiting_on;
	extern std::mutex dead_lock_verify_mutex;
} // namespace detail

enum class MutexLockResult
{
	LOCKED,
	DEADLOCKED
};

template <bool EnableDeadlockDetection>
class MutexImpl
{
private:
	class LockWord
	{
		enum class LockState : int8_t
		{
			LS_UNLOCKED,
			LS_LOCKED,
			LS_CONTENTED
		};

		static constexpr int M_CONTENDED_MASK = 1 << (sizeof(int) * CHAR_BIT - 1);
		static constexpr int M_UNLOCKED       = -1 & ~M_CONTENDED_MASK;

	public:
		using WordType = std::conditional_t<EnableDeadlockDetection, int, LockState>;

	private:
		LockWord(WordType a_word) : word(a_word)
		{}

	public:
		WordType word;

		static LockWord
		get_unlocked_word()
		{
			if constexpr (EnableDeadlockDetection)
				return M_UNLOCKED;
			else
				return LockState::LS_UNLOCKED;
		}

		WordType
		get_value() const
		{
			return word;
		}

		bool
		is_locked() const
		{
			return word != get_unlocked_word().get_value();
		}

		bool
		is_lock_contented() const
		{
			if constexpr (EnableDeadlockDetection)
				return (word & M_CONTENDED_MASK) == 0;
			else
				return word == LockState::LS_CONTENTED;
		}

		LockWord
		as_uncontented_word()
		{
			if constexpr (EnableDeadlockDetection)
				return word & ~M_CONTENDED_MASK;
			else
				return LockState::LS_LOCKED;
		}

		static LockWord
		get_contented_word()
		{
			if constexpr (EnableDeadlockDetection)
				return ThreadLocal::ThreadID() | M_CONTENDED_MASK;
			else
				return LockState::LS_CONTENTED;
		}

		static LockWord
		get_lock_word()
		{
			if constexpr (EnableDeadlockDetection)
				return ThreadLocal::ThreadID();
			else
				return LockState::LS_LOCKED;
		}
	};

	std::atomic<LockWord> word{ LockWord::get_unlocked_word() };

	bool
	check_deadlock() const
	{
		if constexpr (EnableDeadlockDetection)
		{
			std::unordered_map<int, const DeadlockSafeMutex *> waiters;

			auto detect_deadlock = [&]() {
				const DeadlockSafeMutex *waiting_on = this;

				waiters[ThreadLocal::ThreadID()] = waiting_on;

				while (true)
				{
					auto lock_holder = waiting_on->word.load().as_uncontented_word();

					/* Lock was just released.. */
					if (!lock_holder.is_locked())
						return false;

					waiting_on = detail::thread_waiting_on[lock_holder.get_value()];

					/* lock holder is live, so not a dead lock */
					if (waiting_on == nullptr)
						return false;

					/* Found a cycle, so deadlock */
					if (waiters.count(lock_holder.get_value()) != 0)
						return true;

					waiters[lock_holder.get_value()] = waiting_on;
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

		return false;
	}

	bool
	park() const
	{
		if constexpr (EnableDeadlockDetection)
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
		if constexpr (EnableDeadlockDetection)
			detail::thread_waiting_on[ThreadLocal::ThreadID()] = this;
	}

	void
	denounce_wait() const
	{
		if constexpr (EnableDeadlockDetection)
			detail::thread_waiting_on[ThreadLocal::ThreadID()] = nullptr;
	}

	bool
	is_lock_contented() const
	{
		return word.load().is_lock_contented();
	}

	bool
	uncontended_path_available()
	{
		while (true)
		{
			auto old = word.load();

			if (!old.is_locked())
				return true;

			if (old.is_lock_contented()
			    || word.compare_exchange_strong(old, old.get_contented_word()))
			{
				return false;
			}

			_mm_pause();
		}
	}

	bool
	try_lock_contended()
	{
		auto old = LockWord::get_unlocked_word();

		return word.compare_exchange_strong(old, LockWord::get_contented_word());
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
	static constexpr bool DEADLOCK_SAFE = EnableDeadlockDetection;

	MutexImpl()                  = default;
	MutexImpl(MutexImpl &&)      = delete;
	MutexImpl(const MutexImpl &) = delete;

	bool
	try_lock()
	{
		auto old = LockWord::get_unlocked_word();

		return word.compare_exchange_strong(old, LockWord::get_lock_word());
	}

	bool
	is_locked() const
	{
		return word.load().is_locked();
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
		auto old = word.exchange(LockWord::get_unlocked_word());

		if (old.is_lock_contented())
		{
			detail::parkinglot.unpark(this,
			                          [](auto) { return parking_lot::UnparkControl::RemoveBreak; });
		}
	}
};

} // namespace indexes::utils
