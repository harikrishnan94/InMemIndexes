#pragma once

#include "ThreadLocal.h"
#include "TypeSafeInt.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <deque>
#include <functional>
#include <initializer_list>
#include <limits>
#include <memory>
#include <mutex>
#include <set>

#include <gsl/gsl>

namespace indexes::utils
{
template <typename epoch_t,
          typename ReclaimedType,
          int ReclamationThreshold = 1000,
          typename Enable          = void>
class EpochManager;

// Provides support for epoch based reclaimation.
template <typename epoch_t, typename ReclaimedType, int ReclamationThreshold>
class EpochManager<
    epoch_t,
    ReclaimedType,
    ReclamationThreshold,
    typename std::enable_if_t<
        (ReclamationThreshold > 0)
        && std::is_integral_v<
               epoch_t> && !std::is_same_v<epoch_t, bool> && !std::is_same_v<epoch_t, char>>>
{
	// Calling thread must be registered by calling `RegisterThread` before using epoch based safe
	// memory reclamation.

	using ReclaimedPtrType = ReclaimedType *;

public:
	// It is undefined behaviour to use epoch manager, without registering the
	// thread. Returns false, if active registered thread count is more than `MAX_THREADS`. If
	// returned false, thread is not registered.

	// enter_epoch guarantees that all shared objects, accessed by the calling thread,
	// after enter_epoch is called are safe.
	inline void
	enter_epoch()
	{
		m_local_epoch[ThreadLocal::ThreadID()].epoch = now();
	}

	// exit_epoch marks quiescent state of the calling thread.
	// Enables reclaimation of objects retired before calling thread's epoch.
	inline void
	exit_epoch()
	{
		m_local_epoch[ThreadLocal::ThreadID()].epoch.store(QUIESCENT_STATE,
		                                                   std::memory_order_release);
	}

	// Switch to new epoch and return old epoch
	inline epoch_t
	switch_epoch()
	{
		return m_global_epoch++;
	}

	// Returns current threads epoch
	inline epoch_t
	my_epoch()
	{
		return m_local_epoch[ThreadLocal::ThreadID()].epoch.load(std::memory_order_relaxed);
	}

	// Return current epoch
	inline epoch_t
	now() const
	{
		return m_global_epoch;
	}

	// retire objects and start a new epoch.
	// Objects will be reclaimed at a suitable and safe epoch.
	// When reclaimed `reclaimer` will be called for each reclaimed object.
	void
	retire_in_new_epoch(std::function<void(ReclaimedPtrType object)> reclaimer,
	                    gsl::span<ReclaimedPtrType> objects)
	{
		retire(reclaimer, objects, switch_epoch());
	}

	inline void
	retire_in_new_epoch(std::function<void(ReclaimedPtrType object)> reclaimer,
	                    ReclaimedPtrType object)
	{
		retire_in_new_epoch(reclaimer, { &object, 1 });
	}

	// retire objects in current (without starting new epoch) epoch.
	// Objects will be reclaimed at a suitable and safe epoch.
	// When reclaimed `reclaimer` will be called for each reclaimed object.
	void
	retire_in_current_epoch(std::function<void(ReclaimedPtrType object)> reclaimer,
	                        gsl::span<ReclaimedPtrType> objects)
	{
		retire(reclaimer, objects, now());
	}

	inline void
	retire_in_current_epoch(std::function<void(ReclaimedPtrType object)> reclaimer,
	                        ReclaimedPtrType object)
	{
		retire_in_current_epoch(reclaimer, { &object, 1 });
	}

	// Reclaim objects which are safe to reclaim.
	// Returns # objects still not reclaimed (but retired).
	// A long running thread using an epoch could prevent
	// reclaimation of objects visible to that thread.
	size_t
	do_reclaim()
	{
		return reclaim_in_retire_list(m_retire_list[ThreadLocal::ThreadID()], get_min_used_epoch());
	}

	void
	reclaim_all()
	{
		epoch_t min_used_epoch = get_min_used_epoch();

		for (auto &retire_list : m_retire_list)
		{
			reclaim_in_retire_list(retire_list, min_used_epoch);
		}
	}

	// Getter/Setter for reclaimation threshold.
	// # objects to collect before triggering reclaimation.
	// A low value means possibly reduced peak memory consumption, but would less
	// performance, because of frequent calls to `DoReclaim`.
	void
	set_reclamation_threshold(int threshold)
	{
		if (threshold > 0)
			m_reclaimation_threshold = threshold;
	}

	int
	get_reclamation_threshold(int threshold)
	{
		return m_reclaimation_threshold;
	}

	EpochManager()
	    : m_reclaimation_threshold{ ReclamationThreshold }
	    , m_global_epoch{ 0 }
	    , m_local_epoch(ThreadLocal::MAX_THREADS)
	    , m_retire_list(ThreadLocal::MAX_THREADS)
	{
		for (int i = 0; i < ThreadLocal::MAX_THREADS; i++)
			m_local_epoch[i].epoch.store(QUIESCENT_STATE, std::memory_order_relaxed);
	}

	EpochManager(const EpochManager &) = delete;
	EpochManager(EpochManager &&)      = delete;

private:
	class Retiree
	{
	public:
		Retiree(ReclaimedPtrType object,
		        std::function<void(ReclaimedPtrType object)> reclaimer,
		        epoch_t retired_epoch)
		    : m_object(object), m_retired_epoch(retired_epoch), m_reclaimer(reclaimer)
		{}

		// Can reclaim this object after the given `epoch`
		bool
		can_reclaim(epoch_t epoch) const
		{
			return epoch > m_retired_epoch;
		}

		void
		reclaim()
		{
			return m_reclaimer(m_object);
		}

	private:
		// Object to reclaim safely.
		ReclaimedPtrType m_object;
		// Epoch on which the object was retired.
		epoch_t m_retired_epoch;
		// Callback to call during reclaimation.
		std::function<void(ReclaimedPtrType object)> m_reclaimer;
	};

	static size_t
	reclaim_in_retire_list(std::deque<Retiree> &retire_list, epoch_t min_used_epoch)
	{
		auto begin        = std::begin(retire_list);
		auto reclaim_upto = begin;

		for (auto &retiree : retire_list)
		{
			if (!retiree.can_reclaim(min_used_epoch))
				break;

			retiree.reclaim();
			reclaim_upto++;
		}

		size_t num_reclaimed = reclaim_upto - begin;

		if (num_reclaimed)
		{
			retire_list.erase(begin, reclaim_upto);
			retire_list.shrink_to_fit();
		}

		return retire_list.size();
	}

	epoch_t
	get_min_used_epoch()
	{
		return std::min_element(std::begin(m_local_epoch),
		                        std::begin(m_local_epoch) + ThreadLocal::MaxThreadID() + 1,
		                        [](const auto &a, const auto &b) {
			                        return a.epoch.load() < b.epoch.load();
		                        })
		    ->epoch;
	}

	void
	retire(std::function<void(ReclaimedPtrType object)> reclaimer,
	       gsl::span<ReclaimedPtrType> objects,
	       epoch_t retired_epoch)
	{
		auto &retire_list = m_retire_list[ThreadLocal::ThreadID()];

		for (auto object : objects)
			retire_list.emplace_back(object, reclaimer, retired_epoch);

		if (static_cast<int>(retire_list.size()) >= m_reclaimation_threshold)
			do_reclaim();
	}

	// Reclaimation threshold. Default 1000 objects
	std::atomic<int> m_reclaimation_threshold;

	// Global epoch
	std::atomic<epoch_t> m_global_epoch;

	// Quiescent state
	static constexpr epoch_t QUIESCENT_STATE = std::numeric_limits<epoch_t>::max();

	struct alignas(128) AlignedAtomicEpoch
	{
		std::atomic<epoch_t> epoch;
	};

	// Thread local epoch, denotes the epoch of each thread.
	// Accessed using `slot` by each thread.
	std::vector<AlignedAtomicEpoch> m_local_epoch;

	// Thread local retire list. Accessed using `slot` by each thread.
	std::vector<std::deque<Retiree>> m_retire_list;
};
} // namespace indexes::utils
