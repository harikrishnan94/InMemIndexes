// include/hashtable/concurrent_map.h
// Hashtbale implementation

#pragma once

#include "indexes/utils/EpochManager.h"
#include "indexes/utils/Mutex.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cinttypes>
#include <cstddef>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <type_traits>
#include <utility>

#define HT_DEBUG(expr)     \
	do                     \
	{                      \
		if (Traits::DEBUG) \
		{                  \
			expr;          \
		}                  \
	} while (0)

#define HT_DEBUG_ASSERT(expr) HT_DEBUG(assert(expr))
#define HT_DEBUG_ONLY(expr) ((void) (expr))

namespace indexes::hashtable
{
struct hashtable_traits_default
{
	static constexpr bool DEBUG = false;

	using LinkType = uint8_t;

	static constexpr LinkType LINEAR_SEARCH_LIMIT = std::numeric_limits<LinkType>::max();
};

struct hashtable_traits_debug : hashtable_traits_default
{
	static constexpr bool DEBUG = true;
};

template <typename Key,
          typename Value,
          typename Hash   = std::hash<Key>,
          typename Traits = hashtable_traits_default>
class concurrent_map
{
public:
	using key_type        = Key;
	using mapped_type     = Value;
	using value_type      = std::pair<const Key, Value>;
	using size_type       = std::size_t;
	using difference_type = std::ptrdiff_t;
	using hasher          = Hash;
	using reference       = value_type &;
	using const_reference = const value_type &;
	using pointer         = value_type *;
	using const_pointer   = const value_type *;

private:
	static constexpr Hash raw_hash = hasher{};

	static size_t
	next_pow_2(size_t n)
	{
		int count = 0;

		// First n in the below condition
		// is for the case where n is 0
		if (n && !(n & (n - 1)))
			return n;

		while (n != 0)
		{
			n >>= 1;
			count += 1;
		}

		return static_cast<size_t>(1) << count;
	}

	class HashTable
	{
	public:
		struct HashBucket
		{
			using KeyValuePair = std::pair<key_type, mapped_type>;

			std::atomic<size_t> hash;
			KeyValuePair key_value;

			static constexpr size_t EMPTY_HASH      = std::numeric_limits<size_t>::max();
			static constexpr size_t TOMB_STONE_HASH = EMPTY_HASH - 1;

			bool
			is_free() const
			{
				return hash == EMPTY_HASH;
			}

			bool
			has_value() const
			{
				return hash < TOMB_STONE_HASH;
			}

			HashBucket(size_t a_hash, const key_type &a_key, const mapped_type &a_val)
			    : hash(a_hash), key_value{ a_key, a_val }
			{}

			void
			emplace(size_t hash, const key_type &key, const mapped_type &val)
			{
				new (&this->key_value) KeyValuePair{ key, val };
				this->hash.store(hash, std::memory_order_release);
			}

			mapped_type
			exchange(const mapped_type &val)
			{
				mapped_type oldval = key_value.second;

				this->key_value.second.~mapped_type();
				new (&key_value.second) mapped_type{ val };

				return oldval;
			}

			void
			mark_as_empty()
			{
				hash.store(TOMB_STONE_HASH, std::memory_order_release);
			}

			bool
			equals(size_t hash, const key_type &key) const
			{
				return this->hash == hash && key_value.first == key;
			}

			void
			destroy()
			{
				if (has_value())
					key_value.~KeyValuePair();
			}

			~HashBucket()
			{
				destroy();
			}
		};

		struct alignas(128) HashTablePerThreadStats
		{
			std::atomic<size_t> num_values;
			std::atomic<size_t> num_tomb_stones;
		};

		struct Link
		{
			std::atomic<typename Traits::LinkType> first;
			std::atomic<typename Traits::LinkType> next;
			indexes::utils::Mutex m;
		};

		size_t num_buckets;
		std::unique_ptr<HashTablePerThreadStats[]> stats;

		std::unique_ptr<uint8_t[]> mem;
		std::unique_ptr<Link[]> link;
		HashBucket *buckets;

		HashTable(size_t inital_num_buckets)
		    : num_buckets(next_pow_2(inital_num_buckets))
		    , stats(std::make_unique<HashTablePerThreadStats[]>(utils::ThreadLocal::MAX_THREADS))
		    , mem(std::make_unique<uint8_t[]>(num_buckets * sizeof(HashBucket)))
		    , link(std::make_unique<Link[]>(num_buckets))
		    , buckets(reinterpret_cast<HashBucket *>(mem.get()))
		{
			init_buckets();
		}

		HashTable(HashTable &&ht)
		    : num_buckets(std::move(ht.num_buckets))
		    , stats(std::move(ht.stats))
		    , mem(std::move(ht.mem))
		    , link(std::move(ht.link))
		    , buckets(std::move(ht.buckets))
		{}

		HashTable &
		operator=(HashTable &&ht)
		{
			num_buckets = std::move(ht.num_buckets);
			stats       = std::move(ht.stats);
			mem         = std::move(ht.mem);
			link        = std::move(ht.link);
			buckets     = ht.buckets;

			ht.buckets     = nullptr;
			ht.num_buckets = 0;

			return *this;
		}

		~HashTable()
		{
			destroy_buckets();
		}

		void
		increment_num_values()
		{
			std::atomic<size_t> &num_values = stats[utils::ThreadLocal::ThreadID()].num_values;

			num_values.store(num_values.load() + 1, std::memory_order_relaxed);
		}

		void
		increment_num_tomb_stones()
		{
			std::atomic<size_t> &num_tomb_stones =
			    stats[utils::ThreadLocal::ThreadID()].num_tomb_stones;

			num_tomb_stones.store(num_tomb_stones.load() + 1, std::memory_order_relaxed);
		}

		std::pair<size_t, size_t>
		get_stats() const
		{
			size_t num_values      = 0;
			size_t num_tomb_stones = 0;

			for (int i = 0; i < utils::ThreadLocal::MAX_THREADS; i++)
			{
				num_values += stats[i].num_values.load(std::memory_order_relaxed);
				num_tomb_stones += stats[i].num_tomb_stones.load(std::memory_order_relaxed);
			}

			return { num_values, num_tomb_stones };
		}

		void
		init_buckets()
		{
			std::for_each(buckets, buckets + num_buckets, [](auto &bucket) {
				bucket.hash.store(HashBucket::EMPTY_HASH, std::memory_order_relaxed);
			});
		}

		void
		destroy_buckets()
		{
			std::for_each(buckets, buckets + num_buckets, [](auto &bucket) { bucket.destroy(); });
		}

		static size_t
		get_hash(const key_type &key)
		{
			size_t hash = raw_hash(key);

			return hash < HashBucket::TOMB_STONE_HASH ? hash : 0;
		}

		size_t
		get_ideal_bucket(size_t hash) const
		{
			return hash & (num_buckets - 1);
		}

		size_t
		add_bucket_circular(size_t bucket, size_t link) const
		{
			return (bucket + link) & (num_buckets - 1);
		}

		struct SearchResult
		{
			size_t hash;
			size_t bucket;
			std::atomic<typename Traits::LinkType> *link;
		};

		std::pair<bool, SearchResult>
		search(const key_type &key) const
		{
			SearchResult sres;

			sres.hash   = get_hash(key);
			sres.bucket = get_ideal_bucket(sres.hash);
			sres.link   = std::addressof(link[sres.bucket].first);

			if (buckets[sres.bucket].equals(sres.hash, key))
				return { true, sres };

			if (*sres.link)
			{
				do
				{
					sres.bucket = add_bucket_circular(sres.bucket, *sres.link);
					sres.link   = std::addressof(link[sres.bucket].next);

					if (buckets[sres.bucket].equals(sres.hash, key))
						return { true, sres };
				} while (*sres.link);

				return { false, sres };
			}

			return { false, sres };
		}

		std::optional<typename Traits::LinkType>
		get_bucket_to_insert(SearchResult sres) const
		{
			size_t bucket = sres.bucket;

			for (size_t link = 0; link <= Traits::LINEAR_SEARCH_LIMIT && link < num_buckets;
			     link++, bucket = add_bucket_circular(sres.bucket, link))
			{
				if (buckets[bucket].is_free())
					return { link };
			}

			return std::nullopt;
		}

		enum class InsertResult
		{
			InsertResult_New,
			InsertResult_AlreadyPresent,
			InsertResult_Overflow,
		};

		using MutexLock = std::unique_lock<indexes::utils::Mutex>;

		struct InsertSearchResult
		{
			InsertResult res;
			MutexLock lock;
			typename Traits::LinkType bucket_link;
			SearchResult sres;
		};

		InsertSearchResult
		insert(const key_type &key)
		{
			bool found;
			SearchResult sres;

			while (true)
			{
				std::tie(found, sres) = search(key);

				if (found)
				{
					MutexLock lock{ link[sres.bucket].m };

					if (buckets[sres.bucket].has_value())
					{
						return { InsertResult::InsertResult_AlreadyPresent,
							     std::move(lock),
							     0,
							     sres };
					}
					else
					{
						continue;
					}
				}

				if (auto bucket_link = get_bucket_to_insert(sres))
				{
					sres.bucket = add_bucket_circular(sres.bucket, *bucket_link);

					MutexLock lock{ link[sres.bucket].m };

					if (buckets[sres.bucket].is_free())
					{
						increment_num_values();

						return { InsertResult::InsertResult_New,
							     std::move(lock),
							     *bucket_link,
							     sres };
					}
					else
					{
						continue;
					}
				}
				else
				{
					return { InsertResult::InsertResult_Overflow, {}, 0, sres };
				}
			};
		}
	};

	std::atomic<HashTable *> ht;
	std::atomic<bool> is_migration_in_progress;
	indexes::utils::Mutex migration_mutex;
	std::atomic<int> num_migrations;

	indexes::utils::EpochManager<uint64_t, void> m_gc;

	struct EpochGuard
	{
		concurrent_map *map;

		EpochGuard(concurrent_map *a_map) : map(a_map)
		{
			map->m_gc.enter_epoch();
		}

		~EpochGuard()
		{
			map->m_gc.exit_epoch();
		}
	};

	static void
	insert_key_val_into_ht(HashTable &ht,
	                       typename HashTable::SearchResult &sres,
	                       typename Traits::LinkType bucket_link,
	                       const key_type &key,
	                       const mapped_type &val)
	{
		ht.buckets[sres.bucket].emplace(sres.hash, key, val);
		sres.link->store(bucket_link, std::memory_order_release);
	}

	bool
	try_migrate_table(size_t new_num_buckets)
	{
		auto *new_ht           = new HashTable{ new_num_buckets };
		auto *old_ht           = ht.load();
		size_t old_num_buckets = old_ht->num_buckets;

		for (size_t bucket = 0; bucket < old_num_buckets; bucket++)
		{
			auto &old_bucket = old_ht->buckets[bucket];
			auto &link       = old_ht->link[bucket];
			auto bucket_lock = std::lock_guard<indexes::utils::Mutex>{ link.m };

			if (old_bucket.has_value())
			{
				auto ires = new_ht->insert(old_bucket.key_value.first);

				HT_DEBUG_ASSERT(ires.res != HashTable::InsertResult::InsertResult_AlreadyPresent);

				if (ires.res == HashTable::InsertResult::InsertResult_Overflow)
					return false;

				insert_key_val_into_ht(*new_ht,
				                       ires.sres,
				                       ires.bucket_link,
				                       old_bucket.key_value.first,
				                       old_bucket.key_value.second);
			}
		}

		ht.store(new_ht);
		m_gc.retire_in_new_epoch([&](void *ptr) { delete reinterpret_cast<HashTable *>(ptr); },
		                         reinterpret_cast<void *>(old_ht));

		return true;
	}

	void
	wait_for_migration_to_end()
	{
		std::lock_guard<indexes::utils::Mutex> migration_lock{ migration_mutex };
	}

	void
	migrate_table()
	{
		typename HashTable::MutexLock migration_lock{ migration_mutex, std::try_to_lock };

		if (migration_lock)
		{
			is_migration_in_progress = true;

			size_t new_num_buckets = next_pow_2(size() * 2);

			while (!try_migrate_table(new_num_buckets))
				;

			is_migration_in_progress = false;

			num_migrations++;
		}
		else
		{
			wait_for_migration_to_end();
		}
	}

public:
	static constexpr size_t MINIMUM_CAPACITY = 4;

	concurrent_map(size_t initial_capacity = MINIMUM_CAPACITY)
	    : ht(new HashTable(std::max(initial_capacity, MINIMUM_CAPACITY)))
	    , is_migration_in_progress(false)
	    , migration_mutex()
	    , num_migrations(0)
	{}

	concurrent_map(concurrent_map &&o_map)
	    : ht(o_map.ht.load()), is_migration_in_progress(false), migration_mutex(), num_migrations(0)
	{
		o_map.ht.store(nullptr);
	}

	std::optional<mapped_type>
	Search(const key_type &key)
	{
		EpochGuard eg{ this };
		HashTable &ht      = *this->ht.load();
		auto [found, sres] = ht.search(key);
		std::optional<mapped_type> val{ std::nullopt };

		if (found)
			val = ht.buckets[sres.bucket].key_value.second;

		return val;
	}

	bool
	Insert(const key_type &key, const mapped_type &val)
	{
		while (true)
		{
			EpochGuard eg{ this };
			HashTable &ht = *this->ht.load();
			auto ires     = ht.insert(key);

			if (is_migration_in_progress)
			{
				if (ires.lock)
					ires.lock.unlock();

				wait_for_migration_to_end();
				continue;
			}

			if (ires.res == HashTable::InsertResult::InsertResult_New)
			{
				insert_key_val_into_ht(ht, ires.sres, ires.bucket_link, key, val);
				return true;
			}

			if (ires.res == HashTable::InsertResult::InsertResult_AlreadyPresent)
				return false;

			break;
		}

		migrate_table();

		return Insert(key, val);
	}

	std::optional<mapped_type>
	Upsert(const key_type &key, const mapped_type &val)
	{
		while (true)
		{
			std::optional<mapped_type> oldval{ std::nullopt };
			EpochGuard eg{ this };
			HashTable &ht = *this->ht.load();
			auto ires     = ht.insert(key);

			if (is_migration_in_progress)
			{
				if (ires.lock)
					ires.lock.unlock();

				wait_for_migration_to_end();
				continue;
			}

			if (ires.res == HashTable::InsertResult::InsertResult_New)
			{
				insert_key_val_into_ht(ht, ires.sres, ires.bucket_link, key, val);
				return oldval;
			}

			if (ires.res == HashTable::InsertResult::InsertResult_AlreadyPresent)
			{
				oldval = ht.buckets[ires.sres.bucket].exchange(val);
				return oldval;
			}

			break;
		}

		migrate_table();

		return Upsert(key, val);
	}

	std::optional<mapped_type>
	Update(const key_type &key, const mapped_type &val)
	{
		std::optional<mapped_type> oldval{ std::nullopt };

		while (true)
		{
			EpochGuard eg{ this };
			HashTable &ht      = *this->ht.load();
			auto [found, sres] = ht.search(key);

			if (found)
			{
				typename HashTable::MutexLock lock{ ht.link[sres.bucket].m };

				if (is_migration_in_progress)
				{
					lock.unlock();
					wait_for_migration_to_end();
					continue;
				}

				if (ht.buckets[sres.bucket].has_value())
					oldval = ht.buckets[sres.bucket].exchange(val);
				else
					continue;
			}

			return oldval;
		}
	}

	std::optional<mapped_type>
	Delete(const key_type &key)
	{
		std::optional<mapped_type> val{ std::nullopt };

		while (true)
		{
			EpochGuard eg{ this };
			HashTable &ht      = *this->ht.load();
			auto [found, sres] = ht.search(key);

			if (found)
			{
				auto &bucket = ht.buckets[sres.bucket];

				typename HashTable::MutexLock lock{ ht.link[sres.bucket].m };

				if (is_migration_in_progress)
				{
					lock.unlock();
					wait_for_migration_to_end();
					continue;
				}

				if (bucket.has_value())
				{
					val = bucket.key_value.second;

					bucket.mark_as_empty();
					ht.increment_num_tomb_stones();

					m_gc.retire_in_new_epoch(
					    [&](void *ptr) {
						    reinterpret_cast<typename HashTable::HashBucket *>(ptr)->destroy();
					    },
					    reinterpret_cast<void *>(&bucket));
				}
				else
				{
					continue;
				}
			}

			return val;
		}
	}

	size_t
	size() const
	{
		std::atomic_thread_fence(std::memory_order_seq_cst);

		auto [num_values, num_tomb_stones] = ht.load()->get_stats();

		return num_values - num_tomb_stones;
	}

	int
	load_factor() const
	{
		HashTable &ht                      = *this->ht.load();
		auto [num_values, num_tomb_stones] = ht.get_stats();

		return static_cast<int>(((num_values - num_tomb_stones) * 100) / ht.num_buckets);
	}

	bool
	empty() const
	{
		return size() == 0;
	}
}; // namespace indexes::hashtable
} // namespace indexes::hashtable
