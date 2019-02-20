// include/hashtable/concurrent_map.h
// Hashtbale implementation

#pragma once

#include "common.h"

namespace indexes::hashtable
{
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

			size_t hash;
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
			emplace(const key_type &key, const mapped_type &val)
			{
				new (&this->key_value) KeyValuePair{ key, val };
			}

			mapped_type
			exchange(const mapped_type &val)
			{
				mapped_type oldval = key_value.second;

				new (&key_value.second) mapped_type{ val };

				return oldval;
			}

			void
			mark_as_empty()
			{
				this->destroy();
				hash = TOMB_STONE_HASH;
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

		size_t num_buckets;
		size_t num_values;
		size_t num_tomb_stones;

		std::unique_ptr<uint8_t[]> mem;
		std::unique_ptr<std::pair<typename Traits::LinkType, typename Traits::LinkType>[]> link;
		HashBucket *buckets;

		HashTable(size_t inital_num_buckets)
		    : num_buckets(inital_num_buckets)
		    , num_values(0)
		    , num_tomb_stones(0)
		    , mem(std::make_unique<uint8_t[]>(num_buckets * sizeof(HashBucket)))
		    , link(std::make_unique<
		           std::pair<typename Traits::LinkType, typename Traits::LinkType>[]>(num_buckets))
		    , buckets(reinterpret_cast<HashBucket *>(mem.get()))
		{
			init_buckets();
		}

		HashTable(HashTable &&ht)
		    : num_buckets(std::move(ht.num_buckets))
		    , num_values(std::move(ht.num_values))
		    , num_tomb_stones(std::move(ht.num_tomb_stones))
		    , mem(std::move(ht.mem))
		    , link(std::move(ht.link))
		    , buckets(std::move(ht.buckets))
		{}

		HashTable &
		operator=(HashTable &&ht)
		{
			num_buckets     = std::move(ht.num_buckets);
			num_values      = std::move(ht.num_values);
			num_tomb_stones = std::move(ht.num_tomb_stones);
			mem             = std::move(ht.mem);
			link            = std::move(ht.link);
			buckets         = ht.buckets;

			ht.buckets         = nullptr;
			ht.num_buckets     = 0;
			ht.num_values      = 0;
			ht.num_tomb_stones = 0;

			return *this;
		}

		~HashTable()
		{
			destroy_buckets();
		}

		void
		init_buckets()
		{
			std::for_each(buckets, buckets + num_buckets, [](auto &bucket) {
				bucket.hash = HashBucket::EMPTY_HASH;
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
		add_bucket_circular(size_t bucket, uint64_t link) const
		{
			return (bucket + link) & (num_buckets - 1);
		}

		struct SearchResult
		{
			size_t hash;
			size_t ideal_bucket;
			size_t bucket;
			typename Traits::LinkType *link;
		};

		std::pair<bool, SearchResult>
		search(const key_type &key) const
		{
			SearchResult sres;

			sres.hash         = get_hash(key);
			sres.ideal_bucket = get_ideal_bucket(sres.hash);
			sres.bucket       = sres.ideal_bucket;
			sres.link         = &link[sres.ideal_bucket].first;

			if (buckets[sres.bucket].equals(sres.hash, key))
				return { true, sres };

			if (*sres.link)
			{
				do
				{
					sres.bucket = add_bucket_circular(sres.bucket, *sres.link);
					sres.link   = &link[sres.bucket].second;

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

			for (uint64_t link = 0; link <= Traits::LINEAR_SEARCH_LIMIT && link < num_buckets;
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

		std::pair<InsertResult, size_t>
		insert(const key_type &key)
		{
			auto [found, sres] = search(key);

			if (found)
				return { InsertResult::InsertResult_AlreadyPresent, sres.bucket };

			if (auto next_bucket = get_bucket_to_insert(sres))
			{
				size_t bucket = add_bucket_circular(sres.bucket, *next_bucket);

				*sres.link           = *next_bucket;
				buckets[bucket].hash = sres.hash;
				num_values++;

				return { InsertResult::InsertResult_New, bucket };
			}

			return { InsertResult::InsertResult_Overflow, 0 };
		}

		std::optional<mapped_type>
		erase(const key_type &key)
		{
			auto [found, sres] = search(key);
			std::optional<mapped_type> val{ std::nullopt };

			if (found)
			{
				auto &bucket = buckets[sres.bucket];

				val = bucket.key_value.second;
				bucket.mark_as_empty();
				num_tomb_stones++;

				return val;
			}

			return val;
		}
	};

	HashTable ht;
	int num_migrations;

	bool
	try_migrate_table(size_t new_num_buckets)
	{
		HashTable new_ht{ new_num_buckets };

		for (typename HashTable::HashBucket *old_bucket = ht.buckets;
		     old_bucket != ht.buckets + ht.num_buckets;
		     old_bucket++)
		{
			if (old_bucket->has_value())
			{
				auto [res, bucket] = new_ht.insert(old_bucket->key_value.first);

				HT_DEBUG_ASSERT(res != HashTable::InsertResult::InsertResult_AlreadyPresent);

				if (res == HashTable::InsertResult::InsertResult_Overflow)
					return false;

				new_ht.buckets[bucket].emplace(old_bucket->key_value.first,
				                               old_bucket->key_value.second);
			}
		}

		ht = std::move(new_ht);
		return true;
	}

	void
	migrate_table()
	{
		size_t new_num_buckets = ht.num_buckets * 2;

		while (!try_migrate_table(new_num_buckets))
			;

		num_migrations++;
	}

public:
	concurrent_map(size_t initial_capacity = 4) : ht(initial_capacity), num_migrations(0)
	{}

	concurrent_map(concurrent_map &&) = default;

	std::optional<mapped_type>
	Search(const key_type &key) const
	{
		auto [found, sres] = ht.search(key);
		std::optional<mapped_type> val{ std::nullopt };

		if (found)
			val = ht.buckets[sres.bucket].key_value.second;

		return val;
	}

	bool
	Insert(const key_type &key, const mapped_type &val)
	{
		auto [res, bucket] = ht.insert(key);

		if (res == HashTable::InsertResult::InsertResult_New)
		{
			ht.buckets[bucket].emplace(key, val);
			return true;
		}
		else if (res == HashTable::InsertResult::InsertResult_AlreadyPresent)
		{
			return false;
		}
		else
		{
			migrate_table();
			return Insert(key, val);
		}
	}

	std::optional<mapped_type>
	Upsert(const key_type &key, const mapped_type &val)
	{
		auto [res, bucket] = ht.insert(key);
		std::optional<mapped_type> oldval{ std::nullopt };

		if (res == HashTable::InsertResult::InsertResult_New)
		{
			ht.buckets[bucket].emplace(key, val);
		}
		else if (res == HashTable::InsertResult::InsertResult_AlreadyPresent)
		{
			oldval = ht.buckets[bucket].exchange(val);
		}
		else
		{
			migrate_table();
			Insert(key, val);
		}

		return oldval;
	}

	std::optional<mapped_type>
	Delete(const key_type &key)
	{
		return ht.erase(key);
	}

	size_t
	size() const
	{
		return ht.num_values - ht.num_tomb_stones;
	}

	int
	load_factor() const
	{
		return static_cast<int>((size() * 100) / ht.num_buckets);
	}

	bool
	empty() const
	{
		return size() == 0;
	}
};
} // namespace indexes::hashtable
