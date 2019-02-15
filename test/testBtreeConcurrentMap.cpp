#include "indexes/btree/concurrent_map.h"
#include "sha512.h"
#include "testConcurrentMapUtils.h"

#include <catch.hpp>
#include <gsl/span>
#include <tsl/robin_set.h>

#include <limits>
#include <map>
#include <random>
#include <string>
#include <thread>
#include <vector>

struct IntCompare
{
	int
	operator()(int a, int b) const
	{
		return (a < b) ? -1 : (a > b);
	}
};

struct StringCompare
{
	int
	operator()(const std::string &a, const std::string &b) const
	{
		return a.compare(b);
	}
};

struct LongCompare
{
	int
	operator()(const int64_t &a, const int64_t &b) const
	{
		return (a < b) ? -1 : (a > b);
	}
};

struct btree_small_page_traits : indexes::btree::btree_traits_debug
{
	static constexpr int NODE_SIZE            = 256;
	static constexpr int NODE_MERGE_THRESHOLD = 80;
};

struct btree_traits_string_key : indexes::btree::btree_traits_debug
{
	static constexpr int NODE_SIZE            = 448;
	static constexpr int NODE_MERGE_THRESHOLD = 80;
};

struct btree_medium_page_traits : indexes::btree::btree_traits_default
{
	static constexpr int NODE_SIZE            = 384;
	static constexpr int NODE_MERGE_THRESHOLD = 50;
	static constexpr bool STAT                = true;
};

template class indexes::btree::concurrent_map<int, int, IntCompare, btree_small_page_traits>;
template class indexes::btree::
    concurrent_map<std::string, int, StringCompare, btree_traits_string_key>;

TEST_CASE("BtreeConcurrentMapBasic", "[btree]")
{
	indexes::utils::ThreadLocal::RegisterThread();

	indexes::btree::concurrent_map<int, int, IntCompare, btree_small_page_traits> map;

	int num_keys = 100000;

	std::uniform_int_distribution<int> udist{ 1, num_keys };
	std::random_device r;
	std::seed_seq seed{ r(), r(), r(), r(), r(), r(), r(), r() };
	std::mt19937 rnd(seed);

	std::map<int, int> key_values;

	for (int i = 0; i < num_keys; i++)
	{
		int key = udist(rnd);

		map.Upsert(key, i);
		key_values[key] = i;
	}

	REQUIRE(map.size() == key_values.size());

	// Forward iterator
	{
		auto map_iter     = map.lower_bound(std::numeric_limits<int>::min());
		auto kv_iter      = key_values.lower_bound(std::numeric_limits<int>::min());
		auto map_iter_end = map.upper_bound(std::numeric_limits<int>::max());
		auto kv_iter_end  = key_values.upper_bound(std::numeric_limits<int>::max());

		while (map_iter != map_iter_end)
		{
			REQUIRE(*map_iter++ == *kv_iter++);
		}

		REQUIRE(kv_iter == kv_iter_end);

		auto iter      = map.begin();
		auto iter_copy = iter++;

		REQUIRE(--iter == iter_copy);
		REQUIRE(iter != ++iter_copy);
		REQUIRE(iter != map.end());
	}

	// reverse iterator
	{
		auto map_iter = map.rbegin();
		auto kv_iter  = key_values.rbegin();

		while (map_iter != map.rend())
		{
			REQUIRE(*map_iter++ == *kv_iter++);
		}

		REQUIRE(kv_iter == key_values.rend());

		auto iter      = map.rbegin();
		auto iter_copy = iter++;

		REQUIRE(--iter == iter_copy);
		REQUIRE(iter != ++iter_copy);
		REQUIRE(iter != map.rend());
	}

	for (int i = 0; i < num_keys; i++)
	{
		int key        = udist(rnd);
		auto map_lower = map.lower_bound(key);
		auto map_upper = map.upper_bound(key);
		auto kv_lower  = key_values.lower_bound(key);
		auto kv_upper  = key_values.upper_bound(key);

		if (kv_lower != key_values.end())
			REQUIRE(*map_lower == *kv_lower);
		else
			REQUIRE(map_lower == map.end());

		if (kv_upper != key_values.end())
			REQUIRE(*map_upper == *kv_upper);
		else
			REQUIRE(map_upper == map.end());
	}

	for (const auto &kv : key_values)
	{
		REQUIRE(*map.Delete(kv.first) == kv.second);
	}

	REQUIRE(map.size() == 0);
	REQUIRE(map.Search(0).has_value() == false);

	// lower and upper bound
	{
		int key = udist(rnd);

		map.Insert(key, key);
		REQUIRE(map.lower_bound(key) != map.upper_bound(key));

		map.Delete(key);
		REQUIRE(map.lower_bound(key) == map.upper_bound(key));

		key = std::numeric_limits<int>::min();
		REQUIRE(map.lower_bound(key) == map.upper_bound(key));

		key = std::numeric_limits<int>::max();
		REQUIRE(map.lower_bound(key) == map.upper_bound(key));
	}

	// Iterator operator
	{
		int key = udist(rnd);

		map.Insert(key, key);
		REQUIRE((*map.lower_bound(key)).first == map.lower_bound(key)->first);
	}

	indexes::utils::ThreadLocal::UnregisterThread();
}

TEST_CASE("BtreeConcurrentMapString", "[btree]")
{
	indexes::utils::ThreadLocal::RegisterThread();
	indexes::btree::concurrent_map<std::string, int, StringCompare, btree_traits_string_key> map;

	int num_keys = 100000;

	std::uniform_int_distribution<int> udist;
	std::random_device r;
	std::seed_seq seed{ r(), r(), r(), r(), r(), r(), r(), r() };
	std::mt19937 rnd(seed);

	std::map<std::string, int> key_values;

	for (int i = 0; i < num_keys; i++)
	{
		std::string key = sha512(std::to_string(udist(rnd)));

		map.Upsert(key, i);
		key_values[key] = i;
	}

	REQUIRE(map.size() == key_values.size());

	// Forward iterator
	{
		auto map_iter     = map.begin();
		auto kv_iter      = key_values.begin();
		auto map_iter_end = map.end();
		auto kv_iter_end  = key_values.end();

		while (map_iter != map_iter_end)
		{
			REQUIRE(*map_iter++ == *kv_iter++);
		}

		REQUIRE(kv_iter == kv_iter_end);

		auto iter      = map.begin();
		auto iter_copy = iter++;

		REQUIRE(--iter == iter_copy);
		REQUIRE(iter != ++iter_copy);
		REQUIRE(iter != map.end());
	}

	// reverse iterator
	{
		auto map_iter = map.rbegin();
		auto kv_iter  = key_values.rbegin();

		while (map_iter != map.rend())
		{
			REQUIRE(*map_iter++ == *kv_iter++);
		}

		REQUIRE(kv_iter == key_values.rend());

		auto iter      = map.rbegin();
		auto iter_copy = iter++;

		REQUIRE(--iter == iter_copy);
		REQUIRE(iter != ++iter_copy);
		REQUIRE(iter != map.rend());
	}

	for (int i = 0; i < num_keys; i++)
	{
		std::string key = sha512(std::to_string(udist(rnd)));
		auto map_lower  = map.lower_bound(key);
		auto map_upper  = map.upper_bound(key);
		auto kv_lower   = key_values.lower_bound(key);
		auto kv_upper   = key_values.upper_bound(key);

		if (kv_lower != key_values.end())
			REQUIRE(*map_lower == *kv_lower);
		else
			REQUIRE(map_lower == map.end());

		if (kv_upper != key_values.end())
			REQUIRE(*map_upper == *kv_upper);
		else
			REQUIRE(map_upper == map.end());
	}

	for (const auto &kv : key_values)
	{
		REQUIRE(*map.Delete(kv.first) == kv.second);
	}

	REQUIRE(map.size() == 0);
	REQUIRE(map.Search("").has_value() == false);

	indexes::utils::ThreadLocal::UnregisterThread();
}

TEST_CASE("BtreeConcurrentMapMixed", "[btree]")
{
	MixedMapTest<indexes::btree::concurrent_map<int, int, IntCompare, btree_small_page_traits>>();
}

TEST_CASE("BtreeConcurrentMapConcurrencyRandom", "[btree]")
{
	ConcurrentMapTest<
	    indexes::btree::concurrent_map<int64_t, int64_t, LongCompare, btree_medium_page_traits>>(
	    true);
}

TEST_CASE("BtreeConcurrentMapConcurrencyContented", "[btree]")
{
	ConcurrentMapTest<
	    indexes::btree::concurrent_map<int64_t, int64_t, LongCompare, btree_medium_page_traits>>(
	    false);
}
