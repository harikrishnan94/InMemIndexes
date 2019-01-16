#include "btree/concurrent_map.h"
#include "sha512.h"

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
	operator()(const long &a, const long &b) const
	{
		return (a < b) ? -1 : (a > b);
	}
};

struct btree_small_page_traits : btree::btree_traits_debug
{
	static constexpr int NODE_SIZE            = 256;
	static constexpr int NODE_MERGE_THRESHOLD = 80;
};

struct btree_traits_string_key : btree::btree_traits_debug
{
	static constexpr int NODE_SIZE            = 384;
	static constexpr int NODE_MERGE_THRESHOLD = 80;
};

struct btree_medium_page_traits : btree::btree_traits_default
{
	static constexpr int NODE_SIZE            = 384;
	static constexpr int NODE_MERGE_THRESHOLD = 50;
	static constexpr bool STAT                = true;
};

template class btree::concurrent_map<int, int, IntCompare, btree_small_page_traits>;
template class btree::concurrent_map<std::string, int, StringCompare, btree_traits_string_key>;

TEST_CASE("BtreeConcurrentMapBasic", "[btree]")
{
	btree::utils::ThreadLocal::RegisterThread();

	btree::concurrent_map<int, int, IntCompare, btree_small_page_traits> map;

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

	btree::utils::ThreadLocal::UnregisterThread();
}

TEST_CASE("BtreeConcurrentMapString", "[btree]")
{
	btree::utils::ThreadLocal::RegisterThread();
	btree::concurrent_map<std::string, int, StringCompare, btree_traits_string_key> map;

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

	btree::utils::ThreadLocal::UnregisterThread();
}

TEST_CASE("BtreeConcurrentMapMixed", "[btree]")
{
	btree::utils::ThreadLocal::RegisterThread();
	btree::concurrent_map<int, int, IntCompare, btree_small_page_traits> map;

	int num_operations = 1024 * 1024;
	int cardinality    = num_operations * 0.1;

	constexpr auto INSERT_OP = 1;
	constexpr auto LOOKUP_OP = 2;
	constexpr auto DELETE_OP = 3;

	std::random_device r;
	std::seed_seq seed{ r(), r(), r(), r(), r(), r(), r(), r() };
	std::mt19937 rnd(seed);

	std::uniform_int_distribution<int> key_dist{ 1, cardinality };
	std::uniform_int_distribution<int> val_dist;
	std::uniform_int_distribution<decltype(INSERT_OP)> op_dist{ INSERT_OP, DELETE_OP };

	std::map<int, int> key_values;

	for (int i = 0; i < num_operations; i++)
	{
		const auto key = key_dist(rnd);
		const auto op  = op_dist(rnd);
		int val;

		switch (op)
		{
			case INSERT_OP:
			{
				val = val_dist(rnd);

				if (key_values.count(key))
				{
					REQUIRE(*map.Search(key) == key_values[key]);
					REQUIRE(*map.Update(key, val) == key_values[key]);
				}
				else
				{
					REQUIRE(map.Insert(key, val) == true);
					REQUIRE(*map.Search(key) == val);
				}

				key_values[key] = val;
				break;
			}

			case LOOKUP_OP:
			{
				if (key_values.count(key))
					REQUIRE(*map.Search(key) == key_values[key]);
				else
					REQUIRE(map.Search(key).has_value() == false);

				break;
			}

			case DELETE_OP:
			{
				if (key_values.count(key))
				{
					REQUIRE(*map.Search(key) == key_values[key]);
					REQUIRE(*map.Delete(key) == key_values[key]);
					key_values.erase(key);
				}
				else
				{
					REQUIRE(map.Delete(key).has_value() == false);
				}
				break;
			}

			default:
				continue;
		}
	}

	REQUIRE(map.size() == key_values.size());

	for (const auto &kv : key_values)
	{
		REQUIRE(*map.Delete(kv.first) == kv.second);
	}

	REQUIRE(map.size() == 0);

	btree::utils::ThreadLocal::UnregisterThread();
}

static std::vector<long>
generateUniqueValues(int size)
{
	std::vector<long> vals;
	std::random_device rd;
	std::mt19937_64 gen{ rd() };
	std::uniform_int_distribution<long> dist(0, size - 1);
	tsl::robin_set<long> used_vals;

	used_vals.reserve(size);
	vals.reserve(size);

	for (int i = 0; i < size; i++)
	{
		long val;

		do
		{
			val = dist(gen);
		} while (used_vals.count(val));

		used_vals.insert(val);
		vals.emplace_back(val);
	}

	return vals;
}

static void
insert_worker(btree::concurrent_map<long, long, LongCompare, btree_medium_page_traits> &map,
              gsl::span<long> vals)
{
	btree::utils::ThreadLocal::RegisterThread();

	for (auto val : vals)
	{
		map.Insert(val, val);
	}

	btree::utils::ThreadLocal::UnregisterThread();
}

enum class OpType
{
	INSERT,
	DELETE,
	DELETE_AND_INSERT
};

static void
delete_worker(btree::concurrent_map<long, long, LongCompare, btree_medium_page_traits> &map,
              gsl::span<long> vals,
              OpType op)
{
	btree::utils::ThreadLocal::RegisterThread();

	for (auto val : vals)
	{
		switch (op)
		{
			case OpType::DELETE:
				map.Delete(val);
				break;

			case OpType::INSERT:
				map.Insert(val, val);
				break;

			case OpType::DELETE_AND_INSERT:
				map.Delete(val);
				map.Insert(val, val);
				break;
		}
	}

	btree::utils::ThreadLocal::UnregisterThread();
}

TEST_CASE("BtreeConcurrentMapConcurrency", "[btree]")
{
	btree::concurrent_map<long, long, LongCompare, btree_medium_page_traits> map;
	constexpr int PER_THREAD_OP_COUNT = 256 * 1024;
	constexpr int NUM_THREADS         = 4;
	std::vector<long> vals            = generateUniqueValues(NUM_THREADS * PER_THREAD_OP_COUNT);

	btree::utils::ThreadLocal::RegisterThread();

	auto run_test = [&](OpType op) {
		std::vector<std::thread> workers;
		int startval = 0;
		int quantum  = vals.size() / NUM_THREADS;

		for (int i = 0; i < NUM_THREADS; i++)
		{
			if (op == OpType::INSERT)
			{
				workers.emplace_back(insert_worker,
				                     std::ref(map),
				                     gsl::span<long>{ vals.data() + startval, quantum });
			}
			else
			{
				workers.emplace_back(delete_worker,
				                     std::ref(map),
				                     gsl::span<long>{ vals.data() + startval, quantum },
				                     op);
			}

			startval += quantum;
		}

		for (auto &worker : workers)
		{
			worker.join();
		}
	};

	// Insert check
	{
		run_test(OpType::INSERT);

		REQUIRE(map.size() == vals.size());

		for (auto val : vals)
		{
			REQUIRE(*map.Search(val) == val);
		}
	}

	// Delete And Insert check
	{
		run_test(OpType::DELETE_AND_INSERT);

		for (auto val : vals)
		{
			REQUIRE(*map.Search(val) == val);
		}

		REQUIRE(map.size() == vals.size());
	}

	// Delete check
	{
		run_test(OpType::DELETE);

		REQUIRE(map.size() == 0);
	}

	btree::utils::ThreadLocal::UnregisterThread();
}
