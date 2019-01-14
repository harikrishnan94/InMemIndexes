#include "btree/concurrent_map.h"

#include <benchmark/benchmark.h>
#include <map>
#include <memory>
#include <random>

constexpr long MAXSIZE                    = 10 * 1024 * 1024;
static const std::unique_ptr<long[]> keys = []() {
	auto keys = std::make_unique<long[]>(MAXSIZE);
	std::random_device r;
	std::seed_seq seed2{ r(), r(), r(), r(), r(), r(), r(), r() };
	std::mt19937 rnd(seed2);
	std::uniform_int_distribution<long> dist{ 1, 1000 * MAXSIZE };

	std::generate(keys.get(), keys.get() + MAXSIZE, [&]() { return dist(rnd); });

	return keys;
}();

struct IntCompare : std::binary_function<long, long, int>
{
	inline int
	operator()(long a, long b) const
	{
		return (a < b) ? -1 : (a > b);
	}
};

static void
BM_BtreeInsert(benchmark::State &state)
{
	btree::utils::ThreadLocal::RegisterThread();
	btree::concurrent_map<long, long, IntCompare> map;
	long ind = 0;

	for (auto _ : state)
	{
		auto key = keys[ind];

		map.Insert(key, ind);

		ind++;

		if (ind == state.range(0))
			ind = 0;
	}

	btree::utils::ThreadLocal::UnregisterThread();
}

static void
BM_BtreeSearch(benchmark::State &state)
{
	btree::utils::ThreadLocal::RegisterThread();

	long ind        = 0;
	static auto map = []() {
		btree::concurrent_map<long, long, IntCompare> map;

		for (long i = 0; i < MAXSIZE; i++)
		{
			auto key = keys[i];

			map.Insert(key, i);
		}

		return map;
	}();

	for (auto _ : state)
	{
		auto key = keys[ind++];

		map.Search(key);

		if (ind == state.range(0))
			ind = 0;
	}

	btree::utils::ThreadLocal::UnregisterThread();
}

BENCHMARK(BM_BtreeInsert)->RangeMultiplier(4)->Range(1024, MAXSIZE);
BENCHMARK(BM_BtreeSearch)->RangeMultiplier(4)->Range(1024, MAXSIZE);
