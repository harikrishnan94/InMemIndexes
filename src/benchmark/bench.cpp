#include "indexes/btree/concurrent_map.h"

#include <benchmark/benchmark.h>
#include <inttypes.h>
#include <map>
#include <memory>
#include <random>

constexpr int64_t MAXSIZE                    = 10 * 1024 * 1024;
static const std::unique_ptr<int64_t[]> keys = []() {
	auto keys = std::make_unique<int64_t[]>(MAXSIZE);
	std::random_device r;
	std::seed_seq seed2{ r(), r(), r(), r(), r(), r(), r(), r() };
	std::mt19937 rnd(seed2);
	std::uniform_int_distribution<int64_t> dist{ 1, 1000 * MAXSIZE };

	std::generate(keys.get(), keys.get() + MAXSIZE, [&]() { return dist(rnd); });

	return keys;
}();

struct LongCompare
{
	inline int
	operator()(int64_t a, int64_t b) const
	{
		return (a < b) ? -1 : (a > b);
	}
};

static void
BM_BtreeInsert(benchmark::State &state)
{
	btree::utils::ThreadLocal::RegisterThread();
	btree::concurrent_map<int64_t, int64_t, LongCompare> map;
	int64_t ind = 0;

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

	int64_t ind     = 0;
	static auto map = []() {
		btree::concurrent_map<int64_t, int64_t, LongCompare> map;

		for (int64_t i = 0; i < MAXSIZE; i++)
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
