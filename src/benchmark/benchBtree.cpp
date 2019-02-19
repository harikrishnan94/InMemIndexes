#include "indexes/btree/concurrent_map.h"
#include "utils/utils.h"

#include <benchmark/benchmark.h>
#include <inttypes.h>
#include <map>
#include <memory>
#include <random>

constexpr int64_t MAXSIZE = 10 * 1024 * 1024;

static int64_t *
get_rand_values()
{
	static const std::unique_ptr<int64_t[]> keys =
	    utils::generateRandomValues<int64_t>(MAXSIZE, 1, MAXSIZE * 1000);

	return keys.get();
}

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
	indexes::utils::ThreadLocal::RegisterThread();
	indexes::btree::concurrent_map<int64_t, int64_t, LongCompare> map;
	int64_t ind   = 0;
	int64_t *keys = get_rand_values();

	for (auto _ : state)
	{
		auto key = keys[ind];

		map.Insert(key, ind);

		ind++;

		if (ind == state.range(0))
			ind = 0;
	}

	indexes::utils::ThreadLocal::UnregisterThread();
}

static void
BM_BtreeSearch(benchmark::State &state)
{
	indexes::utils::ThreadLocal::RegisterThread();

	int64_t *keys   = get_rand_values();
	int64_t ind     = 0;
	static auto map = [&keys]() {
		indexes::btree::concurrent_map<int64_t, int64_t, LongCompare> map;

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

	indexes::utils::ThreadLocal::UnregisterThread();
}

BENCHMARK(BM_BtreeInsert)->RangeMultiplier(4)->Range(1024, MAXSIZE);
BENCHMARK(BM_BtreeSearch)->RangeMultiplier(4)->Range(1024, MAXSIZE);
