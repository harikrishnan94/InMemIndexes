#include "btree/map.h"

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

static void
BM_BtreeInsert(benchmark::State &state)
{
	btree::map<long, long> map;
	long ind = 0;

	for (auto _ : state)
	{
		auto key = keys[ind];

		map[key] = ind;

		ind++;

		if (ind == state.range(0))
			ind = 0;
	}
}

static void
BM_BtreeSearch(benchmark::State &state)
{
	long ind        = 0;
	static auto map = []() {
		btree::map<long, long> map;

		for (long i = 0; i < MAXSIZE; i++)
		{
			auto key = keys[i];

			map[key] = i;
		}

		return map;
	}();

	for (auto _ : state)
	{
		auto key = keys[ind++];

		map.find(key);

		if (ind == state.range(0))
			ind = 0;
	}
}

BENCHMARK(BM_BtreeInsert)->RangeMultiplier(4)->Range(1024, MAXSIZE);
BENCHMARK(BM_BtreeSearch)->RangeMultiplier(4)->Range(1024, MAXSIZE);
