#include "indexes/hashtable/concurrent_map.h"
#include "utils/utils.h"

#include <inttypes.h>
#include <memory>
#include <random>
#include <unordered_map>

#include <absl/hash/hash.h>
#include <benchmark/benchmark.h>

constexpr uint64_t MAXSIZE = 10 * 1024 * 1024;

static uint64_t *
get_rand_values()
{
	static const std::unique_ptr<uint64_t[]> keys =
	    utils::generateRandomValues<uint64_t>(MAXSIZE, 1, MAXSIZE * 1000);

	return keys.get();
}

static void
BM_HashTableInsert(benchmark::State &state)
{
	indexes::utils::ThreadLocal::RegisterThread();
	indexes::hashtable::concurrent_map<uint64_t, uint64_t, absl::Hash<uint64_t>> map;
	uint64_t ind             = 0;
	uint64_t *keys           = get_rand_values();
	uint64_t num_unique_keys = static_cast<uint64_t>(state.range(0));

	for (auto _ : state)
	{
		auto key = keys[ind];

		map.Insert(key, ind);

		ind++;

		if (ind == num_unique_keys)
			ind = 0;
	}

	indexes::utils::ThreadLocal::UnregisterThread();
}

static void
BM_HashTableSearch(benchmark::State &state)
{
	indexes::utils::ThreadLocal::RegisterThread();
	uint64_t *keys           = get_rand_values();
	uint64_t ind             = 0;
	uint64_t num_unique_keys = static_cast<uint64_t>(state.range(0));
	static auto map          = [&keys]() {
        indexes::hashtable::concurrent_map<uint64_t, uint64_t, absl::Hash<uint64_t>> map;

        for (uint64_t i = 0; i < MAXSIZE; i++)
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

		if (ind == num_unique_keys)
			ind = 0;
	}

	indexes::utils::ThreadLocal::UnregisterThread();
}

BENCHMARK(BM_HashTableInsert)->RangeMultiplier(4)->Range(1024, MAXSIZE);
BENCHMARK(BM_HashTableSearch)->RangeMultiplier(4)->Range(1024, MAXSIZE);
