#include "testConcurrentMapUtils.h"

#include <tsl/robin_set.h>

std::vector<int64_t>
generateUniqueValues(int num_threads, int perthread_count, ConcurrentMapTestWorkload workload)
{
	std::random_device rd;
	std::mt19937_64 gen{ rd() };
	int size = perthread_count * num_threads;
	std::vector<int64_t> vals(size);

	if (workload == ConcurrentMapTestWorkload::WL_CONTENTED)
	{
		for (int i = 0, k = 0; i < size; k++)
		{
			for (int j = 0; j < num_threads; j++)
			{
				vals[j * perthread_count + k] = i++;
			}
		}
	}
	else
	{
		std::uniform_int_distribution<int64_t> dist(0, size - 1);
		tsl::robin_set<int64_t> used_vals;

		used_vals.reserve(size);

		for (int i = 0; i < size; i++)
		{
			int64_t val;

			do
			{
				val = dist(gen);
			} while (used_vals.count(val));

			used_vals.insert(val);
			vals[i] = val;
		}
	}

	return vals;
}
