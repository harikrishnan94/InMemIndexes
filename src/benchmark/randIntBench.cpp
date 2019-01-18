#include "btree/concurrent_map.h"
#include "utils/uniform_generator.h"
#include "utils/zipfian_generator.h"

#include <atomic>
#include <chrono>
#include <cinttypes>
#include <cxxopts.hpp>
#include <future>
#include <iostream>
#include <random>
#include <thread>

struct BMArgs
{
	std::string dist;
	int64_t rowcount;
	int64_t opercount;
	int num_threads;
	int read_p;
	int insert_p;
	int delete_p;
	int update_p;

	bool
	check_operations_proportions() const
	{
		return read_p + insert_p + delete_p + update_p == 100;
	}

	bool
	check_distribution() const
	{
		return dist == "zipf" || dist == "uniform";
	}
};

struct btree_big_page_traits : btree::btree_traits_default
{
	static constexpr int NODE_SIZE = 4 * 1024;
};

struct LongCompare
{
	int
	operator()(int a, int b) const
	{
		return (a < b) ? -1 : (a > b);
	}
};

using Map = btree::concurrent_map<long, long, LongCompare, btree_big_page_traits>;

static uint64_t
hasher(uint64_t k)
{
#define BIG_CONSTANT(x) (x##LLU)
	k ^= k >> 33;
	k *= BIG_CONSTANT(0xff51afd7ed558ccd);
	k ^= k >> 33;
	k *= BIG_CONSTANT(0xc4ceb9fe1a85ec53);
	k ^= k >> 33;

	return k;
}

/*
 * class Permutation - Generates permutation of k numbers, ranging from
 *                     0 to k - 1
 *
 * This is usually used to randomize insert() to a data structure such that
 *   (1) Each Insert() call could hit the data structure
 *   (2) There is no extra overhead for failed insertion because all keys are
 *       unique
 */
template <typename IntType>
class Permutation
{
private:
	std::vector<IntType> data;

public:
	/*
	 * Generate() - Generates a permutation and store them inside data
	 */
	void
	Generate(size_t count, IntType start = IntType{ 0 })
	{
		// Extend data vector to fill it with elements
		data.resize(count);

		// This function fills the vector with IntType ranging from
		// start to start + count - 1
		std::iota(data.begin(), data.end(), start);

		// The two arguments define a closed interval, NOT open interval
		std::random_device device;
		std::default_random_engine engine{ device() };
		std::uniform_int_distribution<IntType> dist(0, count - 1);

		// Then swap all elements with a random position
		for (size_t i = 0; i < count; i++)
		{
			IntType random_key = dist(engine);

			// Swap two numbers
			std::swap(data[i], data[random_key]);
		}

		return;
	}

	/*
	 * Constructor
	 */
	Permutation()
	{}

	/*
	 * Constructor - Starts the generation process
	 */
	Permutation(size_t count, IntType start = IntType{ 0 })
	{
		Generate(count, start);

		return;
	}

	/*
	 * operator[] - Accesses random elements
	 *
	 * Note that return type is reference type, so element could be
	 * modified using this method
	 */
	inline IntType &operator[](size_t index)
	{
		return data[index];
	}

	inline const IntType &operator[](size_t index) const
	{
		return data[index];
	}
};

static auto
insert_values(Map &map, int64_t rowcount, int num_threads)
{
	std::vector<std::thread> workers;
	Permutation<long> generator(rowcount);

	constexpr int BATCH             = 100;
	std::atomic<int64_t> next_batch = 0;

	auto start = std::chrono::steady_clock::now();

	for (int i = 0; i < num_threads; i++)
	{
		workers.emplace_back([&]() {
			btree::utils::ThreadLocal::RegisterThread();

			do
			{
				auto start = next_batch.fetch_add(BATCH);
				auto end   = std::min(start + BATCH, rowcount);

				for (auto i = start; i < end; i++)
				{
					auto key = generator[i];

					map.Insert(key, hasher(key));
				}
			} while (next_batch < rowcount);

			btree::utils::ThreadLocal::UnregisterThread();
		});
	}

	for (auto &worker : workers)
	{
		worker.join();
	}

	auto end               = std::chrono::steady_clock::now();
	auto populate_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

	return std::max(populate_duration.count(), static_cast<std::chrono::milliseconds::rep>(1));
}

static void
worker(std::promise<uint64_t> result,
       std::string dist,
       Map &map,
       int64_t rowcount,
       std::atomic_int64_t &opercount,
       int read_p,
       int insert_p,
       int delete_p,
       int update_p)
{
	btree::utils::ThreadLocal::RegisterThread();

	enum Oper
	{
		READ,
		INSERT,
		DELETE,
		UPDATE
	};

	ycsbc::ZipfianGenerator zgenerator{ 0, static_cast<uint64_t>(rowcount) };
	ycsbc::UniformGenerator ugenerator{ 0, static_cast<uint64_t>(rowcount) }; // TODO

	auto get_key = [&]() {
		if (dist == "zipf")
		{
			return static_cast<std::function<uint64_t()>>(
			    [&]() { return zgenerator.NextUnlocked(); });
		}
		else if (dist == "uniform")
		{
			return static_cast<std::function<uint64_t()>>(
			    [&]() { return ugenerator.NextUnlocked(); });
		}
		else
		{
			return static_cast<std::function<uint64_t()>>([]() {
				std::terminate();
				return 0;
			});
		}
	}();

	std::random_device rd;
	std::mt19937_64 gen{ rd() }; // Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<int> opdis(0, 99);
	uint64_t num_successful_ops = 0;
	constexpr int BATCH         = 100;

	auto get_op = [&]() {
		auto num = opdis(gen);

		return num < read_p ? READ
		                    : (num < (read_p + insert_p)
		                           ? INSERT
		                           : (num < (read_p + insert_p + delete_p) ? DELETE : UPDATE));
	};

	while (opercount-- > 0)
	{
		auto local_opercount = BATCH;

		while (local_opercount--)
		{
			switch (get_op())
			{
				case READ:
					if (map.Search(get_key()))
						num_successful_ops++;

					break;

				case DELETE:
					if (map.Delete(get_key()))
						num_successful_ops++;

					break;

				case INSERT:
					if (map.Insert(get_key(), hasher(local_opercount)))
						num_successful_ops++;

					break;

				case UPDATE:
					if (map.Update(get_key(), hasher(local_opercount)))
						num_successful_ops++;

					break;
			}
		}

		opercount -= BATCH;
	}

	result.set_value(num_successful_ops);

	btree::utils::ThreadLocal::UnregisterThread();
}

static void
do_benchmark(const BMArgs &args)
{
	Map map;

	{
		auto millis_elapsed = insert_values(map, args.rowcount, args.num_threads);

		std::cout << "Populated " << args.rowcount << " values in " << millis_elapsed << " ms\n";
		std::cout << "Insert Transaction throughput (KTPS) : " << args.rowcount / millis_elapsed
		          << std::endl;
	}

	{
		std::vector<std::thread> workers;
		std::vector<std::future<uint64_t>> results;
		std::atomic_int64_t shared_opercount{ args.opercount };
		uint64_t num_successful_ops = 0;

		auto start = std::chrono::steady_clock::now();

		for (int i = 0; i < args.num_threads; i++)
		{
			std::promise<uint64_t> result;

			results.emplace_back(result.get_future());
			workers.emplace_back(worker,
			                     std::move(result),
			                     args.dist,
			                     std::ref(map),
			                     args.rowcount,
			                     std::ref(shared_opercount),
			                     args.read_p,
			                     args.insert_p,
			                     args.delete_p,
			                     args.update_p);
		}

		for (auto &result : results)
		{
			num_successful_ops += result.get();
		}

		auto end               = std::chrono::steady_clock::now();
		auto populate_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
		auto millis_elapsed =
		    std::max(populate_duration.count(), static_cast<std::chrono::milliseconds::rep>(1));

		std::cout << "Completed " << num_successful_ops << " transactions out of " << args.opercount
		          << " in " << millis_elapsed << " ms\n";
		std::cout << "Transaction throughput (KTPS) : " << args.opercount / millis_elapsed
		          << std::endl;

		for (auto &worker : workers)
		{
			worker.join();
		}
	}

	map.reclaim_all();
}

int
main(int argc, char *argv[])
{
	cxxopts::Options options(argv[0], "Random integer benchmark");

	options.add_options()("R,rowcount",
	                      "Record count",
	                      cxxopts::value<int64_t>())("O,opercount",
	                                                 "Operation count",
	                                                 cxxopts::value<int64_t>());
	options.add_options()("T,threads", "# threads to use for benchmark", cxxopts::value<int>());
	options.add_options()("D,dist",
	                      "Distribution of data. One of `uniform` or `zipf` distribution",
	                      cxxopts::value<std::string>());
	options.add_options()("r,read",
	                      "Read proportion",
	                      cxxopts::value<int>())("i,insert",
	                                             "Insert proportion",
	                                             cxxopts::value<
	                                                 int>())("d,delete",
	                                                         "Delete proportion",
	                                                         cxxopts::value<
	                                                             int>())("u,update",
	                                                                     "Update proportion",
	                                                                     cxxopts::value<int>());

	try
	{
		auto result = options.parse(argc, argv);

		if (!result.count("rowcount"))
			throw std::string{ "Record count is required" };

		if (!result.count("opercount"))
			throw std::string{ "Operation count" };

		if (!result.count("threads"))
			throw std::string{ "# threads is required" };

		if (!result.count("dist"))
			throw std::string{ "Distribution of data is required" };

		if (!result.count("read"))
			throw std::string{ "Read proportion" };

		if (!result.count("insert"))
			throw std::string{ "Insert proportion" };

		if (!result.count("delete"))
			throw std::string{ "Delete proportion" };

		if (!result.count("update"))
			throw std::string{ "Update proportion" };

		BMArgs args;

		args.rowcount    = result["rowcount"].as<int64_t>();
		args.opercount   = result["opercount"].as<int64_t>();
		args.num_threads = result["threads"].as<int>();
		args.dist        = result["dist"].as<std::string>();
		args.read_p      = result["read"].as<int>();
		args.insert_p    = result["insert"].as<int>();
		args.delete_p    = result["delete"].as<int>();
		args.update_p    = result["update"].as<int>();

		if (args.rowcount % args.num_threads != 0)
			args.rowcount = ((args.rowcount / args.num_threads) + 1) * args.num_threads;

		if (!args.check_operations_proportions())
			throw std::string{ "Sum of Read, Insert, Delete and Update proportions should match" };

		if (!args.check_distribution())
			throw std::string{ "Unsupported data distribution requested" };

		do_benchmark(args);
	}
	catch (const std::string &e)
	{
		std::cerr << "ERROR: " << e << std::endl;
		std::cerr << options.help() << std::endl;
	}
	catch (...)
	{
		std::cerr << options.help() << std::endl;
	}
}
