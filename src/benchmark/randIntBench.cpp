#include "btree/concurrent_map.h"
#include "utils/uniform_generator.h"
#include "utils/zipfian_generator.h"

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

struct LongCompare : std::binary_function<long, long, int>
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
};

static void
insert_values(std::string dist, Map &map, int64_t rowcount)
{
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

	while (rowcount)
	{
		auto key = get_key();

		map.Insert(key, hasher(key));
		rowcount--;
	}
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
}

static void
do_benchmark(const BMArgs &args)
{
	Map map;

	{
		auto start = std::chrono::steady_clock::now();
		insert_values(args.dist, map, args.rowcount);
		auto end               = std::chrono::steady_clock::now();
		auto populate_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
		auto millis_elapsed =
		    std::max(populate_duration.count(), static_cast<std::chrono::milliseconds::rep>(1));

		std::cout << "Populated " << args.rowcount << " values in " << millis_elapsed << " ms\n";
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

		std::cout << "Completed " << num_successful_ops << " out of " << args.opercount << " in "
		          << millis_elapsed << " ms\n";
		std::cout << "Transaction throughput (KTPS) : " << args.opercount / millis_elapsed
		          << std::endl;

		for (auto &worker : workers)
		{
			worker.join();
		}
	}
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
