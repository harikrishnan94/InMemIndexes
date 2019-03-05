#include "indexes/btree/concurrent_map.h"
#include "indexes/hashtable/concurrent_map.h"
#include "utils/uniform_generator.h"
#include "utils/zipfian_generator.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cinttypes>
#include <future>
#include <iostream>
#include <random>
#include <string>
#include <thread>

#include <absl/hash/hash.h>
#include <boost/program_options.hpp>

struct btree_big_page_traits : indexes::btree::btree_traits_default
{
	static constexpr int NODE_SIZE = 4 * 1024;
};

using u64 = uint64_t;

struct LongCompare
{
	int
	operator()(u64 a, u64 b) const
	{
		return (a < b) ? -1 : (a > b);
	}
};

using BtreeMap = indexes::btree::concurrent_map<u64, u64, LongCompare, btree_big_page_traits>;
using HashMap  = indexes::hashtable::concurrent_map<u64, u64, absl::Hash<u64>>;

struct BMArgs
{
	enum class MapType
	{
		BtreeMap,
		HashMap
	};

	MapType map;
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

template <typename MapType>
static auto
insert_values(MapType &map, int64_t rowcount, int num_threads)
{
	std::vector<std::thread> workers;
	Permutation<int64_t> generator(rowcount);

	constexpr int BATCH             = 100;
	std::atomic<int64_t> next_batch = 0;

	auto start = std::chrono::steady_clock::now();

	for (int i = 0; i < num_threads; i++)
	{
		workers.emplace_back([&]() {
			indexes::utils::ThreadLocal::RegisterThread();

			do
			{
				auto start  = next_batch.fetch_add(BATCH);
				auto end    = std::min(start + BATCH, rowcount);
				auto hasher = absl::Hash<int64_t>{};

				for (auto i = start; i < end; i++)
				{
					auto key = generator[i];

					map.Insert(key, hasher(key));
				}
			} while (next_batch < rowcount);

			indexes::utils::ThreadLocal::UnregisterThread();
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

template <typename MapType>
static void
worker(std::promise<uint64_t> result,
       std::string dist,
       MapType &map,
       int64_t rowcount,
       std::atomic<int64_t> &opercount,
       int read_p,
       int insert_p,
       int delete_p,
       int update_p)
{
	indexes::utils::ThreadLocal::RegisterThread();

	enum Oper
	{
		READ,
		INSERT,
		DELETE,
		UPDATE
	};

	auto get_key = [&]() {
		if (dist == "zipf")
		{
			return static_cast<std::function<uint64_t()>>([&]() {
				static thread_local ycsbc::ZipfianGenerator zgenerator{ 0,
					                                                    static_cast<uint64_t>(
					                                                        rowcount) };
				return zgenerator.NextUnlocked();
			});
		}
		else if (dist == "uniform")
		{
			return static_cast<std::function<uint64_t()>>([&]() {
				static thread_local ycsbc::UniformGenerator ugenerator{ 0,
					                                                    static_cast<uint64_t>(
					                                                        rowcount) };
				return ugenerator.NextUnlocked();
			});
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
	auto hasher                 = absl::Hash<int64_t>{};

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

	indexes::utils::ThreadLocal::UnregisterThread();
}

template <typename MapType>
static void
do_benchmark(const BMArgs &args)
{
	MapType map;

	{
		auto millis_elapsed = insert_values(map, args.rowcount, args.num_threads);

		std::cout << "Populated " << args.rowcount << " values in " << millis_elapsed << " ms\n";
		std::cout << "Insert Transaction throughput (KTPS) : " << args.rowcount / millis_elapsed
		          << std::endl;
	}

	{
		std::vector<std::thread> workers;
		std::vector<std::future<uint64_t>> results;
		std::atomic<int64_t> shared_opercount{ args.opercount };
		uint64_t num_successful_ops = 0;

		auto start = std::chrono::steady_clock::now();

		for (int i = 0; i < args.num_threads; i++)
		{
			std::promise<uint64_t> result;

			results.emplace_back(result.get_future());
			workers.emplace_back(worker<MapType>,
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

	// map.reclaim_all();
}

static void
do_benchmark(const BMArgs &args)
{
	switch (args.map)
	{
		case BMArgs::MapType::HashMap:
			do_benchmark<HashMap>(args);
			break;

		case BMArgs::MapType::BtreeMap:
			do_benchmark<BtreeMap>(args);
			break;
	}
}

int
main(int argc, char *argv[])
{
	namespace po = boost::program_options;

	po::options_description options{ "Random integer benchmark" };

	options.add_options()("help,h", "Display this help message");

	options.add_options()("map,m", po::value<std::string>()->required(), "Maptype Hash/Btree");

	options.add_options()("rowcount,R",
	                      po::value<int64_t>()->required(),
	                      "Record count")("opercount,O",
	                                      po::value<int64_t>()->required(),
	                                      "Operation count");

	options.add_options()("threads,T",
	                      po::value<int>()->required(),
	                      "# threads to use for benchmark");

	options.add_options()("dist,D",
	                      po::value<std::string>()->required(),
	                      "Distribution of data. One of `uniform` or `zipf` distribution");

	options.add_options()("read,r", po::value<int>()->required(), "Read proportion")(
	    "insert,i",
	    po::value<int>()->required(),
	    "Insert proportion")("delete,d",
	                         po::value<int>()->required(),
	                         "Delete proportion")("update,u",
	                                              po::value<int>()->required(),
	                                              "Update proportion");

	try
	{
		po::variables_map vm;

		po::store(po::command_line_parser(argc, argv).options(options).run(), vm);
		po::notify(vm);

		BMArgs args;
		std::string maptype;

		maptype          = vm["map"].as<std::string>();
		args.rowcount    = vm["rowcount"].as<int64_t>();
		args.opercount   = vm["opercount"].as<int64_t>();
		args.num_threads = vm["threads"].as<int>();
		args.dist        = vm["dist"].as<std::string>();
		args.read_p      = vm["read"].as<int>();
		args.insert_p    = vm["insert"].as<int>();
		args.delete_p    = vm["delete"].as<int>();
		args.update_p    = vm["update"].as<int>();

		if (args.rowcount % args.num_threads != 0)
			args.rowcount = ((args.rowcount / args.num_threads) + 1) * args.num_threads;

		if (!args.check_operations_proportions())
			throw std::string{ "Sum of Read, Insert, Delete and Update proportions should match" };

		if (!args.check_distribution())
			throw std::string{ "Unsupported data distribution requested" };

		std::transform(maptype.begin(), maptype.end(), maptype.begin(), ::tolower);

		if (maptype == "hash")
			args.map = BMArgs::MapType::HashMap;
		else if (maptype == "btree")
			args.map = BMArgs::MapType::BtreeMap;

		do_benchmark(args);
	}
	catch (const po::error &ex)
	{
		std::cerr << "ERROR: " << ex.what() << std::endl;
		std::cerr << options << std::endl;
	}
	catch (...)
	{
		std::cerr << options << std::endl;
	}
}
