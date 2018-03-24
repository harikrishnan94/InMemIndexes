#include "btree/btree.h"
#include "test/catch.hpp"
#include "test/sha512.h"

#include <iostream>
#include <random>
#include <unordered_map>


TEST_CASE("BtreeBasicTest", "[bwtree]")
{
	auto key_size = [](const void *key, void *extra_arg)
					{
						return static_cast<int>(sizeof(long));
					};
	auto compare_key = [] (const void *a1, const void *a2, void *extra_arg)
					   {
						   return static_cast<int>(*reinterpret_cast<const long *>(a1) -
												   *reinterpret_cast<const long *>(a2));
					   };
	auto get_ptr = [](long v)
				   {
					   return reinterpret_cast<const void *>(v);
				   };
	auto get_key_ptr = [](long *v)
					   {
						   return reinterpret_cast<const void *>(v);
					   };


	btree_key_val_info_t kv_info = { compare_key, key_size, NULL };

	int		btree_pagesize = 8192;
	int		num_keys	   = 1024 * 1024;
	btree_t btree		   = btree_create(btree_pagesize, &kv_info);
	long	key;

	std::uniform_int_distribution<int> uniform_dist{ 1, num_keys };
	std::random_device				   r;
	std::seed_seq					   seed2{ r(), r(), r(), r(), r(), r(), r(), r() };
	std::mt19937					   e2(seed2);

	std::unordered_map<long, long> key_values;

	for (int i = 0; i < num_keys; i++)
	{
		key = uniform_dist(e2);

		btree_insert(btree, get_key_ptr(&key), get_ptr(i));
		key_values[key] = i;
	}

	for (const auto &kv: key_values)
	{
		void *val;

		REQUIRE(btree_delete(btree, &kv.first, &val) == true);
		REQUIRE(reinterpret_cast<long>(val) == kv.second);
		REQUIRE(btree_find(btree, &kv.first, &val) == false);
	}
}


TEST_CASE("BtreeTestString", "[btree]")
{
	auto key_size = [](const void *key, void *extra_arg)
					{
						return static_cast<int>(std::strlen(static_cast<const char *>(key)) + 1);
					};
	auto delete_node = [](void *node, void *extra_arg)
					   {
						   delete[] static_cast<char *>(node);
					   };
	auto compare_key = [] (const void *a1, const void *a2, void *extra_arg)
					   {
						   return std::strcmp(static_cast<const char *>(a1),
											  static_cast<const char *>(a2));
					   };
	auto get_ptr = [](long v)
				   {
					   return reinterpret_cast<const void *>(v);
				   };

	btree_key_val_info_t kv_info = { compare_key, key_size, NULL };

	int		btree_pagesize = 8 * 1024;
	long	num_keys	   = 1 * 1024 * 1024;
	btree_t btree		   = btree_create(btree_pagesize, &kv_info);
	long	key;

	std::unordered_map<const void *, long> key_values;

	for (int i = 0; i < num_keys; i++)
	{
		char *output = new char[2 * SHA512::DIGEST_SIZE + 1];

		sha512(output, &i, sizeof(int));

		btree_insert(btree, static_cast<const void *>(output), get_ptr(i));
		key_values[output] = i;
	}

	for (const auto &kv: key_values)
	{
		void *val;

		REQUIRE(btree_delete(btree, kv.first, &val) == true);
		REQUIRE(reinterpret_cast<long>(val) == kv.second);
	}
}


TEST_CASE("BtreeMixedTest", "[btree]")
{
	auto key_size = [](const void *key, void *extra_arg)
					{
						return static_cast<int>(sizeof(long));
					};
	auto compare_key = [] (const void *a1, const void *a2, void *extra_arg)
					   {
						   return static_cast<int>(*reinterpret_cast<const long *>(a1) -
												   *reinterpret_cast<const long *>(a2));
					   };
	auto get_ptr = [](int v)
				   {
					   return reinterpret_cast<const void *>(v);
				   };
	auto get_key_ptr = [](const int *v)
					   {
						   return reinterpret_cast<const void *>(v);
					   };


	btree_key_val_info_t kv_info = { compare_key, key_size, NULL };

	int		btree_pagesize = 128;
	int		num_operations = 1024 * 1024;
	int		cardinality	   = num_operations * 0.3;
	btree_t btree		   = btree_create(btree_pagesize, &kv_info);

	constexpr auto INSERT_OP = 1;
	constexpr auto LOOKUP_OP = 2;
	constexpr auto DELETE_OP = 3;

	std::random_device r;
	std::seed_seq	   seed2{ r(), r(), r(), r(), r(), r(), r(), r() };
	std::mt19937	   e2(seed2);

	std::uniform_int_distribution<int>				   key_dist{ 1, cardinality };
	std::uniform_int_distribution<decltype(INSERT_OP)> op_dist{ INSERT_OP, DELETE_OP };

	std::unordered_map<int, int> key_values;

	for (int i = 0; i < num_operations; i++)
	{
		const auto key	= key_dist(e2);
		const auto op	= op_dist(e2);
		void	   *val = reinterpret_cast<void *>(key * key_dist(e2));

		switch (op)
		{
			case INSERT_OP:
			{
				if (key_values.count(key))
				{
					REQUIRE(btree_find(btree, get_key_ptr(&key), &val) == true);
					REQUIRE(btree_delete(btree, get_key_ptr(&key), &val) == true);
					key_values.erase(key);
				}

				REQUIRE(btree_insert(btree, get_key_ptr(&key), val) == true);
				key_values[key] = reinterpret_cast<long>(val);
				break;
			}

			case LOOKUP_OP:
			{
				if (key_values.count(key))
				{
					REQUIRE(btree_find(btree, get_key_ptr(&key), &val) == true);
					REQUIRE(reinterpret_cast<long>(val) == key_values[key]);
				}
				else
				{
					REQUIRE(btree_find(btree, get_key_ptr(&key), &val) == false);
				}
				break;
			}

			case DELETE_OP:
			{
				if (key_values.count(key))
				{
					REQUIRE(btree_delete(btree, get_key_ptr(&key), &val) == true);
					REQUIRE(reinterpret_cast<long>(val) == key_values[key]);
					key_values.erase(key);
				}
				else
				{
					REQUIRE(btree_delete(btree, get_key_ptr(&key), &val) == false);
				}
				break;
			}

			default:
				continue;
		}
	}

	for (const auto &kv: key_values)
	{
		void *val;

		REQUIRE(btree_delete(btree, &kv.first, &val) == true);
		REQUIRE(reinterpret_cast<long>(val) == kv.second);
		REQUIRE(btree_find(btree, &kv.first, &val) == false);
	}
}
