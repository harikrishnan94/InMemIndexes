#include "btree/btree.h"
#include "test/catch.hpp"
#include "test/sha512.h"

#include <iostream>
#include <unordered_map>


TEST_CASE("BtreeBasicTest", "[bwtree]")
{
	auto key_size = [](const void *key, void *extra_arg)
					{
						return static_cast<int>(sizeof(long));
					};
	auto delete_node = [](void *node, void *extra_arg)
					   { };
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


	btree_key_val_info_t kv_info = { compare_key, delete_node, key_size, NULL };

	int		btree_pagesize = 8192;
	long	num_keys	   = 1024 * 1024;
	btree_t btree		   = btree_create(btree_pagesize, &kv_info);
	long	key;

	std::unordered_map<long, long> key_values;

	for (int i = 0; i < num_keys; i++)
	{
		key = rand() % num_keys;

		btree_insert(btree, get_key_ptr(&key), get_ptr(i));
		key_values[key] = i;
	}

	for (const auto &kv: key_values)
	{
		void *val;

		REQUIRE(btree_delete(btree, &kv.first, &val) == true);
		REQUIRE(reinterpret_cast<long>(val) == kv.second);
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

	btree_key_val_info_t kv_info = { compare_key, delete_node, key_size, NULL };

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
