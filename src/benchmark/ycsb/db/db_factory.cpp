//
//  db_factory.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "indexes/btree/concurrent_map.h"
#include "indexes/btree/map.h"

#include "db/concurrent_map_db.h"
#include "db/db_factory.h"
#include "db/locked_map_db.h"

#include <map>
#include <string>
#include <tsl/robin_map.h>
#include <unordered_map>

using namespace std;
using ycsbc::DB;
using ycsbc::DBFactory;

template <typename Key,
          typename T,
          typename Hash         = std::hash<Key>,
          typename KeyEqual     = std::equal_to<Key>,
          typename Allocator    = std::allocator<std::pair<Key, T>>,
          typename GrowthPolicy = tsl::rh::power_of_two_growth_policy<2>>
using robin_map = tsl::robin_map<Key, T, Hash, KeyEqual, Allocator, false, GrowthPolicy>;

struct StringCompare
{
	int
	operator()(const std::string &a, const std::string &b) const
	{
		return a.compare(b);
	}
};

DB *
DBFactory::CreateDB(utils::Properties &props)
{
	if (props["dbname"] == "stl_map")
	{
		return new LockedMapDB<SCAN, std::map>;
	}
	else if (props["dbname"] == "stl_umap")
	{
		return new LockedMapDB<NOSCAN, std::unordered_map>;
	}
	else if (props["dbname"] == "robinmap")
	{
		return new LockedMapDB<NOSCAN, robin_map>;
	}
	else if (props["dbname"] == "btree")
	{
		return new LockedMapDB<SCAN, indexes::btree::map>;
	}
	else if (props["dbname"] == "concurrent_btree")
	{
		return new ConcurrentMapDB<
		    SCAN,
		    indexes::btree::concurrent_map<std::string, DB::KVPair *, StringCompare>>;
	}
	else
	{
		return nullptr;
	}
}
