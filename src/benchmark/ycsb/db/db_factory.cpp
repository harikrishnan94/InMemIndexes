//
//  db_factory.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "btree/map.h"
#include "db/db_factory.h"
#include "db/map_db.h"

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

DB *
DBFactory::CreateDB(utils::Properties &props)
{
	if (props["dbname"] == "stl_map")
		return new MapDB<SCAN, SYNCHRONIZE, std::map>;
	else if (props["dbname"] == "stl_umap")
		return new MapDB<NOSCAN, SYNCHRONIZE, std::unordered_map>;
	else if (props["dbname"] == "robinmap")
		return new MapDB<NOSCAN, SYNCHRONIZE, robin_map>;
	else if (props["dbname"] == "btree")
		return new MapDB<SCAN, SYNCHRONIZE, btree::map>;
	else
		return nullptr;
}
