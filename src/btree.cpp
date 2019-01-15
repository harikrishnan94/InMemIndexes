#include "btree/concurrent_map.h"
#include "btree/map.h"

struct LongCompare
{
	int
	operator()(int a, int b) const
	{
		return (a < b) ? -1 : (a > b);
	}
};

template class btree::concurrent_map<long, long, LongCompare>;
