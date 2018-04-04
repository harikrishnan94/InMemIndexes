#include "btree/btree.h"
#include "btree/btree_nodes.h"

#include <assert.h>
#include <stdlib.h>

#define MIN_PAGE_SIZE	   64
#define MAX_PAGE_SIZE	   (8 * 1024)
#define MAPPING_TABLE_SIZE (1024 * 1024)


struct btree_data_t
{
	btree_key_val_info_t  *kv_info;
	int					  pagesize;
	btree_mapping_table_t mtable;

	btree_page_id_t root;
};


bwtree_t
btree_create(int pagesize, btree_key_val_info_t *kv_info)
{
	bwtree_t btree;

	assert(kv_info != NULL);
	assert(pagesize <= MAX_PAGE_SIZE && pagesize >= MIN_PAGE_SIZE);

	btree = malloc(sizeof(struct btree_data_t));

	if (pagesize < MIN_PAGE_SIZE || pagesize > MAX_PAGE_SIZE)
		return NULL;

	btree->kv_info	= kv_info;
	btree->pagesize = pagesize;
	btree->root		= NullPageId;
	btree->mtable	= mapping_table_create(MAPPING_TABLE_SIZE);

	mapping_reserve_num_pids(btree->mtable, 1);
	mapping_table_install_page(btree->mtable, NullPageId, NULL);

	btree->root = mapping_table_alloc_pid(btree->mtable);
	mapping_table_install_page(btree->mtable, btree->root,
							   NewEmptyDataPage(btree, btree->root, true));

	return btree;
}


void
btree_destroy(bwtree_t btree)
{
	mapping_table_destroy(btree->mtable);
	free(btree);
}


int
BwTreeKeySize(bwtree_t btree, const void *key)
{
	return key ? btree->kv_info->key_size(key, btree->kv_info->extra_arg) : 0;
}


int
BwTreeKeyCmp(bwtree_t btree, const void *key1, const void *key2)
{
	Assert(key1 == NULL ? key2 != NULL : true);

	if (key1 == NULL)
		return -1;

	if (key2 == NULL)
		return 1;

	return btree->kv_info->cmp_key(key1, key2, btree->kv_info->extra_arg);
}


btree_page_id_t
BwTreeGetRoot(bwtree_t btree)
{
	return btree->root;
}


void
BwTreeSetRoot(bwtree_t btree, btree_page_id_t pid)
{
	btree->root = pid;
}


int
BwTreePageSize(bwtree_t btree)
{
	return btree->pagesize;
}


btree_mapping_table_t
BwTreeGetMappingTable(bwtree_t btree)
{
	return btree->mtable;
}


bool
btree_insert(bwtree_t btree, const void *key, const void *value)
{
	TraversalContext *tcontext = TraversalContextCreate(btree, key, INSERT_OP);

	TraverseTree(tcontext);
	PostInsertDelta(tcontext->current_snapshot.pid, (btree_key_t) key, value, tcontext);
	AdjustNodeSize(tcontext);
	TraversalContextDestroy(tcontext);

	return true;
}


bool
btree_delete(bwtree_t btree, const void *key, void **value)
{
	TraversalContext *tcontext = TraversalContextCreate(btree, key, DELETE_OP);
	bool			 deleted   = false;
	KeyValPair		 key_val;

	TraverseTree(tcontext);
	key_val = tcontext->key_val;

	if (tcontext->keyPresent)
	{
		PostDeleteDelta(tcontext->current_snapshot.pid, key_val.key, value, tcontext);
		*value	= key_val.value.val;
		deleted = true;
	}

	AdjustNodeSize(tcontext);
	TraversalContextDestroy(tcontext);

	return deleted;
}


bool
btree_find(bwtree_t btree, const void *key, void **value)
{
	TraversalContext *tcontext = TraversalContextCreate(btree, key, LOOKUP_OP);
	KeyValPair		 key_val;
	bool			 keyPresent;

	TraverseTree(tcontext);
	key_val	   = tcontext->key_val;
	keyPresent = tcontext->keyPresent;
	TraversalContextDestroy(tcontext);

	if (keyPresent)
	{
		*value = key_val.value.val;
		return true;
	}

	return false;
}
