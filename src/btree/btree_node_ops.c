#include "btree/btree_nodes.h"
#include <fixed_stack.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef ENABLE_DUMP_BTREE

static void			   dump_level(bwtree_t btree, btree_page_id_t pid);
static btree_page_id_t dump_page(bwtree_t btree, btree_page_id_t pid);

#endif

#define GetPrevSlot(slot) ((slot) - 1)
#define PidToPtr(pid)	  ((void *) (uintptr_t) (pid))
#define PtrToPid(ptr)	  ((btree_page_id_t) (uintptr_t) (ptr))
#define ELEMSIZE(keysize) (MAXALIGN(keysize) + sizeof(btree_key_t) + sizeof(btree_value_t))
#define IS_LEAF(nodetype) ((nodetype) & (LEAF_TYPE))
#define FOLLOW_RIGHT_LINK true
#define IGNORE_RIGHT_LINK false

static void			 LoadLatestSnapshot(btree_page_id_t node_id, TraversalContext *tcontext);
static void			 UpdateParentNodeId(btree_page_id_t node_id, TraversalContext *tcontext);
static void			 LoadLatestParentSnapshot(TraversalContext *tcontext);
static BwTreeNodeTag GetSnapshotType(TraversalContext *tcontext);
static btree_value_t NavigateSnapshot(DeltaNode *page, bool follow_rightlink,
									  TraversalContext *tcontext);
static bool NavigateNode(btree_page_id_t node_id, TraversalContext *tcontext,
						 btree_value_t *value);

static void RecycleDeltaChain(DeltaNode *node);

static fstack_t *GetReversedDeltaChain(DeltaNode *node);
static void		ConsolidatePage(btree_page_id_t nodeid, TraversalContext *tcontext);
static void		CollectValuesFromPage(DeltaNode *page, btree_key_t *keys, void *values,
									  bool follow_rightlink, TraversalContext *tcontext);

static void InsertIntoSortedArray(bwtree_t btree, btree_key_t *keys, btree_value_t *values,
								  btree_key_t key, btree_value_t value, key_count_t num_keys);
static void DeleteFromSortedArray(bwtree_t btree, btree_key_t *keys, btree_value_t *values,
								  btree_key_t key, btree_value_t value, key_count_t num_keys);
static page_slot_t bsearch_page(bwtree_t btree, const btree_key_t scan_key, const
								btree_key_t *keys, key_count_t num_keys, bool *keyPresent);

static page_slot_t GetSplitSpot(DeltaNode *page);
static void		   CollectValuesFromRightPage(SplitNodeDelta *split_delta, btree_key_t *keys,
											  void *values, TraversalContext *tcontext);
static NodeMeta GetSplitMeta(bwtree_t btree, NodeMeta *meta, btree_page_id_t pid,
							 page_slot_t split_slot, btree_key_t *keys, btree_value_t *values);
static bool SplitPage(btree_page_id_t nodeid, TraversalContext *tcontext);
static bool PostSplitDelta(DeltaNode *oldpage, btree_key_t split_key, btree_page_id_t split_page_id,
						   TraversalContext *tcontext);

static bool can_merge_pages(bwtree_t btree, DeltaNode *left_page, DeltaNode *right_page,
							btree_key_t merge_key);
static btree_page_id_t GetLeftSibiling(btree_key_t low_key, btree_key_t *merge_key,
									   TraversalContext *tcontext);
static bool			  PostRemoveNodeDelta(DeltaNode *oldpage, TraversalContext *tcontext);
static AbortNodeDelta *PostAbortNodeDelta(DeltaNode *parent_page, DeltaNode *child_page,
										  TraversalContext *tcontext);
static void RemoveAbortNodeDelta(DeltaNode *page, AbortNodeDelta *abort_node,
								 TraversalContext *tcontext);
static bool PostMergeDelta(DeltaNode *oldpage, btree_key_t merge_key, DeltaNode *merge_node,
						   TraversalContext *tcontext);
static bool MergePages(btree_page_id_t right_nodeid, TraversalContext *tcontext);

TraversalContext *
TraversalContextCreate(bwtree_t btree, const void *search_key, int traversal_op)
{
	TraversalContext *tcontext = malloc(sizeof(TraversalContext));

	memset(tcontext, 0, sizeof(TraversalContext));

	tcontext->btree		   = btree;
	tcontext->search_key   = (btree_key_t) search_key;
	tcontext->traversal_op = traversal_op;

	return tcontext;
}


void
TraversalContextDestroy(TraversalContext *tcontext)
{
	free(tcontext);
}


void
TraverseTree(TraversalContext *tcontext)
{
	btree_page_id_t nodeid = BwTreeGetRoot(tcontext->btree);

	while (true)
	{
		btree_value_t value;

		Assert(nodeid != NullPageId);

		if (NavigateNode(nodeid, tcontext, &value))
		{
			nodeid = BwTreeGetRoot(tcontext->btree);
			continue;
		}

		if (IS_LEAF(GetSnapshotType(tcontext)))
			break;

		UpdateParentNodeId(nodeid, tcontext);
		nodeid = value.downlink;
	}
}


bool
AdjustNodeSize(TraversalContext *tcontext)
{
	btree_page_id_t nodeid			 = tcontext->current_snapshot.pid;
	bwtree_t		btree			 = tcontext->btree;
	bool			consolidate_page = false;
	DeltaNode		*node;

	LoadLatestSnapshot(nodeid, tcontext);
	node = tcontext->current_snapshot.page;

	if (DeltaChainLen(node) >= (MAX_DELTA_NODES_PER_PAGE - 1))
		consolidate_page = true;

	if (NodePageSize(node) > BwTreePageSize(btree))
		consolidate_page = SplitPage(nodeid, tcontext);

	/* Don't care if root is underfull */
	if (nodeid != BwTreeGetRoot(btree) &&
		NodePageSize(node) < BwTreePageSize(btree) / 2)
	{
		if (MergePages(nodeid, tcontext))
		{
			consolidate_page = true;
			nodeid			 = tcontext->left_node_id;
		}
	}

	if (consolidate_page)
		ConsolidatePage(nodeid, tcontext);

	return consolidate_page;
}


bool
PostInsertDelta(btree_page_id_t nodeid, btree_key_t key, const void *value,
				TraversalContext *tcontext)
{
	DeltaNode	  *node;
	InsertDelta	  *delta;
	btree_value_t insert_value;

	LoadLatestSnapshot(nodeid, tcontext);

	node = tcontext->current_snapshot.page;
	Assert(node != NULL);

	insert_value.val = (void *) value;
	delta			 = NewInsertDelta(tcontext->btree, GetNodeMeta(node), node, key, insert_value);

	return mapping_table_install_page_to_replace(BwTreeGetMappingTable(tcontext->btree), nodeid,
												 node, delta);
}


bool
PostDeleteDelta(btree_page_id_t nodeid, btree_key_t key, const void *value,
				TraversalContext *tcontext)
{
	DeltaNode	  *node;
	DeleteDelta	  *delta;
	btree_value_t delete_value;

	LoadLatestSnapshot(nodeid, tcontext);

	node = tcontext->current_snapshot.page;
	Assert(node != NULL);

	delete_value.val = (void *) value;
	delta			 = NewDeleteDelta(tcontext->btree, GetNodeMeta(node), node, key, delete_value);

	return mapping_table_install_page_to_replace(BwTreeGetMappingTable(tcontext->btree), nodeid,
												 node, delta);
}


static void
LoadLatestSnapshot(btree_page_id_t node_id, TraversalContext *tcontext)
{
	btree_mapping_table_t mtable;

	mtable							= BwTreeGetMappingTable(tcontext->btree);
	tcontext->current_snapshot.pid	= node_id;
	tcontext->current_snapshot.page = mapping_table_lookup_page(mtable, node_id);
}


static void
LoadLatestParentSnapshot(TraversalContext *tcontext)
{
	btree_mapping_table_t mtable;

	mtable						   = BwTreeGetMappingTable(tcontext->btree);
	tcontext->parent_snapshot.page = mapping_table_lookup_page(mtable,
															   tcontext->parent_snapshot.pid);
}


static BwTreeNodeTag
GetSnapshotType(TraversalContext *tcontext)
{
	Assert(tcontext->current_snapshot.pid != 0);
	Assert(tcontext->current_snapshot.page != NULL);

	return NodeTag(tcontext->current_snapshot.page);
}


static void
UpdateParentNodeId(btree_page_id_t node_id, TraversalContext *tcontext)
{
	tcontext->parent_snapshot.pid = node_id;
}


static bool
NavigateNode(btree_page_id_t node_id, TraversalContext *tcontext, btree_value_t *value)
{
	LoadLatestSnapshot(node_id, tcontext);

	/* Skip adjusting pageSize on the way during lookup operations */
	if (tcontext->traversal_op != LOOKUP_OP)
		if (AdjustNodeSize(tcontext))
			return true;

	*value = NavigateSnapshot(tcontext->current_snapshot.page, FOLLOW_RIGHT_LINK, tcontext);
	return false;
}


static btree_value_t
NavigateSnapshot(DeltaNode *page, bool follow_rightlink, TraversalContext *tcontext)
{
	bwtree_t	  btree;
	NodeMeta	  *meta;
	btree_key_t	  *keys;
	btree_value_t *values;
	bool		  keyPresent;
	page_slot_t	  slot;

	btree  = tcontext->btree;
	page   = tcontext->current_snapshot.page;
	meta   = GetNodeMeta(page);
	keys   = malloc(sizeof(btree_key_t) * (meta->num_keys + (meta->delta_chain_len + 1)));
	values = malloc(sizeof(btree_value_t) * (meta->num_keys + (meta->delta_chain_len + 1)));

	CollectValuesFromPage(page, keys, values, follow_rightlink, tcontext);
	slot = bsearch_page(btree, tcontext->search_key, keys, meta->num_keys, &tcontext->keyPresent);
	slot = IS_LEAF(GetSnapshotType(tcontext)) ? slot : GetPrevSlot(slot);

	tcontext->key_val.key	= keys[slot];
	tcontext->key_val.value = values[slot];

	free(keys);
	free(values);

	return tcontext->key_val.value;
}


static bool
SplitPage(btree_page_id_t nodeid, TraversalContext *tcontext)
{
	bwtree_t			  btree;
	NodeMeta			  *meta;
	NodeMeta			  split_page_meta;
	DeltaNode			  *page;
	btree_key_t			  *keys;
	btree_value_t		  *values;
	btree_key_t			  *split_key;
	btree_page_id_t		  split_page_id;
	DataPage			  *split_page;
	bool				  split_delta_ret;
	page_slot_t			  split_slot;
	btree_mapping_table_t mtable;

	btree		  = tcontext->btree;
	mtable		  = BwTreeGetMappingTable(btree);
	page		  = tcontext->current_snapshot.page;
	meta		  = GetNodeMeta(page);
	keys		  = malloc(sizeof(btree_key_t) * (meta->num_keys + (DeltaChainLen(page) + 1)));
	values		  = malloc(sizeof(btree_value_t) * (meta->num_keys + (DeltaChainLen(page) + 1)));
	split_slot	  = GetSplitSpot(page);
	split_page_id = mapping_table_alloc_pid(mtable);

	CollectValuesFromPage(page, keys, values, IGNORE_RIGHT_LINK, tcontext);

	split_key		= keys[split_slot];
	split_page_meta = GetSplitMeta(btree, meta, split_page_id, split_slot, keys, values);
	split_page		= NewDataPage(btree, &split_page_meta, keys + split_slot + 1,
								  values + split_slot + 1);

	mapping_table_install_page(mtable, split_page_id, split_page);
	split_delta_ret = PostSplitDelta(page, split_key, split_page_id, tcontext);

	/*
	 * When splitting an index page, split key removed from original page,
	 * and is pushed up to parent page. So, include a -Inf key in splitted page
	 * with split key's downlink
	 */
	if (NodeIsIndex(page))
		PostInsertDelta(split_page_id, NULL, PidToPtr(values[split_slot].downlink), tcontext);

	if (split_delta_ret)
	{
		/*
		 * If Root is split, create new root with split key as separator key for
		 * splitted pages and make the splitted pages the children to new root.
		 */
		if (NodePid(page) != BwTreeGetRoot(btree))
		{
			btree_page_id_t parent_pid = tcontext->parent_snapshot.pid;

			Assert(parent_pid != 0);

			PostInsertDelta(parent_pid, split_key, PidToPtr(split_page_id), tcontext);
		}
		else
		{
			btree_page_id_t parent_pid = mapping_table_alloc_pid(mtable);
			DataPage		*root_page = NewEmptyDataPage(btree, parent_pid, false);

			mapping_table_install_page(mtable, parent_pid, root_page);
			PostInsertDelta(parent_pid, NULL, PidToPtr(nodeid), tcontext);
			PostInsertDelta(parent_pid, split_key, PidToPtr(split_page_id), tcontext);
			BwTreeSetRoot(btree, parent_pid);
		}
	}

	free(keys);
	free(values);

	return split_delta_ret;
}


static NodeMeta
GetSplitMeta(bwtree_t btree, NodeMeta *meta, btree_page_id_t pid,
			 page_slot_t split_slot, btree_key_t *keys, btree_value_t *values)
{
	NodeMeta split_page_meta;
	bool	 is_leaf = IS_LEAF(meta->nodeType);

	split_page_meta.nodeType = meta->nodeType;
	split_page_meta.num_keys = meta->num_keys - (split_slot + 1);
	split_page_meta.pageSize = sizeof(DataPage);
	split_page_meta.nodeSize = sizeof(SplitNodeDelta);
	split_page_meta.pid		 = pid;
	split_page_meta.rightpid = meta->rightpid;

	for (page_slot_t i = split_slot + 1; i < meta->num_keys; i++)
	{
		int keysize = BwTreeKeySize(btree, keys[i]);

		split_page_meta.pageSize += ELEMSIZE(keysize);
	}

	split_page_meta.nodeSize = split_page_meta.pageSize;

	return split_page_meta;
}


static page_slot_t
GetSplitSpot(DeltaNode *page)
{
	NodeMeta *meta = GetNodeMeta(page);

	return (NodeIsLeaf(page) && (meta->num_keys % 2 == 0)) ? (meta->num_keys - 1) / 2 :
		   meta->num_keys / 2;
}


static bool
PostSplitDelta(DeltaNode *oldpage, btree_key_t split_key, btree_page_id_t split_page_id,
			   TraversalContext *tcontext)
{
	bwtree_t			  btree		   = tcontext->btree;
	btree_mapping_table_t mtable	   = BwTreeGetMappingTable(btree);
	SplitNodeDelta		  *split_delta = NewSplitNodeDelta(btree, GetNodeMeta(oldpage), oldpage,
														   split_key, split_page_id);

	return mapping_table_install_page_to_replace(mtable, NodePid(oldpage), oldpage,
												 split_delta);
}


static bool
MergePages(btree_page_id_t right_nodeid, TraversalContext *tcontext)
{
	btree_mapping_table_t mtable;
	btree_key_t			  merge_key;
	btree_page_id_t		  left_nodeid;
	btree_page_id_t		  parent_pid;
	DeltaNode			  *parent_node;
	DeltaNode			  *left_node;
	DeltaNode			  *right_node;
	AbortNodeDelta		  *abort_delta;
	btree_key_t			  low_key;

	mtable		= BwTreeGetMappingTable(tcontext->btree);
	right_node	= mapping_table_lookup_page(mtable, right_nodeid);
	low_key		= LowKey(right_node);
	left_nodeid = low_key ? GetLeftSibiling(low_key, &merge_key, tcontext) : InvalidPageId;

	if (left_nodeid == InvalidPageId)
		return false;

	parent_pid	= tcontext->parent_snapshot.pid;
	left_node	= mapping_table_lookup_page(mtable, left_nodeid);
	parent_node = tcontext->parent_snapshot.page;

	/*
	 * Merge only if merged page size will be less than BwTreePageSize,
	 * to avoid splitting after merge
	 */
	if (!can_merge_pages(tcontext->btree, left_node, right_node, merge_key))
		return false;

	abort_delta = PostAbortNodeDelta(parent_node, right_node, tcontext);

	if (abort_delta && PostRemoveNodeDelta(right_node, tcontext))
	{
		RemoveAbortNodeDelta(parent_node, abort_delta, tcontext);

		if (PostMergeDelta(left_node, merge_key, right_node, tcontext))
		{
			tcontext->left_node_id = left_nodeid;
			mapping_table_recycle_pid(mtable, right_nodeid);
			return PostDeleteDelta(parent_pid, merge_key, PidToPtr(right_nodeid), tcontext);
		}

		return false;
	}

	RemoveAbortNodeDelta(parent_node, abort_delta, tcontext);
	return false;
}


static btree_page_id_t
GetLeftSibiling(btree_key_t low_key, btree_key_t *merge_key, TraversalContext *tcontext)
{
	btree_page_id_t parent_pid;
	DeltaNode		*parent_page;
	NodeMeta		*meta;
	btree_key_t		*keys;
	btree_value_t	*values;
	bool			keyPresent;
	page_slot_t		slot;
	btree_page_id_t left_sibiling;

	parent_pid	  = tcontext->parent_snapshot.pid;
	left_sibiling = InvalidPageId;

	Assert(low_key != NULL);

	if (parent_pid == NullPageId || parent_pid == InvalidPageId)
		return InvalidPageId;

	LoadLatestParentSnapshot(tcontext);

	parent_page = tcontext->parent_snapshot.page;
	meta		= GetNodeMeta(parent_page);

	keys   = malloc(sizeof(btree_key_t) * (meta->num_keys + (DeltaChainLen(parent_page) + 1)));
	values = malloc(sizeof(btree_value_t) * (meta->num_keys + (DeltaChainLen(parent_page) + 1)));

	CollectValuesFromPage(parent_page, keys, values, FOLLOW_RIGHT_LINK, tcontext);
	slot = GetPrevSlot(bsearch_page(tcontext->btree, low_key, keys, meta->num_keys, &keyPresent));

	/* Don't merge leftmost page */
	if (slot)
	{
		*merge_key	  = keys[slot];
		left_sibiling = values[slot - 1].downlink;
	}

	free(keys);
	free(values);

	return left_sibiling;
}


static bool
can_merge_pages(bwtree_t btree, DeltaNode *left_page, DeltaNode *right_page, btree_key_t merge_key)
{
	int keysize = merge_key ? BwTreeKeySize(btree, merge_key) : 0;

	return NodePageSize(left_page) + NodePageSize(right_page) + keysize - sizeof(DataPage) <=
		   BwTreePageSize(btree);
}


static bool
PostMergeDelta(DeltaNode *oldpage, btree_key_t merge_key, DeltaNode *merge_node,
			   TraversalContext *tcontext)
{
	bwtree_t			  btree		   = tcontext->btree;
	btree_mapping_table_t mtable	   = BwTreeGetMappingTable(btree);
	MergeNodeDelta		  *merge_delta = NewMergeNodeDelta(btree, GetNodeMeta(oldpage), oldpage,
														   merge_key, merge_node);

	return mapping_table_install_page_to_replace(mtable, NodePid(oldpage), oldpage,
												 merge_delta);
}


static bool
PostRemoveNodeDelta(DeltaNode *oldpage, TraversalContext *tcontext)
{
	bwtree_t			  btree			= tcontext->btree;
	btree_mapping_table_t mtable		= BwTreeGetMappingTable(btree);
	RemoveNodeDelta		  *remove_delta = NewRemoveNodeDelta(btree, GetNodeMeta(oldpage), oldpage);

	if (mapping_table_install_page_to_replace(mtable, NodePid(oldpage), oldpage, remove_delta))
		return remove_delta;

	return NULL;
}


static AbortNodeDelta *
PostAbortNodeDelta(DeltaNode *parent_page, DeltaNode *child_page, TraversalContext *tcontext)
{
	bwtree_t			  btree		   = tcontext->btree;
	btree_mapping_table_t mtable	   = BwTreeGetMappingTable(btree);
	AbortNodeDelta		  *abort_delta = NewAbortNodeDelta(tcontext->btree,
														   GetNodeMeta(parent_page),
														   parent_page, child_page);

	if (mapping_table_install_page_to_replace(mtable, NodePid(parent_page), parent_page,
											  abort_delta))
	{
		return abort_delta;
	}

	return NULL;
}


static void
RemoveAbortNodeDelta(DeltaNode *page, AbortNodeDelta *abort_node,
					 TraversalContext *tcontext)
{
	bwtree_t			  btree	 = tcontext->btree;
	btree_mapping_table_t mtable = BwTreeGetMappingTable(btree);
	bool				  install_success;

	install_success = mapping_table_install_page_to_replace(mtable, NodePid(page),
															abort_node, page);
	assert(install_success == true);
}


static void
ConsolidatePage(btree_page_id_t nodeid, TraversalContext *tcontext)
{
	DataPage	  *new_page;
	DeltaNode	  *old_page;
	bwtree_t	  btree;
	NodeMeta	  *meta;
	btree_key_t	  *keys;
	btree_value_t *values;

	LoadLatestSnapshot(nodeid, tcontext);

	btree	 = tcontext->btree;
	old_page = tcontext->current_snapshot.page;
	meta	 = GetNodeMeta(old_page);
	keys	 = malloc(sizeof(btree_key_t) * (meta->num_keys + (DeltaChainLen(old_page) + 1)));
	values	 = malloc(sizeof(btree_value_t) * (meta->num_keys + (DeltaChainLen(old_page) + 1)));

	Assert(meta->pid == nodeid);

	CollectValuesFromPage(old_page, keys, values, IGNORE_RIGHT_LINK, tcontext);

	new_page = NewDataPage(btree, meta, keys, values);
	mapping_table_install_page_to_replace(BwTreeGetMappingTable(btree), nodeid, old_page, new_page);
	RecycleDeltaChain(old_page);

	free(keys);
	free(values);
}


static void
CollectValuesFromPage(DeltaNode *page, btree_key_t *keys, void *values, bool follow_rightlink,
					  TraversalContext *tcontext)
{
	bwtree_t	btree;
	NodeMeta	*meta;
	fstack_t	*stack;
	DeltaNode	*node;
	key_count_t num_keys;
	bool		keyPresent;
	page_slot_t slot;
	btree_key_t high_key;
	bool		is_leaf;

	Assert(page != NULL);

	btree	 = tcontext->btree;
	meta	 = GetNodeMeta(page);
	stack	 = GetReversedDeltaChain(page);
	is_leaf	 = NodeIsLeaf(page);
	num_keys = 0;
	high_key = NodeIsSplitDelta(page) ? castToNode(page, SplitNodeDelta)->split_key : NULL;

	if (follow_rightlink && high_key && BwTreeKeyCmp(btree, tcontext->search_key, high_key) > 0)
		return CollectValuesFromRightPage(castToNode(page, SplitNodeDelta), keys, values, tcontext);

	while (!stack_is_empty(stack))
	{
		/* For index pages split key is not included in base page, so ignore split key */
		int compare_val = is_leaf ? 0 : -1;

		stack_pop(stack, DeltaNode *, node);

		switch (NodeTag(node))
		{
			case T_BwTreeLeafType:
			case T_BwTreeIndexType:
			{
				DataPage	  *data_page	= castToNode(node, DataPage);
				key_count_t	  page_num_keys = data_page->num_keys;
				btree_key_t	  *src_keys		= data_page->keys;
				btree_value_t *src_values	= data_page->values;

				for (page_slot_t i = 0; i < page_num_keys; i++)
				{
					btree_key_t	  key	= src_keys[i];
					btree_value_t value = src_values[i];

					if (high_key == NULL || BwTreeKeyCmp(btree, key, high_key) <= compare_val)
					{
						InsertIntoSortedArray(btree, keys, values, key, value, num_keys);
						num_keys++;
					}
				}

				break;
			}

			case T_BwTreeLeafInsertDeltaType:
			case T_BwTreeIndexInsertDeltaType:
			{
				InsertDelta	  *delta_page  = castToNode(node, InsertDelta);
				btree_key_t	  insert_key   = delta_page->insert_key;
				btree_value_t insert_value = delta_page->insert_value;

				if (high_key == NULL || BwTreeKeyCmp(btree, insert_key, high_key) <= compare_val)
				{
					InsertIntoSortedArray(btree, keys, values, insert_key, insert_value, num_keys);
					num_keys++;
				}

				break;
			}

			case T_BwTreeLeafDeleteDeltaType:
			case T_BwTreeIndexDeleteDeltaType:
			{
				DeleteDelta	  *delta_page  = castToNode(node, DeleteDelta);
				btree_key_t	  delete_key   = delta_page->delete_key;
				btree_value_t delete_value = delta_page->delete_value;

				if (high_key == NULL || BwTreeKeyCmp(btree, delete_key, high_key) <= compare_val)
				{
					DeleteFromSortedArray(btree, keys, values, delete_key, delete_value, num_keys);
					num_keys--;
				}

				break;
			}

			case T_BwTreeLeafMergeNodeDeltaType:
			case T_BwTreeIndexMergeNodeDeltaType:
			{
				MergeNodeDelta *merge_delta		= castToNode(node, MergeNodeDelta);
				DeltaNode	   *merged_node		= merge_delta->merge_node;
				key_count_t	   merged_key_count = NodeKeyCount(merged_node);
				btree_key_t	   *merged_keys		= malloc(sizeof(btree_key_t) *
														 (merged_key_count +
														  (DeltaChainLen(merged_node) + 1)));
				btree_value_t *merged_values = malloc(sizeof(btree_value_t) *
													  (merged_key_count +
													   (DeltaChainLen(merged_node) + 1)));

				Assert(high_key == NULL);
				CollectValuesFromPage(merged_node, merged_keys, merged_values, IGNORE_RIGHT_LINK,
									  tcontext);

				if (NodeIsIndex(node))
					merged_keys[0] = merge_delta->merge_key;

				for (page_slot_t i = 0; i < merged_key_count; i++)
				{
					InsertIntoSortedArray(btree, keys, values, merged_keys[i], merged_values[i],
										  num_keys);
					num_keys++;
				}

				free(merged_keys);
				free(merged_values);

				break;
			}

			case T_BwTreeLeafSplitNodeDeltaType:
			case T_BwTreeIndexSplitNodeDeltaType:
				break;

			case T_BwTreeIndexAbortNodeDeltaType:
			case T_BwTreeLeafRemoveNodeDeltaType:
			case T_BwTreeIndexRemoveNodeDeltaType:
				break;

			default:
				assert(false && "Unexpected node type");
				abort();
		}
	}

	stack_destroy(stack);

	assert(meta->num_keys == num_keys);
}


static void
CollectValuesFromRightPage(SplitNodeDelta *split_delta, btree_key_t *keys, void *values,
						   TraversalContext *tcontext)
{
	DeltaNode *right_node;

	LoadLatestSnapshot(split_delta->split_node_id, tcontext);
	right_node = tcontext->current_snapshot.page;

	CollectValuesFromPage(right_node, keys, values, FOLLOW_RIGHT_LINK, tcontext);
}


static void
InsertIntoSortedArray(bwtree_t btree, btree_key_t *keys, btree_value_t *values, btree_key_t key,
					  btree_value_t value, key_count_t num_keys)
{
	bool		keyPresent;
	page_slot_t slot = bsearch_page(btree, key, keys, num_keys, &keyPresent);

	assert(keyPresent == false);

	if (slot != num_keys)
	{
		memmove(keys + slot + 1, keys + slot, (num_keys - slot) * sizeof(btree_key_t));
		memmove(values + slot + 1, values + slot, (num_keys - slot) * sizeof(btree_value_t));
	}

	keys[slot]	 = key;
	values[slot] = value;
}


static void
DeleteFromSortedArray(bwtree_t btree, btree_key_t *keys, btree_value_t *values, btree_key_t key,
					  btree_value_t value, key_count_t num_keys)
{
	bool		keyPresent;
	page_slot_t slot = bsearch_page(btree, key, keys, num_keys, &keyPresent);

	Assert(num_keys != 0);

	assert(keyPresent == true);

	if (slot != num_keys)
	{
		memmove(keys + slot, keys + slot + 1, (num_keys - slot - 1) * sizeof(btree_key_t));
		memmove(values + slot, values + slot + 1, (num_keys - slot - 1) * sizeof(btree_value_t));
	}
}


static void
RecycleDeltaChain(DeltaNode *node)
{
	while (node)
	{
		DeltaNode *cur_node = node;

		if (NodeIsMergeDelta(node))
			RecycleDeltaChain(castToNode(node, MergeNodeDelta)->merge_node);

		node = node->prev_node;
		free(cur_node);
	}
}


static fstack_t *
GetReversedDeltaChain(DeltaNode *node)
{
	fstack_t *stack = stack_create(sizeof(BaseNode *), DeltaChainLen(node) + 1);

	while (node)
	{
		assert(!stack_is_full(stack));

		stack_push(stack, DeltaNode *, node);
		node = PrevNode(node);
	}

	return stack;
}


static page_slot_t
bsearch_page(bwtree_t btree, const btree_key_t scan_key,
			 const btree_key_t *keys, key_count_t num_keys, bool *keyPresent)
{
	page_slot_t low, mid, high;
	int			result;

	low	   = 0;
	high   = num_keys - 1;
	result = -1;

	while (low <= high)
	{
		mid	   = (low + high) / 2;
		result = BwTreeKeyCmp(btree, scan_key, keys[mid]);

		if (result == 0)
			break;
		else if (result < 0)
			high = mid - 1;
		else
			low = mid + 1;
	}

	*keyPresent = result == 0;
	return result == 0 ? mid : low;
}


#ifdef ENABLE_DUMP_BTREE

void
dump_btree(bwtree_t btree)
{
	btree_page_id_t	 nodeid = BwTreeGetRoot(btree);
	TraversalContext *tcontext;

	printf("\n");

	while (true)
	{
		DeltaNode	  *page;
		NodeMeta	  *meta;
		btree_key_t	  *keys;
		btree_value_t *values;
		bool		  is_leaf;

		dump_level(btree, nodeid);

		tcontext = TraversalContextCreate(btree, NULL, LOOKUP_OP);

		LoadLatestSnapshot(nodeid, tcontext);

		page   = tcontext->current_snapshot.page;
		meta   = GetNodeMeta(page);
		keys   = malloc(sizeof(btree_key_t) * (meta->num_keys + (DeltaChainLen(page) + 1)));
		values = malloc(sizeof(btree_value_t) * (meta->num_keys + (DeltaChainLen(page) + 1)));

		Assert(meta->pid == nodeid);

		CollectValuesFromPage(page, keys, values, FOLLOW_RIGHT_LINK, tcontext);
		is_leaf = IS_LEAF(GetSnapshotType(tcontext));
		nodeid	= values[0].downlink;
		TraversalContextDestroy(tcontext);

		free(keys);
		free(values);

		if (is_leaf)
			break;
	}

	printf("\n");
}


static void
dump_level(bwtree_t btree, btree_page_id_t pid)
{
	btree_page_id_t nodeid = pid;

	while (nodeid != NullPageId)
	{
		nodeid = dump_page(btree, nodeid);
		printf(", ");
	}

	printf("\n");
}


static btree_page_id_t
dump_page(bwtree_t btree, btree_page_id_t pid)
{
	TraversalContext *tcontext = TraversalContextCreate(btree, NULL, LOOKUP_OP);
	DeltaNode		 *page;
	NodeMeta		 *meta;
	btree_key_t		 *keys;
	btree_value_t	 *values;

	LoadLatestSnapshot(pid, tcontext);

	page   = tcontext->current_snapshot.page;
	meta   = GetNodeMeta(page);
	keys   = malloc(sizeof(btree_key_t) * (meta->num_keys + (DeltaChainLen(page) + 1)));
	values = malloc(sizeof(btree_value_t) * (meta->num_keys + (DeltaChainLen(page) + 1)));

	Assert(meta->pid == pid);

	CollectValuesFromPage(page, keys, values, FOLLOW_RIGHT_LINK, tcontext);
	TraversalContextDestroy(tcontext);

	printf("|");

	for (page_slot_t i = 0; i < meta->num_keys; i++)
	{
		if (keys[i])
		{
			printf("%ld", *(long *) keys[i]);

			if (i != meta->num_keys - 1)
				printf(", ");
		}
	}

	printf("|");

	free(keys);
	free(values);

	return NodeRightPid(page);
}


#endif
