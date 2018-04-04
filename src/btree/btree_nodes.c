#include <btree/btree_nodes.h>

#include <stdlib.h>
#include <string.h>


#define ELEMSIZE(keysize) (MAXALIGN(keysize) + sizeof(btree_key_t) + sizeof(btree_value_t))

static void
copy_keys(bwtree_t btree, btree_key_t *dst_keys, btree_key_t *src_keys,
		  key_count_t num_keys, char *next_key_start, int valid_data_size)
{
	int next_key_offset = 0;
	int keysize;

	for (page_slot_t i = 0; i < num_keys; i++)
	{
		keysize			 = BwTreeKeySize(btree, src_keys[i]);
		dst_keys[i]		 = (btree_key_t) (next_key_start + next_key_offset);
		next_key_offset += MAXALIGN(keysize);

		Assert(next_key_offset < valid_data_size);

		if (src_keys[i])
			memcpy(dst_keys[i], src_keys[i], keysize);
		else
			dst_keys[i] = NULL;
	}
}


/* Consolidated Page's meta is required */
DataPage *
NewDataPage(bwtree_t btree, NodeMeta *meta, btree_key_t *keys, btree_value_t *values)
{
	DataPage *page = malloc(meta->pageSize);

	Assert(meta->num_keys != 0);

	page->delta.base.meta = *meta;
	PrevNode(page)		  = NULL;
	page->num_keys		  = meta->num_keys;
	page->keys			  = (btree_key_t *) ((char *) page + sizeof(DataPage));
	NodeTag(page)		  = NodeIsLeaf(page) ? T_BwTreeLeafType : T_BwTreeIndexType;
	page->values		  = (btree_value_t *) ((char *) page + sizeof(DataPage) +
											   sizeof(btree_key_t) * meta->num_keys);

	/*
	 * New data page is created only when, a page is consolidated or split.
	 * In either case the new page is the only delta.
	 */
	DeltaChainLen(page) = 1;

	copy_keys(btree, page->keys, keys, meta->num_keys,
			  (char *) page->values + sizeof(btree_value_t) * meta->num_keys, meta->pageSize);
	memcpy(page->values, values, sizeof(btree_value_t) * meta->num_keys);

	/* Index page's 0th key is NULL, so take 1th key */
	LowKey(page) = NodeIsLeaf(page) || page->num_keys == 1 ? page->keys[0] : page->keys[1];

	return page;
}


extern DataPage *
NewEmptyDataPage(bwtree_t btree, btree_page_id_t pid, bool is_leaf)
{
	NodeMeta *meta;
	DataPage *page = malloc(sizeof(DataPage));

	meta				= &page->delta.base.meta;
	meta->num_keys		= 0;
	meta->pid			= pid;
	meta->rightpid		= NullPageId;
	meta->pageSize		= sizeof(DataPage);
	meta->nodeSize		= sizeof(DataPage);
	meta->nodeType		= is_leaf ? T_BwTreeLeafType : T_BwTreeIndexType;
	page->num_keys		= 0;
	page->keys			= NULL;
	page->values		= NULL;
	DeltaChainLen(page) = 1;
	PrevNode(page)		= NULL;
	LowKey(page)		= NULL;

	return page;
}


/* Current Page's meta is required */
InsertDelta *
NewInsertDelta(bwtree_t btree, NodeMeta *meta, DeltaNode *prev_node,
			   btree_key_t insert_key, btree_value_t insert_value)
{
	NodeMeta	new_meta;
	int			keysize;
	InsertDelta *delta_page;

	new_meta		   = *meta;
	keysize			   = BwTreeKeySize(btree, insert_key);
	new_meta.pageSize += ELEMSIZE(keysize);
	new_meta.num_keys += 1;
	new_meta.nodeSize  = sizeof(InsertDelta) + MAXALIGN(keysize);

	new_meta.nodeType = NodeIsLeaf(prev_node) ? T_BwTreeLeafInsertDeltaType :
						T_BwTreeIndexInsertDeltaType;
	new_meta.delta_chain_len++;

	delta_page = malloc(new_meta.nodeSize);

	delta_page->delta.base.meta = new_meta;
	PrevNode(delta_page)		= prev_node;
	delta_page->insert_key		= (btree_key_t) ((char *) delta_page + sizeof(InsertDelta));
	delta_page->insert_value	= insert_value;

	if (insert_key)
	{
		memcpy(delta_page->insert_key, insert_key, keysize);

		if (LowKey(prev_node) == NULL || BwTreeKeyCmp(btree, insert_key, LowKey(prev_node)) < 0)
			LowKey(delta_page) = delta_page->insert_key;
		else
			LowKey(delta_page) = LowKey(prev_node);
	}
	else
	{
		delta_page->insert_key = NULL;
		LowKey(delta_page)	   = LowKey(prev_node);
	}

	return delta_page;
}


/*
 * Current Page's meta is required.
 * Delete key must be pointing to memory in the page.
 */
DeleteDelta *
NewDeleteDelta(bwtree_t btree, NodeMeta *meta, DeltaNode *prev_node,
			   btree_key_t delete_key, btree_value_t delete_value)
{
	NodeMeta	new_meta;
	int			keysize;
	DeleteDelta *delta_page;

	new_meta		   = *meta;
	keysize			   = BwTreeKeySize(btree, delete_key);
	new_meta.pageSize -= ELEMSIZE(keysize);
	new_meta.num_keys -= 1;
	new_meta.nodeSize  = sizeof(DeleteDelta);

	new_meta.nodeType = NodeIsLeaf(prev_node) ? T_BwTreeLeafDeleteDeltaType :
						T_BwTreeIndexDeleteDeltaType;
	new_meta.delta_chain_len++;

	delta_page = malloc(new_meta.nodeSize);

	delta_page->delta.base.meta = new_meta;
	PrevNode(delta_page)		= prev_node;
	delta_page->delete_key		= delete_key;
	delta_page->delete_value	= delete_value;
	LowKey(delta_page)			= LowKey(prev_node);

	return delta_page;
}


/* Split key must be pointing to memory in the page. */
SplitNodeDelta *
NewSplitNodeDelta(bwtree_t btree, NodeMeta *meta, DeltaNode *prev_node,
				  btree_key_t split_key, btree_page_id_t right_pid)
{
	SplitNodeDelta *delta_page;
	NodeMeta	   new_meta;
	DeltaNode	   *right_node;

	right_node		   = mapping_table_lookup_page(BwTreeGetMappingTable(btree), right_pid);
	new_meta		   = *meta;
	new_meta.nodeSize  = sizeof(SplitNodeDelta);
	new_meta.num_keys -= NodeKeyCount(right_node);
	new_meta.pageSize -= NodePageSize(right_node) - sizeof(DataPage);
	new_meta.rightpid  = right_pid;

	new_meta.nodeType = NodeIsLeaf(prev_node) ? T_BwTreeLeafSplitNodeDeltaType :
						T_BwTreeIndexSplitNodeDeltaType;
	new_meta.delta_chain_len++;

	/* For index pages, split key pushed up to parent, so exclude that from pageSize */
	if (NodeIsIndex(right_node))
	{
		new_meta.pageSize -= ELEMSIZE(BwTreeKeySize(btree, split_key));
		new_meta.num_keys -= 1;
	}

	delta_page					= malloc(new_meta.nodeSize);
	delta_page->delta.base.meta = new_meta;
	PrevNode(delta_page)		= prev_node;
	delta_page->split_node_id	= right_pid;
	delta_page->split_key		= split_key;
	LowKey(delta_page)			= LowKey(prev_node);

	return delta_page;
}


MergeNodeDelta *
NewMergeNodeDelta(bwtree_t btree, NodeMeta *meta, DeltaNode *prev_node,
				  btree_key_t merge_key, DeltaNode *merge_node)
{
	MergeNodeDelta *merge_delta;
	NodeMeta	   new_meta;
	int			   keysize;

	new_meta		   = *meta;
	keysize			   = BwTreeKeySize(btree, merge_key);
	new_meta.nodeSize  = sizeof(MergeNodeDelta) + MAXALIGN(keysize);
	new_meta.pageSize += merge_node->base.meta.pageSize - sizeof(DataPage);
	new_meta.num_keys += merge_node->base.meta.num_keys;
	new_meta.rightpid  = merge_node->base.meta.rightpid;
	new_meta.nodeType  = NodeIsLeaf(prev_node) ? T_BwTreeLeafMergeNodeDeltaType :
						 T_BwTreeIndexMergeNodeDeltaType;
	new_meta.delta_chain_len++;

	/*
	 * In index pages, merge_key is included in the new merged page,
	 * so include sizeof merge_key also, in pagesize
	 */
	if (NodeIsLeaf(prev_node))
	{
		new_meta.nodeType = T_BwTreeLeafMergeNodeDeltaType;
	}
	else
	{
		new_meta.pageSize += MAXALIGN(keysize);
		new_meta.nodeType  = T_BwTreeIndexMergeNodeDeltaType;
	}

	merge_delta					 = malloc(new_meta.nodeSize);
	merge_delta->delta.base.meta = new_meta;
	PrevNode(merge_delta)		 = prev_node;
	merge_delta->merge_key		 = (btree_key_t) ((char *) merge_delta + sizeof(MergeNodeDelta));
	merge_delta->merge_node		 = merge_node;
	LowKey(merge_delta)			 = LowKey(prev_node);

	if (merge_key)
		memcpy(merge_delta->merge_key, merge_key, keysize);
	else
		merge_delta->merge_key = NULL;

	return merge_delta;
}


RemoveNodeDelta *
NewRemoveNodeDelta(bwtree_t btree, NodeMeta *meta, DeltaNode *prev_node)
{
	RemoveNodeDelta *delta_page;

	delta_page					= malloc(sizeof(RemoveNodeDelta));
	delta_page->delta.base.meta = *meta;

	PrevNode(delta_page) = prev_node;
	NodeSize(delta_page) = sizeof(RemoveNodeDelta);
	LowKey(delta_page)	 = LowKey(prev_node);
	NodeTag(delta_page)	 = NodeIsLeaf(prev_node) ? T_BwTreeLeafRemoveNodeDeltaType :
						   T_BwTreeIndexRemoveNodeDeltaType;
	DeltaChainLen(delta_page)++;


	return delta_page;
}


AbortNodeDelta *
NewAbortNodeDelta(bwtree_t btree, NodeMeta *meta, DeltaNode *prev_node, DeltaNode *removed_node)
{
	AbortNodeDelta *delta_page;

	Assert(NodeIsIndex(prev_node));

	delta_page					= malloc(sizeof(AbortNodeDelta));
	delta_page->delta.base.meta = *meta;

	PrevNode(delta_page) = prev_node;
	NodeSize(delta_page) = sizeof(AbortNodeDelta);
	NodeTag(delta_page)	 = T_BwTreeIndexAbortNodeDeltaType;
	LowKey(delta_page)	 = LowKey(prev_node);
	DeltaChainLen(delta_page)++;

	return delta_page;
}


const char *
GetBwTreeNodeType(BaseNode *node)
{
	switch (NodeTag(node))
	{
		case T_BwTreeInvalidType:
			return "BwTreeInvalidType";

		case T_BwTreeLeafType:
			return "BwTreeLeafType";

		case T_BwTreeLeafInsertDeltaType:
			return "BwTreeLeafInsertDeltaType";

		case T_BwTreeLeafDeleteDeltaType:
			return "BwTreeLeafDeleteDeltaType";

		case T_BwTreeLeafRemoveNodeDeltaType:
			return "BwTreeLeafRemoveNodeDeltaType";

		case T_BwTreeLeafSplitNodeDeltaType:
			return "BwTreeLeafSplitNodeDeltaType";

		case T_BwTreeLeafMergeNodeDeltaType:
			return "BwTreeLeafMergeNodeDeltaType";

		case T_BwTreeIndexType:
			return "BwTreeIndexType";

		case T_BwTreeIndexInsertDeltaType:
			return "BwTreeIndexInsertDeltaType";

		case T_BwTreeIndexDeleteDeltaType:
			return "BwTreeIndexDeleteDeltaType";

		case T_BwTreeIndexRemoveNodeDeltaType:
			return "BwTreeIndexRemoveNodeDeltaType";

		case T_BwTreeIndexSplitNodeDeltaType:
			return "BwTreeIndexSplitNodeDeltaType";

		case T_BwTreeIndexMergeNodeDeltaType:
			return "BwTreeIndexMergeNodeDeltaType";

		case T_BwTreeIndexAbortNodeDeltaType:
			return "BwTreeIndexAbortNodeDeltaType";

		default:
			Assert(false && "Invalid Node Type");
			abort();
	}
}
