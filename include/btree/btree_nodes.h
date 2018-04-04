#ifndef BTREE_NODES_H
#define BTREE_NODES_H

#ifdef __cplusplus
extern "C" {
#endif

#include "common.h"
#include <btree/mapping_table.h>

#define MAX_DELTA_NODES_PER_PAGE 8

#define LEAF_TYPE  0x01
#define INDEX_TYPE 0x02
#define DELTA_TYPE 0xF0

typedef enum
{
	T_BwTreeInvalidType = 0x00,

	/* Leaf Node Type */
	T_BwTreeLeafType = 0x01,

	/* Leaf Delta Types */
	T_BwTreeLeafInsertDeltaType		= 0x11,
	T_BwTreeLeafDeleteDeltaType		= 0x21,
	T_BwTreeLeafRemoveNodeDeltaType = 0x31,
	T_BwTreeLeafSplitNodeDeltaType	= 0x41,
	T_BwTreeLeafMergeNodeDeltaType	= 0x51,

	/* Index Node Type */
	T_BwTreeIndexType = 0x02,

	/* Index Delta Types */
	T_BwTreeIndexInsertDeltaType	 = 0x12,
	T_BwTreeIndexDeleteDeltaType	 = 0x22,
	T_BwTreeIndexRemoveNodeDeltaType = 0x32,
	T_BwTreeIndexSplitNodeDeltaType	 = 0x42,
	T_BwTreeIndexMergeNodeDeltaType	 = 0x52,
	T_BwTreeIndexAbortNodeDeltaType	 = 0x62,
} BwTreeNodeTag;

typedef uint16_t node_size_t;
typedef int16_t	 key_count_t;
typedef int16_t	 page_slot_t;
typedef void	 *btree_key_t;
typedef union
{
	void			*val;
	btree_page_id_t downlink;
} btree_value_t;

typedef struct
{
	BwTreeNodeTag	nodeType;
	btree_page_id_t pid;

	node_size_t nodeSize;
	node_size_t pageSize;
	key_count_t num_keys;

	btree_page_id_t rightpid;
	int				delta_chain_len;
} NodeMeta;

typedef struct
{
	NodeMeta	meta;
	btree_key_t low_key;
} BaseNode;

typedef struct DeltaNode
{
	BaseNode		 base;
	struct DeltaNode *prev_node;
} DeltaNode;

typedef struct
{
	DeltaNode delta;

	key_count_t	  num_keys;
	btree_key_t	  *keys;
	btree_value_t *values;
} __attribute__((aligned(MAXIMUM_ALIGNOF))) DataPage;

typedef struct
{
	DeltaNode delta;

	btree_key_t	  insert_key;
	btree_value_t insert_value;
} __attribute__((aligned(MAXIMUM_ALIGNOF))) InsertDelta;

typedef struct
{
	DeltaNode delta;

	btree_key_t	  delete_key;
	btree_value_t delete_value;
} DeleteDelta;

typedef struct
{
	DeltaNode delta;
} RemoveNodeDelta;

typedef struct
{
	DeltaNode delta;

	btree_key_t		*split_key;
	btree_page_id_t split_node_id;
} SplitNodeDelta;

typedef struct
{
	DeltaNode delta;

	btree_key_t *merge_key;
	DeltaNode	*merge_node;
} __attribute__((aligned(MAXIMUM_ALIGNOF))) MergeNodeDelta;

typedef struct
{
	DeltaNode delta;

	BaseNode *removed_node;
} AbortNodeDelta;


#define castToNode(node, nodetype) ((nodetype *) (node))
#define NodeTag(node)			   (((BaseNode *) (node))->meta.nodeType)
#define NodeIsLeaf(node)		   (NodeTag(node) & (LEAF_TYPE))
#define NodeIsIndex(node)		   (NodeTag(node) & (INDEX_TYPE))
#define NodeIsDelta(node)		   (NodeTag(node) & (DELTA_TYPE))

#define NodeIsMergeDelta(node) (NodeTag(node) == T_BwTreeIndexMergeNodeDeltaType || \
								NodeTag(node) == T_BwTreeLeafMergeNodeDeltaType)
#define NodeIsSplitDelta(node) (NodeTag(node) == T_BwTreeIndexSplitNodeDeltaType || \
								NodeTag(node) == T_BwTreeLeafSplitNodeDeltaType)

#define GetBaseNode(node)	((BaseNode *) (node))
#define GetDeltaNode(node)	((DeltaNode *) (node))
#define GetNodeMeta(node)	(& (((BaseNode *) (node))->meta))
#define NodeSize(node)		(GetNodeMeta(node)->nodeSize)
#define NodePageSize(node)	(GetNodeMeta(node)->pageSize)
#define NodeKeyCount(node)	(GetNodeMeta(node)->num_keys)
#define DeltaChainLen(node) (GetNodeMeta(node)->delta_chain_len)
#define NodePid(node)		(GetNodeMeta(node)->pid)
#define NodeRightPid(node)	(GetNodeMeta(node)->rightpid)
#define LowKey(node)		(GetBaseNode(node)->low_key)
#define PrevNode(node)		(GetDeltaNode(node)->prev_node)

extern const char *GetBwTreeNodeType(BaseNode *node);

extern DataPage *NewDataPage(bwtree_t btree, NodeMeta *meta, btree_key_t *keys,
							 btree_value_t *values);
extern DataPage *NewEmptyDataPage(bwtree_t btree, btree_page_id_t pid, bool is_leaf);

extern InsertDelta *NewInsertDelta(bwtree_t btree, NodeMeta *meta, DeltaNode *prev_node,
								   btree_key_t insert_key, btree_value_t insert_value);
extern DeleteDelta *NewDeleteDelta(bwtree_t btree, NodeMeta *meta, DeltaNode *prev_node,
								   btree_key_t delete_key, btree_value_t delete_value);

extern SplitNodeDelta *NewSplitNodeDelta(bwtree_t btree, NodeMeta *meta, DeltaNode *prev_node,
										 btree_key_t split_key, btree_page_id_t right_pid);
extern MergeNodeDelta *NewMergeNodeDelta(bwtree_t btree, NodeMeta *meta, DeltaNode *prev_node,
										 btree_key_t merge_key, DeltaNode *merge_node);
extern RemoveNodeDelta *NewRemoveNodeDelta(bwtree_t btree, NodeMeta *meta, DeltaNode *prev_node);
extern AbortNodeDelta  *NewAbortNodeDelta(bwtree_t btree, NodeMeta *meta,
										  DeltaNode *prev_node, DeltaNode *removed_node);

/********************* Helper Functions for traversal ********************/

extern btree_page_id_t		 BwTreeGetRoot(bwtree_t btree);
extern void					 BwTreeSetRoot(bwtree_t btree, btree_page_id_t pid);
extern btree_mapping_table_t BwTreeGetMappingTable(bwtree_t btree);

/********************* Node Operations ********************/

#define LOOKUP_OP 0
#define INSERT_OP 1
#define DELETE_OP 2

typedef struct
{
	btree_page_id_t pid;
	DeltaNode		*page;
} PageSnapshot;

typedef struct
{
	btree_key_t	  key;
	btree_value_t value;
} KeyValPair;

typedef struct
{
	bwtree_t	btree;
	btree_key_t search_key;
	int			traversal_op;
	bool		keyPresent;

	PageSnapshot current_snapshot;
	PageSnapshot parent_snapshot;

	KeyValPair key_val;

	btree_page_id_t left_node_id;
} TraversalContext;

extern TraversalContext *TraversalContextCreate(bwtree_t btree, const void *search_key, int
												traversal_op);
extern void TraversalContextDestroy(TraversalContext *tcontext);
extern void TraverseTree(TraversalContext *tcontext);
extern bool AdjustNodeSize(TraversalContext *tcontext);

extern bool PostInsertDelta(btree_page_id_t nodeid, btree_key_t key, const void *value,
							TraversalContext *tcontext);
extern bool PostDeleteDelta(btree_page_id_t nodeid, btree_key_t key, const void *value,
							TraversalContext *tcontext);

#ifdef __cplusplus
}
#endif

#endif /* BTREE_NODES_H */
