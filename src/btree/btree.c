#include "btree/btree.h"

#include <stdlib.h>
#include <assert.h>

#define MIN_PAGE_SIZE 256
#define MAX_PAGE_SIZE (8 * 1024)

#define CMP(a, b) ((btree)->cmp(a, b))

struct btree_node_t;

typedef struct btree_node_t btree_node_t;

struct btree_data_t
{
	btree_node_compare_t cmp;
	void				 *cmp_arg;
	btree_node_delete_t	 del;
	void				 *del_arg;

	int			 pagesize;
	btree_node_t *root;
};

typedef enum
{
	ROOT_NODE,
	INDEX_NODE,
	LEAF_NODE
} btree_node_type_t;

typedef struct
{
	btree_node_t *link;
	void		 *key;
} btree_index_term_t;

struct btree_node_t
{
	btree_node_type_t node_type;
	int				  node_size;
	int				  height;
	btree_node_t	  *right_link;
	void			  *max_key;

	union
	{
		btree_index_term_t index_term[0];
		void			   *key[0];
	};
};

static_assert(sizeof(btree_node_t) % 16 == 0, "btree_node_t cannot be aligned to 16 bytes");

btree_t
btree_create(int pagesize, btree_node_compare_t cmp, btree_node_delete_t del,
			 void *cmp_arg, void *del_arg)
{
	btree_t btree = malloc(sizeof(struct btree_data_t));

	if (pagesize < MIN_PAGE_SIZE || pagesize > MAX_PAGE_SIZE)
	{
		return NULL;
	}

	btree->cmp	   = cmp;
	btree->cmp_arg = cmp_arg;
	btree->del	   = del;
	btree->del_arg = del_arg;

	return btree;
}


void
btree_destroy(btree_t btree)
{ }


bool
btree_insert(btree_t btree, const void *node)
{ }
