#include "btree/btree.h"
#include "ilist.h"

#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#define MIN_PAGE_SIZE 64
#define MAX_PAGE_SIZE (8 * 1024)

#define CMP(a, b)	 (btree->kv_info->cmp_key(a, b, btree->kv_info->extra_arg))
#define KEYSIZE(key) (key == NULL ? 0 : btree->kv_info->key_size(key, btree->kv_info->extra_arg))

struct btree_page_t;

typedef struct btree_page_t btree_page_t;

struct btree_data_t
{
	btree_key_val_info_t *kv_info;
	int					 pagesize;
	btree_page_t		 *root;
};

typedef struct
{
	int key_present : 1;
	int slot		: sizeof(int16_t) * 8;
} bsearch_result_t;

typedef struct
{
	union
	{
		void		 *value;
		btree_page_t *downlink;
	};
	char key[0];
} btree_page_item_t;

struct btree_page_t
{
	bool is_leaf;
	int	 rem_page_size;
	int	 height;
	int	 num_keys;

	btree_page_t *rightlink;

	int		page_data_start_offset;
	int16_t max_key_offset;
	int16_t offsetArray[];
};

static bool check_page_integrity(btree_t btree, btree_page_t *page, const void *prev_key);
static bool check_level_integrity(btree_t btree, btree_page_t *page);
static void dump_btree(btree_t btree);
static void dump_btree_level(btree_page_t *page);
static void dump_btree_page(btree_page_t *page);

static inline btree_page_t **
PageGetPtrToDownLinkAtSlot(btree_page_t *page, int slot)
{
	return &((btree_page_item_t *) ((char *) page + page->offsetArray[slot]))->downlink;
}


static inline void **
PageGetValuesAtSlot(btree_page_t *page, int slot)
{
	return &((btree_page_item_t *) ((char *) page + page->offsetArray[slot]))->value;
}


static inline void *
PageGetKeyAtOffset(btree_page_t *page, int offset)
{
	return ((btree_page_item_t *) ((char *) page + offset))->key;
}


static inline void *
PageGetKeyAtSlot(btree_page_t *page, int slot)
{
	if (slot == 0)
	{
		return NULL;
	}
	else
	{
		return PageGetKeyAtOffset(page, page->offsetArray[slot]);
	}
}


#define GetPrevSlot(res) ((res).key_present ? (res).slot : (res).slot - 1)

typedef struct
{
	btree_page_t	 *page;
	bsearch_result_t search_res;
	slist_node		 list_node;
} btree_stack_node_t;


btree_t
btree_create(int pagesize, btree_key_val_info_t *kv_info)
{
	btree_t btree;

	assert(kv_info != NULL);
	assert(pagesize <= MAX_PAGE_SIZE && pagesize >= MIN_PAGE_SIZE);

	btree = malloc(sizeof(struct btree_data_t));

	if (pagesize < MIN_PAGE_SIZE || pagesize > MAX_PAGE_SIZE)
	{
		return NULL;
	}

	btree->kv_info	= kv_info;
	btree->pagesize = pagesize;
	btree->root		= NULL;

	return btree;
}


void
btree_destroy(btree_t btree)
{ }


static void
push_to_stack(slist_head *stack, btree_page_t *page, bsearch_result_t search_res)
{
	btree_stack_node_t *node = malloc(sizeof(btree_stack_node_t));

	node->page		 = page;
	node->search_res = search_res;
	slist_push_head(stack, &node->list_node);
}


static bool
pop_from_stack(slist_head *stack, btree_page_t **page, bsearch_result_t *search_res)
{
	btree_stack_node_t *node;

	if (slist_is_empty(stack))
	{
		*page = NULL;
		return false;
	}

	node = slist_head_element(btree_stack_node_t, list_node, stack);

	*page		= node->page;
	*search_res = node->search_res;

	slist_pop_head_node(stack);
	free(node);

	return true;
}


static void
destroy_stack(slist_head *stack)
{
	btree_page_t	 *page;
	bsearch_result_t search_res;

	while (pop_from_stack(stack, &page, &search_res))
	{ }
}


static bsearch_result_t
bsearch_page(btree_t btree, btree_page_t *page, const void *scan_key)
{
	int				 low, mid, high, result;
	int16_t			 *offsetArray = page->offsetArray;
	bsearch_result_t res;

	low	   = 0;
	high   = page->num_keys - 1;
	result = -1;

	while (low <= high)
	{
		mid	   = (low + high) / 2;
		result = mid == 0 ? 1 : CMP(scan_key, PageGetKeyAtSlot(page, mid));

		if (result == 0)
		{
			break;
		}
		else if (result < 0)
		{
			high = mid - 1;
		}
		else
		{
			low = mid + 1;
		}
	}

	res.key_present = result == 0;
	res.slot		= result == 0 ? mid : low;

	return res;
}


static btree_page_t *
new_page(btree_t btree, bool is_leaf, int height)
{
	btree_page_t *page = malloc(btree->pagesize);

	memset(page, 0, sizeof(btree_page_t));

	page->is_leaf				 = is_leaf;
	page->rem_page_size			 = btree->pagesize - sizeof(btree_page_t);
	page->page_data_start_offset = btree->pagesize;
	page->height				 = height;

	return page;
}


static bool
does_item_fit_into_page(btree_t btree, btree_page_t *page, const void *key, bool isKeyPresent)
{
	int key_size  = KEYSIZE(key);
	int elem_size = isKeyPresent ? 0 : (MAXALIGN(key_size) + sizeof(btree_page_item_t) +
										sizeof(int16_t));

	if (!page->is_leaf)
	{
		assert(isKeyPresent == false);
	}

	return page->rem_page_size >= elem_size;
}


static void *
duplicate_key(btree_t btree, const void *key)
{
	int	 keysize  = KEYSIZE(key);
	void *dup_key = malloc(keysize);

	memcpy(dup_key, key, keysize);

	return dup_key;
}


static void
add_to_page(btree_t btree, btree_page_t *page, const void *key, const void *val,
			int slot, bool isKeyPresent)
{
	int16_t *offsetArray   = page->offsetArray;
	int		key_size	   = KEYSIZE(key);
	int		key_value_size = MAXALIGN(key_size) + sizeof(btree_page_item_t);
	int		elem_size	   = isKeyPresent ? 0 : (key_value_size + sizeof(int16_t));
	bool	isMaxKey	   = false;
	int		keyOffset;

	assert(slot <= page->num_keys);
	assert(does_item_fit_into_page(btree, page, key, isKeyPresent));

	if (slot != page->num_keys)
	{
		if (!isKeyPresent)
		{
			memmove(offsetArray + slot + 1, offsetArray + slot, (page->num_keys - slot) *
					sizeof(int16_t));
		}
	}
	else
	{
		isMaxKey = true;
	}

	if (!isKeyPresent)
	{
		page->num_keys++;
		page->rem_page_size			 -= elem_size;
		page->page_data_start_offset -= key_value_size;
		keyOffset					  = page->page_data_start_offset;
		page->offsetArray[slot]		  = keyOffset;
	}
	else
	{
		assert(slot < page->num_keys);
		assert(page->is_leaf);
		keyOffset = page->offsetArray[slot];
	}

	assert(key == NULL ? (slot == 0 && (page->is_leaf ? val == NULL : true)) : true);
	*PageGetValuesAtSlot(page, slot) = (void *) val;

	if (!isKeyPresent && key)
	{
		void *page_key = PageGetKeyAtSlot(page, slot);

		memcpy(page_key, key, key_size);
	}

	if (isMaxKey && key)
	{
		page->max_key_offset = page->offsetArray[slot];
	}
}


static void
insert_into_page(btree_t btree, btree_page_t *page, const void *key, const void *value)
{
	bsearch_result_t res = bsearch_page(btree, page, key);

	add_to_page(btree, page, key, value, res.slot, res.key_present);
}


struct page_split_state_t
{
	btree_page_t *page_to_split;
	btree_page_t *left_page;
	btree_page_t *right_page;
	void		 *key;
	void		 *value;
	int			 split_slot;
	int			 insert_key_slot;

	btree_page_t *insert_page;
	int			 next_insert_slot;

	void *split_key;
};

static void
add_to_split_page(btree_t btree, struct page_split_state_t *split_state, int slot)
{
	const void *key;
	const void *value;
	bool	   insert_key_processed = false;

	if (slot == split_state->insert_key_slot)
	{
		key							 = split_state->key;
		value						 = split_state->value;
		split_state->insert_key_slot = -1;
		insert_key_processed		 = true;
	}
	else
	{
		key	  = PageGetKeyAtSlot(split_state->page_to_split, slot);
		value = *PageGetValuesAtSlot(split_state->page_to_split, slot);
	}

	if (split_state->next_insert_slot == split_state->split_slot)
	{
		split_state->split_key	 = duplicate_key(btree, key);
		split_state->insert_page = split_state->right_page;
		split_state->split_slot	 = -1;

		if (split_state->page_to_split->is_leaf)
		{
			add_to_page(btree, split_state->insert_page, NULL, NULL, 0, false);
			split_state->next_insert_slot = 1;
		}
		else
		{
			split_state->next_insert_slot = 0;
			key							  = NULL;
		}
	}

	add_to_page(btree, split_state->insert_page, key, value, split_state->next_insert_slot, false);
	split_state->next_insert_slot++;

	if (insert_key_processed)
	{
		add_to_split_page(btree, split_state, slot);
	}
}


static void *
split_page(btree_t btree, btree_page_t *page_to_split, const void *key, const void *value,
		   int insert_key_slot, btree_page_t **right_page_ptr)
{
	btree_page_t *left_page = new_page(btree, page_to_split->is_leaf,
									   page_to_split->height);
	btree_page_t *right_page = new_page(btree, page_to_split->is_leaf,
										page_to_split->height);
	int						  num_keys = page_to_split->num_keys;
	struct page_split_state_t split_state;

	memset(&split_state, 0, sizeof(split_state));
	split_state.page_to_split	= page_to_split;
	split_state.left_page		= left_page;
	split_state.right_page		= right_page;
	split_state.key				= (void *) key;
	split_state.value			= (void *) value;
	split_state.insert_page		= left_page;
	split_state.split_slot		= (num_keys + 1) / 2;
	split_state.insert_key_slot = insert_key_slot;

	for (int slot = 0; slot < num_keys; slot++)
	{
		add_to_split_page(btree, &split_state, slot);
	}

	if (split_state.insert_key_slot == num_keys)
	{
		add_to_page(btree, right_page, key, value, right_page->num_keys, false);
	}

	left_page->rightlink  = right_page;
	right_page->rightlink = page_to_split->rightlink;
	*right_page_ptr		  = right_page;

	memcpy(page_to_split, left_page, btree->pagesize);
	free(left_page);

	return split_state.split_key;
}


static void
split_page_and_insert(btree_t btree, slist_head *stack, const void *key, void *value)
{
	btree_page_t	 *page_to_split;
	btree_page_t	 *left_page;
	bsearch_result_t search_res;
	btree_page_t	 *right_page;
	bool			 split_done		 = false;
	bool			 free_insert_key = false;

	pop_from_stack(stack, &page_to_split, &search_res);

	while (!split_done)
	{
		void *split_key = split_page(btree, page_to_split, key, value, search_res.slot,
									 &right_page);

		left_page = page_to_split;

		if (free_insert_key)
		{
			free((void *) key);
		}

		pop_from_stack(stack, &page_to_split, &search_res);

		if (page_to_split == NULL)
		{
			btree_page_t *root = new_page(btree, false, right_page->height + 1);

			add_to_page(btree, root, NULL, left_page, 0, false);
			add_to_page(btree, root, split_key, right_page, 1, false);
			btree->root = root;

			free(split_key);
			split_done = true;
		}
		else
		{
			if (does_item_fit_into_page(btree, page_to_split, split_key, false))
			{
				insert_into_page(btree, page_to_split, split_key, right_page);

				free(split_key);
				split_done = true;
			}
			else
			{
				key				= split_key;
				value			= right_page;
				free_insert_key = true;
			}
		}
	}
}


static void
btree_search(btree_t btree, const void *key, slist_head *stack, btree_page_t **leaf_page,
			 bsearch_result_t *last_search_res)
{
	btree_page_t	 *page = btree->root;
	bsearch_result_t search_res;

	slist_init(stack);

	if (page != NULL)
	{
		bsearch_result_t search_res;

		while (page != NULL)
		{
			search_res = bsearch_page(btree, page, key);

			push_to_stack(stack, page, search_res);

			if (page->is_leaf)
			{
				*leaf_page		 = page;
				*last_search_res = search_res;
				break;
			}
			else
			{
				page = *PageGetPtrToDownLinkAtSlot(page, GetPrevSlot(search_res));
			}
		}
	}
	else
	{
		*leaf_page					 = NULL;
		last_search_res->key_present = false;
		last_search_res->slot		 = -1;
	}
}


bool
btree_insert(btree_t btree, const void *key, const void *value)
{
	btree_page_t	 *leaf_page;
	slist_head		 stack;
	bsearch_result_t search_res;

	if (btree->root == NULL)
	{
		btree->root = new_page(btree, true, 0);
		add_to_page(btree, btree->root, NULL, NULL, 0, false);
	}

	btree_search(btree, key, &stack, &leaf_page, &search_res);

	if (!search_res.key_present)
	{
		if (does_item_fit_into_page(btree, leaf_page, key, search_res.key_present))
		{
			add_to_page(btree, leaf_page, key, value, search_res.slot, search_res.key_present);
		}
		else
		{
			split_page_and_insert(btree, &stack, key, (void *) value);
		}
	}
	else
	{
		add_to_page(btree, leaf_page, key, value, search_res.slot, search_res.key_present);
	}

#ifdef CHECK_BTREE_INTEGRITY
	if (!check_btree_integrity(btree))
	{
		assert(false);
		abort();
	}
#endif

	destroy_stack(&stack);

	return !search_res.key_present;
}


bool
btree_delete(btree_t btree, const void *key, void **value)
{
	return false;
}


bool
btree_find(btree_t btree, const void *key, void **value)
{
	slist_head		 stack;
	btree_page_t	 *leaf_page;
	bsearch_result_t search_res;

	btree_search(btree, key, &stack, &leaf_page, &search_res);

	if (search_res.key_present)
	{
		*value = *PageGetValuesAtSlot(leaf_page, search_res.slot);
	}

	destroy_stack(&stack);

	return search_res.key_present;
}


int
btree_height(btree_t btree)
{
	return btree->root ? btree->root->height : -1;
}


bool
check_btree_integrity(btree_t btree)
{
	btree_page_t *page = btree->root;

	while (page != NULL)
	{
		if (!check_level_integrity(btree, page))
		{
			return false;
		}

		if (!page->is_leaf && *PageGetPtrToDownLinkAtSlot(page, 0) == NULL)
		{
			return false;
		}

		page = page->is_leaf ? NULL : *PageGetPtrToDownLinkAtSlot(page, 0);
	}

	return true;
}


static bool
check_level_integrity(btree_t btree, btree_page_t *page)
{
	const void *prev_key = NULL;
	int		   height;
	bool	   is_leaf;

	if (page)
	{
		height	= page->height;
		is_leaf = page->is_leaf;
	}

	while (page != NULL)
	{
		if (!check_page_integrity(btree, page, prev_key))
		{
			return false;
		}

		if (height != page->height || is_leaf != page->is_leaf)
		{
			return false;
		}

		prev_key = PageGetKeyAtOffset(page, page->max_key_offset);
		page	 = page->rightlink;
	}

	return true;
}


static bool
check_page_integrity(btree_t btree, btree_page_t *page, const void *prev_key)
{
	int slot;

	if (page == NULL || page->num_keys == 0)
	{
		return true;
	}

	if ((page->is_leaf && page->height != 0) || (!page->is_leaf && page->height == 0))
	{
		return false;
	}

	slot	 = prev_key == NULL ? 2 : 1;
	prev_key = prev_key == NULL ? PageGetKeyAtSlot(page, 1) : prev_key;

	for (; slot < page->num_keys; slot++)
	{
		const void *cur_key = PageGetKeyAtSlot(page, slot);

		if (CMP(prev_key, cur_key) >= 0)
		{
			return false;
		}

		prev_key = cur_key;
	}

	if (CMP(PageGetKeyAtOffset(page, page->max_key_offset),
			PageGetKeyAtSlot(page, page->num_keys - 1)) < 0)
	{
		return false;
	}

	return true;
}


static void
dump_btree(btree_t btree)
{
	btree_page_t *page = btree->root;

	while (page)
	{
		dump_btree_level(page);
		printf("\n");
		page = *PageGetPtrToDownLinkAtSlot(page, 0);
	}
}


static void
dump_btree_level(btree_page_t *page)
{
	while (page)
	{
		dump_btree_page(page);
		page = page->rightlink;

		if (page)
		{
			printf(", ");
		}
	}
}


static void
dump_btree_page(btree_page_t *page)
{
	printf("|");

	for (int i = 1; i < page->num_keys; i++)
	{
		printf("%ld", *(long *) PageGetKeyAtSlot(page, i));

		if (i != page->num_keys - 1)
		{
			printf(", ");
		}
	}

	printf("|");
}
