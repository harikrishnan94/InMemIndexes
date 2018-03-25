#include "btree/btree.h"
#include "fixed_stack.h"

#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#define MIN_PAGE_SIZE 64
#define MAX_PAGE_SIZE (8 * 1024)

#define Max(a, b) ((a) > (b) ? (a) : (b))
#define Min(a, b) ((a) < (b) ? (a) : (b))

#define CMP(a, b)	 (btree->kv_info->cmp_key(a, b, btree->kv_info->extra_arg))
#define KEYSIZE(key) (key == NULL ? 0 : btree->kv_info->key_size(key, btree->kv_info->extra_arg))

struct btree_page_t;

typedef int16_t				page_off_t;
typedef int					page_slot_t;
typedef struct btree_page_t btree_page_t;

struct btree_data_t
{
	btree_key_val_info_t *kv_info;
	int					 pagesize;
	btree_page_t		 *root;
};

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
	int height;
	int rem_page_size;
	int num_keys;
	int logical_pagesize;

	btree_page_t *rightlink;

	page_off_t page_data_start_offset;
	page_off_t offsetArray[];
};

#define PageIsLeaf(page)  ((page)->height == 0)
#define PageIsInner(page) ((page)->height > 0)

static void destroy_page_recursively(btree_page_t *page);

struct stack_node_t
{
	btree_page_t *page;
	int			 slot;
};

static inline btree_page_t **
PageGetPtrToDownLinkAtSlot(btree_page_t *page, page_slot_t slot)
{
	return &((btree_page_item_t *) ((char *) page + page->offsetArray[slot]))->downlink;
}


static inline void **
PageGetValuesAtSlot(btree_page_t *page, page_slot_t slot)
{
	return &((btree_page_item_t *) ((char *) page + page->offsetArray[slot]))->value;
}


static inline void *
PageGetKeyAtOffset(btree_page_t *page, int offset)
{
	return ((btree_page_item_t *) ((char *) page + offset))->key;
}


static inline void *
PageGetKeyAtSlot(btree_page_t *page, page_slot_t slot)
{
	return slot == 0 ? NULL : PageGetKeyAtOffset(page, page->offsetArray[slot]);
}


#define GetPrevSlot(res) ((slot) - 1)


btree_t
btree_create(int pagesize, btree_key_val_info_t *kv_info)
{
	btree_t btree;

	assert(kv_info != NULL);
	assert(pagesize <= MAX_PAGE_SIZE && pagesize >= MIN_PAGE_SIZE);

	btree = malloc(sizeof(struct btree_data_t));

	if (pagesize < MIN_PAGE_SIZE || pagesize > MAX_PAGE_SIZE)
		return NULL;

	btree->kv_info	= kv_info;
	btree->pagesize = pagesize;
	btree->root		= NULL;

	return btree;
}


void
btree_destroy(btree_t btree)
{
	destroy_page_recursively(btree->root);
	free(btree);
}


static void
destroy_page_recursively(btree_page_t *page)
{
	if (page)
	{
		destroy_page_recursively(*PageGetPtrToDownLinkAtSlot(page, 0));

		do
		{
			btree_page_t *old_page = page;

			page = page->rightlink;
			free(old_page);
		} while (page);
	}
}


static void
push_to_stack(fstack_t *stack, btree_page_t *page, page_slot_t slot)
{
	struct stack_node_t snode;
	bool				is_stack_full;

	snode.page = page;
	snode.slot = slot;

	stack_push(stack, struct stack_node_t, snode, is_stack_full);

	assert(!is_stack_full);
}


static bool
pop_from_stack(fstack_t *stack, btree_page_t **page, page_slot_t *slot)
{
	struct stack_node_t snode;
	bool				is_stack_empty;

	stack_pop(stack, struct stack_node_t, snode, is_stack_empty);

	if (is_stack_empty)
	{
		*page = NULL;
		return false;
	}

	*page = snode.page;
	*slot = snode.slot;

	return true;
}


static page_slot_t
bsearch_page(btree_t btree, btree_page_t *page, const void *scan_key, bool *isKeyPresent)
{
	page_slot_t low, mid, high, result;
	page_off_t	*offsetArray = page->offsetArray;

	low	   = 0;
	high   = page->num_keys - 1;
	result = -1;

	while (low <= high)
	{
		mid	   = (low + high) / 2;
		result = mid == 0 ? 1 : CMP(scan_key, PageGetKeyAtSlot(page, mid));

		if (result == 0)
			break;
		else if (result < 0)
			high = mid - 1;
		else
			low = mid + 1;
	}

	*isKeyPresent = result == 0;
	return result == 0 ? mid : low;
}


static btree_page_t *
new_page(btree_t btree, int height)
{
	btree_page_t *page = malloc(btree->pagesize);

	memset(page, 0, sizeof(btree_page_t));

	page->rem_page_size			 = btree->pagesize - sizeof(btree_page_t);
	page->page_data_start_offset = btree->pagesize;
	page->height				 = height;
	page->logical_pagesize		 = sizeof(btree_page_t);

	return page;
}


static bool
does_item_fit_into_page(btree_t btree, btree_page_t *page, const void *key, bool isKeyPresent)
{
	int key_size  = KEYSIZE(key);
	int elem_size = isKeyPresent ? 0 : (MAXALIGN(key_size) + sizeof(btree_page_item_t) +
										sizeof(page_off_t));

	assert(PageIsInner(page) ? isKeyPresent == false : true);

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
			page_slot_t slot, bool isKeyPresent)
{
	page_off_t *offsetArray	  = page->offsetArray;
	int		   key_size		  = KEYSIZE(key);
	int		   key_value_size = MAXALIGN(key_size) + sizeof(btree_page_item_t);
	int		   elem_size	  = isKeyPresent ? 0 : (key_value_size + sizeof(page_off_t));

	assert(slot <= page->num_keys);
	assert(does_item_fit_into_page(btree, page, key, isKeyPresent));

	if (slot != page->num_keys)
	{
		if (!isKeyPresent)
		{
			memmove(offsetArray + slot + 1, offsetArray + slot, (page->num_keys - slot) *
					sizeof(page_off_t));
		}
	}

	if (!isKeyPresent)
	{
		page->num_keys++;
		page->rem_page_size			 -= elem_size;
		page->logical_pagesize		 += elem_size;
		page->page_data_start_offset -= key_value_size;
		page->offsetArray[slot]		  = page->page_data_start_offset;
	}

	assert(page->page_data_start_offset > sizeof(btree_page_t));
	assert(isKeyPresent ? slot < page->num_keys : true);
	assert(isKeyPresent ? PageIsLeaf(page) : true);
	assert(key == NULL ? (slot == 0 && (PageIsLeaf(page) ? val == NULL : true)) : true);

	*PageGetValuesAtSlot(page, slot) = (void *) val;

	if (!isKeyPresent && key)
	{
		void *page_key = PageGetKeyAtSlot(page, slot);

		memcpy(page_key, key, key_size);
	}
}


static void
delete_from_page(btree_t btree, btree_page_t *page, page_slot_t slot)
{
	page_off_t *offsetArray;
	int		   key_size;
	int		   key_value_size;
	int		   elem_size;

	assert(slot > 0 && slot < page->num_keys);

	offsetArray	   = page->offsetArray;
	key_size	   = KEYSIZE(PageGetKeyAtSlot(page, slot));
	key_value_size = MAXALIGN(key_size) + sizeof(btree_page_item_t);
	elem_size	   = key_value_size + sizeof(page_off_t);

	if (slot != page->num_keys - 1)
	{
		memmove(offsetArray + slot, offsetArray + slot + 1,
				(page->num_keys - slot - 1) * sizeof(page_off_t));
	}

	page->logical_pagesize -= elem_size;
	page->num_keys--;
}


static bool
is_page_under_filled(btree_t btree, btree_page_t *page)
{
	return page->logical_pagesize < btree->pagesize / 2;
}


static bool
can_merge_pages(btree_t btree, btree_page_t *page1, btree_page_t *page2)
{
	if (page1->num_keys == 1 || page2->num_keys == 1)
		return true;

	return (page1->logical_pagesize + page2->logical_pagesize - sizeof(btree_page_t)) <=
		   btree->pagesize;
}


static void
insert_into_page(btree_t btree, btree_page_t *page, const void *key, const void *value)
{
	bool		isKeyPresent;
	page_slot_t slot;

	slot = bsearch_page(btree, page, key, &isKeyPresent);
	add_to_page(btree, page, key, value, slot, isKeyPresent);
}


struct page_split_state_t
{
	btree_page_t *page_to_split;
	btree_page_t *left_page;
	btree_page_t *right_page;
	void		 *key;
	void		 *value;
	page_slot_t	 split_slot;
	page_slot_t	 insert_key_slot;

	btree_page_t *insert_page;
	page_slot_t	 next_insert_slot;

	void *split_key;
};

static void
add_to_split_page(btree_t btree, struct page_split_state_t *split_state, page_slot_t slot)
{
	const void *key;
	const void *value;
	bool	   insert_key_processed = false;
	bool	   is_split_point		= false;
	bool	   skip_insert			= false;

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
		split_state->split_key	= duplicate_key(btree, key);
		split_state->split_slot = -1;
		is_split_point			= true;

		if (PageIsInner(split_state->page_to_split))
		{
			split_state->insert_page	  = split_state->right_page;
			split_state->next_insert_slot = 1;
			key							  = NULL;
			skip_insert					  = true;

			add_to_page(btree, split_state->insert_page, NULL, value, 0, false);
		}
	}

	if (!skip_insert)
	{
		add_to_page(btree, split_state->insert_page, key, value,
					split_state->next_insert_slot, false);
		split_state->next_insert_slot++;
	}

	if (is_split_point && PageIsLeaf(split_state->page_to_split))
	{
		split_state->insert_page	  = split_state->right_page;
		split_state->next_insert_slot = 1;
		add_to_page(btree, split_state->insert_page, NULL, NULL, 0, false);
	}

	if (insert_key_processed)
		add_to_split_page(btree, split_state, slot);
}


static void *
split_page(btree_t btree, btree_page_t *page_to_split, const void *key, const void *value,
		   page_slot_t insert_key_slot, btree_page_t **right_page_ptr)
{
	btree_page_t			  *left_page  = new_page(btree, page_to_split->height);
	btree_page_t			  *right_page = new_page(btree, page_to_split->height);
	int						  num_keys	  = page_to_split->num_keys;
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

	for (page_slot_t slot = 0; slot < num_keys; slot++)
	{
		add_to_split_page(btree, &split_state, slot);
	}

	if (split_state.insert_key_slot == num_keys)
		add_to_page(btree, right_page, key, value, right_page->num_keys, false);

	left_page->rightlink  = right_page;
	right_page->rightlink = page_to_split->rightlink;
	*right_page_ptr		  = right_page;

	memcpy(page_to_split, left_page, btree->pagesize);
	free(left_page);

	return split_state.split_key;
}


static void
reinit_page_and_insert(btree_t btree, btree_page_t *page, const void *key, const void *value)
{
	btree_page_t *fresh_page = new_page(btree, page->height);

	add_to_page(btree, fresh_page, NULL, *PageGetPtrToDownLinkAtSlot(page, 0), 0, false);
	add_to_page(btree, fresh_page, key, value, 1, false);
	memcpy(page, fresh_page, btree->pagesize);

	free(fresh_page);
}


static void
split_page_and_insert(btree_t btree, fstack_t *stack, const void *key, void *value)
{
	btree_page_t *page_to_split;
	page_slot_t	 slot;
	bool		 free_insert_key = false;

	pop_from_stack(stack, &page_to_split, &slot);

	while (true)
	{
		btree_page_t *left_page, *right_page, *parent_page;
		void		 *split_key;

		if (page_to_split->num_keys == 1)
		{
			reinit_page_and_insert(btree, page_to_split, key, value);
			break;
		}

		split_key = split_page(btree, page_to_split, key, value, slot, &right_page);
		left_page = page_to_split;

		if (free_insert_key)
			free((void *) key);

		pop_from_stack(stack, &parent_page, &slot);

		if (parent_page == NULL)
		{
			btree_page_t *root = new_page(btree, right_page->height + 1);

			add_to_page(btree, root, NULL, left_page, 0, false);
			add_to_page(btree, root, split_key, right_page, 1, false);
			btree->root = root;

			free(split_key);
			break;
		}
		else
		{
			if (does_item_fit_into_page(btree, parent_page, split_key, false))
			{
				insert_into_page(btree, parent_page, split_key, right_page);

				free(split_key);
				break;
			}
			else
			{
				page_to_split	= parent_page;
				key				= split_key;
				value			= right_page;
				free_insert_key = true;
			}
		}
	}
}


static void
adjust_root(btree_t btree)
{
	btree_page_t *oldroot = btree->root;

	if (PageIsInner(oldroot))
	{
		btree->root = *PageGetPtrToDownLinkAtSlot(oldroot, 0);
		free(oldroot);
	}
}


static int
choose_merge_page(btree_t btree, btree_page_t *parent_page, page_slot_t slot)
{
	btree_page_t *current_page = *PageGetPtrToDownLinkAtSlot(parent_page, slot);
	page_slot_t	 sibiling_page_slot;

	if (slot != 0)
	{
		sibiling_page_slot = slot - 1;

		if (can_merge_pages(btree, current_page,
							*PageGetPtrToDownLinkAtSlot(parent_page, sibiling_page_slot)))
		{
			return sibiling_page_slot;
		}

		sibiling_page_slot = slot + 1;

		if (sibiling_page_slot < parent_page->num_keys &&
			can_merge_pages(btree, current_page,
							*PageGetPtrToDownLinkAtSlot(parent_page, sibiling_page_slot)))
		{
			return sibiling_page_slot;
		}
	}
	else
	{
		sibiling_page_slot = slot + 1;

		if (sibiling_page_slot < parent_page->num_keys &&
			can_merge_pages(btree, current_page,
							*PageGetPtrToDownLinkAtSlot(parent_page, sibiling_page_slot)))
		{
			return sibiling_page_slot;
		}
	}

	return -1;
}


static bool
add_all_keys_to_page(btree_t btree, btree_page_t *dst_page,
					 btree_page_t *src_page, const void *insert_key)
{
	for (page_slot_t i = 0; i < src_page->num_keys; i++)
	{
		const void *key	  = i == 0 && insert_key ? insert_key : PageGetKeyAtSlot(src_page, i);
		const void *value = *PageGetValuesAtSlot(src_page, i);

		if (key || dst_page->num_keys == 0)
		{
			if (!does_item_fit_into_page(btree, dst_page, key, false))
				return false;

			add_to_page(btree, dst_page, key, value, dst_page->num_keys, false);
		}
	}

	return true;
}


static bool
merge_pages(btree_t btree, btree_page_t *parent_page,
			page_slot_t page1_slot, page_slot_t page2_slot, void *insert_key)
{
	btree_page_t *left_page		  = *PageGetPtrToDownLinkAtSlot(parent_page, page1_slot);
	btree_page_t *right_page	  = *PageGetPtrToDownLinkAtSlot(parent_page, page2_slot);
	btree_page_t *merged_page	  = new_page(btree, left_page->height);
	bool		 merge_successful = false;

	if (add_all_keys_to_page(btree, merged_page, left_page, NULL))
	{
		if (add_all_keys_to_page(btree, merged_page, right_page, insert_key))
		{
			merged_page->rightlink = right_page->rightlink;
			merge_successful	   = true;

			memcpy(left_page, merged_page, btree->pagesize);
			free(right_page);
		}
	}

	free(merged_page);

	return merge_successful;
}


static void
merge_with_adjacent_page(btree_t btree, fstack_t *stack)
{
	btree_page_t *parent_page;
	page_slot_t	 slot;
	bool		 is_leaf = true;

	do
	{
		int	 left_page_slot, right_page_slot, sibiling_slot, slot;
		void *right_page_index_key;

		pop_from_stack(stack, &parent_page, &slot);

		slot = GetPrevSlot(slot);

		if (parent_page == NULL)
			break;

		sibiling_slot	= choose_merge_page(btree, parent_page, slot);
		left_page_slot	= Min(slot, sibiling_slot);
		right_page_slot = Max(slot, sibiling_slot);

		if (sibiling_slot == -1)
			break;

		right_page_index_key = is_leaf ? NULL : PageGetKeyAtSlot(parent_page, right_page_slot);
		is_leaf				 = false;

		if (merge_pages(btree, parent_page, left_page_slot, right_page_slot, right_page_index_key))
			delete_from_page(btree, parent_page, right_page_slot);
	} while (is_page_under_filled(btree, parent_page));

	if (btree->root->num_keys == 1)
		adjust_root(btree);
}


static void
btree_search(btree_t btree, const void *key, fstack_t *stack, btree_page_t **leaf_page,
			 page_slot_t *leaf_page_slot, bool *isKeyPresent)
{
	btree_page_t *page = btree->root;

	while (page != NULL)
	{
		page_slot_t slot = bsearch_page(btree, page, key, isKeyPresent);

		if (stack)
			push_to_stack(stack, page, slot);

		if (PageIsLeaf(page))
		{
			*leaf_page		= page;
			*leaf_page_slot = slot;
			break;
		}
		else
		{
			page = *PageGetPtrToDownLinkAtSlot(page, GetPrevSlot(search_res));
		}
	}
}


bool
btree_insert(btree_t btree, const void *key, const void *value)
{
	btree_page_t *leaf_page;
	fstack_t	 *stack;
	page_slot_t	 slot;
	bool		 isKeyPresent;

	if (btree->root == NULL)
	{
		btree->root = new_page(btree, 0);
		add_to_page(btree, btree->root, NULL, NULL, 0, false);
	}

	stack = stack_create(sizeof(struct stack_node_t), btree->root->height + 1);

	btree_search(btree, key, stack, &leaf_page, &slot, &isKeyPresent);

	if (!isKeyPresent)
	{
		if (does_item_fit_into_page(btree, leaf_page, key, isKeyPresent))
			add_to_page(btree, leaf_page, key, value, slot, isKeyPresent);
		else
			split_page_and_insert(btree, stack, key, (void *) value);
	}
	else
	{
		add_to_page(btree, leaf_page, key, value, slot, isKeyPresent);
	}

#ifdef ENABLE_CHECK_BTREE_INTEGRITY
	if (!check_btree_integrity(btree))
	{
		assert(false);
		abort();
	}
#endif

	stack_destroy(stack);

	return !isKeyPresent;
}


bool
btree_delete(btree_t btree, const void *key, void **value)
{
	btree_page_t *leaf_page;
	fstack_t	 *stack;
	page_slot_t	 slot;
	bool		 isKeyPresent;

	if (btree->root == NULL)
		return false;

	stack = stack_create(sizeof(struct stack_node_t), btree->root->height + 1);

	btree_search(btree, key, stack, &leaf_page, &slot, &isKeyPresent);

	if (isKeyPresent)
	{
		*value = *PageGetValuesAtSlot(leaf_page, slot);
		delete_from_page(btree, leaf_page, slot);

		if (is_page_under_filled(btree, leaf_page))
		{
			pop_from_stack(stack, &leaf_page, &slot);
			merge_with_adjacent_page(btree, stack);
		}
	}

#ifdef ENABLE_CHECK_BTREE_INTEGRITY
	if (!check_btree_integrity(btree))
	{
		assert(false);
		abort();
	}
#endif

	stack_destroy(stack);

	return isKeyPresent;
}


bool
btree_find(btree_t btree, const void *key, void **value)
{
	btree_page_t *leaf_page;
	page_slot_t	 slot;
	bool		 isKeyPresent;

	if (btree->root == NULL)
		return false;

	btree_search(btree, key, NULL, &leaf_page, &slot, &isKeyPresent);

	if (isKeyPresent)
		*value = *PageGetValuesAtSlot(leaf_page, slot);

	return isKeyPresent;
}


int
btree_height(btree_t btree)
{
	return btree->root ? btree->root->height + 1 : -1;
}


#ifdef ENABLE_CHECK_BTREE_INTEGRITY

static bool check_page_integrity(btree_t btree, btree_page_t *page, const void *prev_key);
static bool check_level_integrity(btree_t btree, btree_page_t *page);
static bool check_level_reacheablity(btree_page_t *page);

#endif

#ifdef ENABLE_BTREE_DUMP

static void dump_btree_level(btree_page_t *page);
static void dump_btree_page(btree_page_t *page);

#endif


#ifdef ENABLE_CHECK_BTREE_INTEGRITY

bool
check_btree_integrity(btree_t btree)
{
	btree_page_t *page = btree->root;

	if (page && !check_level_reacheablity(page))
		return false;

	while (page != NULL)
	{
		if (!check_level_integrity(btree, page))
			return false;

		if (PageIsInner(page) && *PageGetPtrToDownLinkAtSlot(page, 0) == NULL)
			return false;

		page = PageIsLeaf(page) ? NULL : *PageGetPtrToDownLinkAtSlot(page, 0);
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
		is_leaf = PageIsLeaf(page);
	}

	while (page != NULL)
	{
		if (!check_page_integrity(btree, page, prev_key))
			return false;

		if (height != page->height || is_leaf != PageIsLeaf(page))
			return false;

		prev_key = PageGetKeyAtSlot(page, page->num_keys - 1);
		page	 = page->rightlink;
	}

	return true;
}


static bool
check_page_integrity(btree_t btree, btree_page_t *page, const void *prev_key)
{
	page_slot_t slot;

	if (page == NULL || page->num_keys == 0)
		return true;

	if ((PageIsLeaf(page) && page->height != 0) || (PageIsInner(page) && page->height == 0))
		return false;

	slot	 = prev_key == NULL ? 2 : 1;
	prev_key = prev_key == NULL ? PageGetKeyAtSlot(page, 1) : prev_key;

	for (; slot < page->num_keys; slot++)
	{
		const void *cur_key = PageGetKeyAtSlot(page, slot);

		if (CMP(prev_key, cur_key) >= 0)
			return false;

		prev_key = cur_key;
	}

	if (PageIsLeaf(page))
	{
		for (slot = 1; slot < page->num_keys; slot++)
		{
			void *value;

			if (!btree_find(btree, PageGetKeyAtSlot(page, slot), &value))
				return false;
		}
	}

	return true;
}


static bool
check_level_reacheablity(btree_page_t *page)
{
	if (PageIsInner(page))
	{
		btree_page_t *downlink			  = *PageGetPtrToDownLinkAtSlot(page, 0);
		int			 low_page_count		  = 0;
		int			 reachable_page_count = 0;

		while (downlink)
		{
			low_page_count++;
			downlink = downlink->rightlink;
		}

		while (page)
		{
			reachable_page_count += page->num_keys;
			page				  = page->rightlink;
		}

		return reachable_page_count == low_page_count;
	}
	else
	{
		return true;
	}
}


#endif


#ifdef ENABLE_BTREE_DUMP

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
			printf(", ");
	}
}


static void
dump_btree_page(btree_page_t *page)
{
	printf("|");

	for (page_slot_t slot = 1; slot < page->num_keys; slot++)
	{
		printf("%ld", *(long *) PageGetKeyAtSlot(page, slot));

		if (slot != page->num_keys - 1)
			printf(", ");
	}

	printf("|");
}


#endif
