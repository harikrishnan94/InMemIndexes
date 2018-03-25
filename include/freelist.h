#ifndef FREELIST_H
#define FREELIST_H

#ifdef __cplusplus
extern "C" {
#endif

#include <assert.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <stddef.h>

/*
 * Node of a free list.
 *
 * Embed this in structs that need to be part of a free list.
 */
typedef struct flist_node flist_node;
struct flist_node
{
	flist_node *next;
};

struct atomic_node
{
	_Atomic(flist_node *) next;
};

static_assert(sizeof(flist_node) == sizeof(struct atomic_node),
			  "Atomic pointer size is not equal pointer size");

#define CAST_TO_ATOMIC_NODE(node) ((struct atomic_node *) (node))

/*
 * Head of a free list.
 */
typedef struct flist_head
{
	flist_node head;
} flist_head;

/* free list implementation */

static inline bool
swing_head(flist_head *head, flist_node *prev_head, flist_node *new_head)
{
	return atomic_compare_exchange_strong(&CAST_TO_ATOMIC_NODE(&head->head)->next, &prev_head,
										  new_head);
}


static inline flist_node *
flist_read_head(flist_head *head)
{
	return atomic_load(&CAST_TO_ATOMIC_NODE(&head->head)->next);
}


/*
 * Initialize a singly linked list.
 * Previous state will be thrown away without any cleanup.
 */
static inline void
flist_init(flist_head *head)
{
	atomic_init(&CAST_TO_ATOMIC_NODE(&head->head)->next, NULL);
}


/*
 * Is the list empty?
 */
static inline bool
flist_is_empty(flist_head *head)
{
	return flist_read_head(head) == NULL;
}


/*
 * Insert a node at the beginning of the list.
 */
static inline void
flist_push_head(flist_head *head, flist_node *node)
{
	do
	{
		flist_node *prev_head = flist_read_head(head);

		if (swing_head(head, prev_head, node))
			break;
	} while (1);
}


/*
 * Remove a node from the beginning of the list.
 */
static inline flist_node *
flist_pop_head(flist_head *head)
{
	do
	{
		flist_node *prev_head = flist_read_head(head);

		if (prev_head == NULL)
			return NULL;

		if (swing_head(head, prev_head, prev_head->next))
			return prev_head;
	} while (1);
}


#ifdef __cplusplus
}
#endif

#endif /* FREELIST_H */
