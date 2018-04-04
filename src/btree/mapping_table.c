#include "freelist.h"
#include "btree/mapping_table.h"

#include <stdlib.h>

typedef _Atomic (const void *) atomic_page_t;

struct btree_mapping_table_data_t
{
	uint32_t size;

	atomic_page_t *table;

	_Atomic btree_page_id_t next_pid;
	flist_head				free_nodes;
};

btree_mapping_table_t
mapping_table_create(int size)
{
	btree_mapping_table_t mtable = malloc(sizeof(struct btree_mapping_table_data_t));

	mtable->size	 = size;
	mtable->table	 = malloc(sizeof(void *) * size);
	mtable->next_pid = 0;

	flist_init(&mtable->free_nodes);

	return mtable;
}


void
mapping_table_destroy(btree_mapping_table_t mtable)
{
	free(mtable->table);
	free(mtable);
}


void
mapping_reserve_num_pids(btree_mapping_table_t mtable, uint32_t num_pids)
{
	assert(mtable->next_pid == 0);
	mtable->next_pid += num_pids;
}


btree_page_id_t
mapping_table_alloc_pid(btree_mapping_table_t mtable)
{
	if (!flist_is_empty(&mtable->free_nodes))
	{
		flist_node *free_node = flist_pop_head(&mtable->free_nodes);

		if (free_node)
			return (((uintptr_t) free_node - (uintptr_t) mtable->table) / sizeof(void *));
	}

	if (atomic_load(&mtable->next_pid) < mtable->size)
	{
		btree_page_id_t pid = atomic_fetch_add(&mtable->next_pid, 1);

		if (pid < mtable->size)
			return pid;
	}

	return InvalidPageId;
}


void
mapping_table_recycle_pid(btree_mapping_table_t mtable, btree_page_id_t pid)
{
	assert(pid != InvalidPageId && pid < mtable->size);
	flist_push_head(&mtable->free_nodes, (flist_node *) &mtable->table[pid]);
}


void *
mapping_table_lookup_page(btree_mapping_table_t mtable, btree_page_id_t pid)
{
	assert(pid != InvalidPageId && pid < mtable->size);
	return (void *) atomic_load(mtable->table + pid);
}


void
mapping_table_install_page(btree_mapping_table_t mtable, btree_page_id_t pid, const void *ptr)
{
	assert(pid != InvalidPageId && pid < mtable->size);
	atomic_store(mtable->table + pid, ptr);
}


bool
mapping_table_install_page_to_replace(btree_mapping_table_t mtable, btree_page_id_t pid,
									  const void *old_ptr, const void *new_ptr)
{
	assert(pid != InvalidPageId && pid < mtable->size);
	atomic_compare_exchange_strong(mtable->table + pid, &old_ptr, new_ptr);
	return true;
}
