#ifndef MAPPING_TABLE_H
#define MAPPING_TABLE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <inttypes.h>

struct btree_mapping_table_data_t;

typedef struct btree_mapping_table_data_t *btree_mapping_table_t;


typedef uint32_t btree_page_id_t;

#define InvalidPageId ((btree_page_id_t) -1)

extern btree_mapping_table_t mapping_table_create(int size);
extern void					 mapping_table_destroy(btree_mapping_table_t mtable);

extern void			   mapping_reserve_num_pids(btree_mapping_table_t mtable, uint32_t num_pids);
extern btree_page_id_t mapping_table_alloc_pid(btree_mapping_table_t mtable);
extern void			   mapping_table_recycle_pid(btree_mapping_table_t mtable, btree_page_id_t pid);

extern void *mapping_table_lookup_page(btree_mapping_table_t mtable, btree_page_id_t pid);
extern void mapping_table_install_page(btree_mapping_table_t mtable,
									   btree_page_id_t pid, const void *ptr);
extern bool mapping_table_install_page_to_replace(btree_mapping_table_t mtable,
												  btree_page_id_t pid, const void *old_ptr,
												  const void *new_ptr);

#ifdef __cplusplus
}
#endif

#endif /* MAPPING_TABLE_H */
