#ifndef BTREE_H
#define BTREE_H

#include "common.h"

struct btree_data_t;

typedef struct btree_data_t *btree_t;

extern btree_t btree_create(btree_node_compare_t cmp, btree_node_delete_t del, const void *cmp_arg,
							const void *del_arg);
extern void btree_destroy(btree_t *btree);

extern bool btree_insert(btree_t *btree, const void *node);
extern bool btree_find(btree_t *btree, const void *node);
extern bool btree_delete(btree_t *btree, const void *node);

extern size_t btree_count(btree_t *btree);
extern size_t btree_size(btree_t *btree);

#endif /* BTREE_H */
