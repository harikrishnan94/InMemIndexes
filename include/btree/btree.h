#ifndef BTREE_H
#define BTREE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "common.h"

struct btree_data_t;

typedef struct btree_data_t *btree_t;

extern btree_t btree_create(int pagesize, btree_key_val_info_t *kv_info);
extern void	   btree_destroy(btree_t btree);

extern bool btree_insert(btree_t btree, const void *key, const void *value);
extern bool btree_find(btree_t btree, const void *key, void **value);
extern bool btree_delete(btree_t btree, const void *key, void **value);

extern bool check_btree_integrity(btree_t btree);

extern size_t btree_count(btree_t btree);
extern size_t btree_size(btree_t btree);
extern int	  btree_height(btree_t btree);

#ifdef __cplusplus
}
#endif

#endif /* BTREE_H */
