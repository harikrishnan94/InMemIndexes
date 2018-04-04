#ifndef BTREE_H
#define BTREE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "common.h"

extern bwtree_t btree_create(int pagesize, btree_key_val_info_t *kv_info);
extern void		btree_destroy(bwtree_t btree);

extern bool btree_insert(bwtree_t btree, const void *key, const void *value);
extern bool btree_find(bwtree_t btree, const void *key, void **value);
extern bool btree_delete(bwtree_t btree, const void *key, void **value);

extern size_t btree_count(bwtree_t btree);
extern size_t btree_size(bwtree_t btree);
extern int	  btree_height(bwtree_t btree);

#ifdef ENABLE_BTREE_DUMP

extern void dump_btree(btree_t btree);

#endif


#ifdef __cplusplus
}
#endif

#endif /* BTREE_H */
