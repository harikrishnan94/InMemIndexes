#ifndef COMMON_H
#define COMMON_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stddef.h>

typedef int (*btree_key_compare_t)(const void *a1, const void *a2, void *extra_arg);
typedef void (*btree_node_delete_t)(void *node, void *extra_arg);
typedef int (*btree_key_size_t)(const void *key, void *extra_arg);

typedef struct
{
	btree_key_compare_t cmp_key;
	btree_node_delete_t del_node;
	btree_key_size_t	key_size;
	void				*extra_arg;
} btree_key_val_info_t;

typedef enum
{
	BT_SUCCESS,
	BT_FAILED,
	BT_FOUND
} btree_result_t;

#ifdef __cplusplus
}
#endif

#endif /* COMMON_H */
