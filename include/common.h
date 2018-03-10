#ifndef COMMON_H
#define COMMON_H

#include <stdbool.h>
#include <stddef.h>

typedef int (*btree_node_compare_t)(const void *a1, const void *a2, void *compare_arg);
typedef int (*btree_node_delete_t)(void *node, void *delete_arg);

#endif /* COMMON_H */
