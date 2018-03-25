#ifndef COMMON_H
#define COMMON_H

#ifdef __cplusplus
extern "C" {
#endif

#define CACHE_LINE_SIZE 64
#define MAXIMUM_ALIGNOF 16

#ifndef NDEBUG
#define USE_ASSERT_CHECKING
#endif

/*
 * The above macros will not work with types wider than uintptr_t, like with
 * uint64 on 32-bit platforms.  That's not problem for the usual use where a
 * pointer or a length is aligned, but for the odd case that you need to
 * align something (potentially) wider, use TYPEALIGN64.
 */
#define TYPEALIGN64(ALIGNVAL, LEN) \
	(((uint64_t) (LEN) + ((ALIGNVAL) -1)) & ~((uint64_t) ((ALIGNVAL) -1)))

/* we don't currently need wider versions of the other ALIGN macros */
#define MAXALIGN(LEN)	TYPEALIGN64(MAXIMUM_ALIGNOF, (LEN))
#define CACHEALIGN(LEN) TYPEALIGN64(CACHE_LINE_SIZE, (LEN))


#define CppAsString(identifier)				  # identifier
#define _Static_assert(condition, errmessage) static_assert(condition, errmessage)

#define StaticAssertStmt(condition, errmessage) \
	do { _Static_assert(condition, errmessage); } while (0)
#define StaticAssertExpr(condition, errmessage) \
	({ StaticAssertStmt(condition, errmessage); true; } \
	)

#define Assert(check) assert(check)
#define AssertVariableIsOfType(varname, typename) \
	StaticAssertStmt(__builtin_types_compatible_p(__typeof__(varname), typename), \
					 CppAsString(varname) " does not have type " CppAsString(typename))
#define AssertVariableIsOfTypeMacro(varname, typename) \
	((void) StaticAssertExpr(__builtin_types_compatible_p(__typeof__(varname), typename), \
							 CppAsString(varname) " does not have type " CppAsString(typename)))


#include <stdbool.h>
#include <stddef.h>

typedef int (*btree_key_compare_t)(const void *a1, const void *a2, void *extra_arg);
typedef void (*btree_node_delete_t)(void *node, void *extra_arg);
typedef int (*btree_key_size_t)(const void *key, void *extra_arg);

typedef struct
{
	btree_key_compare_t cmp_key;
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
