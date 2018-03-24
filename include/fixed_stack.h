#ifndef FIXED_STACK_H
#define FIXED_STACK_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdlib.h>

typedef struct
{
	unsigned max_size  : 16;
	unsigned elem_size : 16;
	unsigned head	   : 16;

	char stack_data[];
} fstack_t;


static fstack_t *
stack_create(int elem_size, int max_size)
{
	fstack_t *stack = (fstack_t *) malloc(sizeof(fstack_t) + elem_size * max_size);

	stack->max_size	 = max_size;
	stack->elem_size = elem_size;
	stack->head		 = 0;

	return stack;
}


static void
stack_destroy(fstack_t *stack)
{
	free(stack);
}


#define stack_push(stack, type, data, is_stack_full) \
	do \
	{ \
		fstack_t *_stack = (stack); \
 \
		if (_stack->head < _stack->max_size) \
		{ \
			((type *) _stack->stack_data)[_stack->head] = (data); \
			_stack->head++; \
			is_stack_full = false; \
		} \
		else \
		{ \
			is_stack_full = true; \
		} \
	} \
	while (0)

#define stack_pop(stack, type, data, is_stack_empty) \
	do \
	{ \
		fstack_t *_stack = (stack); \
 \
		if (_stack->head) \
		{ \
			_stack->head--; \
			(data)		   = ((type *) _stack->stack_data)[_stack->head]; \
			is_stack_empty = false; \
		} \
		else \
		{ \
			is_stack_empty = true; \
		} \
	} \
	while (0)

#ifdef __cplusplus
}
#endif

#endif /* FIXED_STACK_H */
