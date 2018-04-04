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


#define stack_is_empty(stack) ((stack)->head == 0)
#define stack_is_full(stack)  ((stack)->head == (stack)->max_size)

#define stack_push(stack, type, data) \
	do \
	{ \
		fstack_t *_stack = (stack); \
 \
		((type *) _stack->stack_data)[_stack->head] = (data); \
		_stack->head++; \
	} \
	while (0)

#define stack_pop(stack, type, data) \
	do \
	{ \
		fstack_t *_stack = (stack); \
 \
		_stack->head--; \
		(data) = ((type *) _stack->stack_data)[_stack->head]; \
	} \
	while (0)

#define stack_peek(stack, type, data) \
	do \
	{ \
		fstack_t *_stack = (stack); \
 \
		(data) = ((type *) _stack->stack_data)[_stack->head - 1]; \
	} \
	while (0)

#ifdef __cplusplus
}
#endif

#endif /* FIXED_STACK_H */
