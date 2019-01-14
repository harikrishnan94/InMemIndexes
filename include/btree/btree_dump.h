#pragma once

#ifdef BTREE_DUMP_ENABLED

#include <iostream>

#define NODE_DUMP_METHODS                                                                    \
	void dump(std::ostream &ostr) const                                                      \
	{                                                                                        \
		ostr << "|";                                                                         \
                                                                                             \
		for (int pos = IsInner() ? 1 : 0; pos < this->num_values; pos++)                     \
		{                                                                                    \
			ostr << get_key(pos);                                                            \
                                                                                             \
			if (pos != this->num_values - 1)                                                 \
				ostr << ", ";                                                                \
		}                                                                                    \
                                                                                             \
		ostr << "|";                                                                         \
	}                                                                                        \
                                                                                             \
	void print(std::ostream &ostr) const                                                     \
	{                                                                                        \
		ostr << this << " = ";                                                               \
                                                                                             \
		if constexpr (IsInner())                                                             \
		{                                                                                    \
			ostr << "[" << get_first_child() << "], ";                                       \
		}                                                                                    \
                                                                                             \
		for (int slot = IsInner() ? 1 : 0; slot < this->num_values; slot++)                  \
		{                                                                                    \
			ostr << "[" << get_key_value(slot)->first << ", " << get_key_value(slot)->second \
			     << "], ";                                                                   \
		}                                                                                    \
	}

#define BTREE_DUMP_METHODS                                                 \
	void print_node(node_t *node, std::ostream &ostr) const                \
	{                                                                      \
		if (node->isLeaf())                                                \
			static_cast<leaf_node_t *>(node)->print(ostr);                 \
		else                                                               \
			static_cast<inner_node_t *>(node)->print(ostr);                \
	}                                                                      \
                                                                           \
	void print_node(node_t *node) const                                    \
	{                                                                      \
		print_node(node, std::cout);                                       \
		std::cout << std::endl;                                            \
	}                                                                      \
                                                                           \
	void print_node(intptr_t node) const                                   \
	{                                                                      \
		print_node(reinterpret_cast<node_t *>(node), std::cout);           \
		std::cout << std::endl;                                            \
	}                                                                      \
                                                                           \
	void dump_node(node_t *node, std::ostream &ostr) const                 \
	{                                                                      \
		if (node->isLeaf())                                                \
			static_cast<leaf_node_t *>(node)->dump(ostr);                  \
		else                                                               \
			static_cast<inner_node_t *>(node)->dump(ostr);                 \
	}                                                                      \
                                                                           \
	void dump_node(node_t *node) const                                     \
	{                                                                      \
		dump_node(node, std::cout);                                        \
		std::cout << std::endl;                                            \
	}                                                                      \
                                                                           \
	void dump(std::ostream &ostr) const                                    \
	{                                                                      \
		std::deque<node_t *> children;                                     \
		int current_height = m_height;                                     \
                                                                           \
		if (m_root)                                                        \
			children.push_back(m_root);                                    \
                                                                           \
		while (children.size())                                            \
		{                                                                  \
			node_t *node = children[0];                                    \
                                                                           \
			children.pop_front();                                          \
                                                                           \
			if (current_height != node->height)                            \
			{                                                              \
				current_height = node->height;                             \
				ostr << std::endl;                                         \
			}                                                              \
                                                                           \
			dump_node(node, ostr);                                         \
                                                                           \
			if (node->isInner())                                           \
				static_cast<inner_node_t *>(node)->get_children(children); \
                                                                           \
			ostr << ", ";                                                  \
		}                                                                  \
                                                                           \
		ostr << std::endl;                                                 \
	}                                                                      \
                                                                           \
	void dump() const                                                      \
	{                                                                      \
		dump(std::cout);                                                   \
	}

#else
#define NODE_DUMP_METHODS
#define BTREE_DUMP_METHODS
#endif
