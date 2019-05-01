#pragma once

#ifdef ART_DUMP_ENABLED

#include <iostream>

#define ART_NODE_DUMP_METHODS                                                  \
  template <typename Cont> void get_children(Cont &nodes, int depth) const {   \
    if (!is_leaf()) {                                                          \
      for (int i = 0; i < MAX_CHILDREN; i++) {                                 \
        node_t *child = find(i);                                               \
                                                                               \
        if (child) {                                                           \
          nodes.emplace_back(child, depth);                                    \
        }                                                                      \
      }                                                                        \
    }                                                                          \
  }                                                                            \
                                                                               \
  void dump(std::ostream &ostr) const {                                        \
    ostr << static_cast<const void *>(this) << "=";                            \
    if (is_leaf()) {                                                           \
      auto leaf = static_cast<const leaf_t *>(this);                           \
      ostr << "|" << itoa_hex(key) << ", " << static_cast<int>(keylen);        \
      ostr << " {" << itoa_hex(leaf->key) << ", " << leaf->value << "}|";      \
    } else {                                                                   \
      ostr << "|" << itoa_hex(key) << ", " << static_cast<int>(keylen) << " "; \
                                                                               \
      for (int i = 0, j = 0; i < MAX_CHILDREN; i++) {                          \
        node_t *child = find(i);                                               \
                                                                               \
        if (child) {                                                           \
          ostr << '[' << itoa_hex(i, true) << ", "                             \
               << static_cast<void *>(child) << ']';                           \
                                                                               \
          if (++j != this->num_children) {                                     \
            ostr << ", ";                                                      \
          }                                                                    \
        }                                                                      \
      }                                                                        \
                                                                               \
      ostr << "|";                                                             \
    }                                                                          \
  }                                                                            \
                                                                               \
  void dump() const { dump(std::cout); }

#define ART_DUMP_METHODS                                                       \
  void dump_node(uintptr_t node, std::ostream &ostr) const {                   \
    reinterpret_cast<node_t *>(node)->dump(ostr);                              \
    std::cout << std::endl;                                                    \
  }                                                                            \
                                                                               \
  void dump(std::ostream &ostr) const {                                        \
    std::deque<std::pair<node_t *, int>> children;                             \
    int current_depth = 0;                                                     \
                                                                               \
    if (root)                                                                  \
      children.emplace_back(root, 0);                                          \
                                                                               \
    while (children.size()) {                                                  \
      node_t *node = children[0].first;                                        \
      int depth = children[0].second;                                          \
                                                                               \
      children.pop_front();                                                    \
                                                                               \
      if (current_depth != depth) {                                            \
        current_depth = depth;                                                 \
        ostr << std::endl;                                                     \
      }                                                                        \
                                                                               \
      node->dump(ostr);                                                        \
      node->get_children(children, depth + 1);                                 \
                                                                               \
      ostr << ", ";                                                            \
    }                                                                          \
                                                                               \
    ostr << std::endl;                                                         \
  }                                                                            \
                                                                               \
  static const char *itoa_hex(key_type k, bool trim = false) {                 \
    static char buf[17];                                                       \
    bool print_zero = !trim;                                                   \
    const char *nibble = "0123456789ABCDEF";                                   \
                                                                               \
    std::fill(buf, buf + sizeof(buf), '\0');                                   \
                                                                               \
    for (int i = sizeof(key_type) - 1, j = 0; i >= 0; i--) {                   \
      uint8_t b = (k >> (i * __CHAR_BIT__)) & 0xFF;                            \
                                                                               \
      if (!b && !print_zero) {                                                 \
        continue;                                                              \
      }                                                                        \
                                                                               \
      buf[j++] = nibble[b >> 4];                                               \
      buf[j++] = nibble[b & 0xF];                                              \
      print_zero = true;                                                       \
    }                                                                          \
                                                                               \
    if (!print_zero) {                                                         \
      buf[0] = nibble[0];                                                      \
      buf[1] = nibble[0];                                                      \
    }                                                                          \
                                                                               \
    return buf;                                                                \
  }                                                                            \
  void dump() const { dump(std::cout); }
#else
#define ART_NODE_DUMP_METHODS
#define ART_DUMP_METHODS
#endif
