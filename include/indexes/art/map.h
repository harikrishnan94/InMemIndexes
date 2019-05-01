// include/art/map.h
// Adaptive Radix Tree implementation for Integer keys

#pragma once

#include "art_dump.h"
#include "indexes/utils/Utils.h"

#include <algorithm>
#include <cassert>
#include <cinttypes>
#include <deque>
#include <optional>
#include <utility>

#define ART_DEBUG(expr)                                                        \
  do {                                                                         \
    if (Traits::DEBUG) {                                                       \
      expr;                                                                    \
    }                                                                          \
  } while (0)

#define ART_DEBUG_ASSERT(expr) ART_DEBUG(assert(expr))
#define ART_DEBUG_ONLY(expr) ((void)(expr))

namespace indexes::art {
struct art_traits_default {
  static constexpr bool DEBUG = false;
};

struct art_traits_debug : art_traits_default {
  static constexpr bool DEBUG = true;
};

template <typename Value, typename Traits = art_traits_default> class map {
public:
  using key_type = std::uint64_t;
  using value_type = Value;

private:
  static constexpr int NUM_BITS = 8;
  static constexpr int MAX_CHILDREN = 1 << NUM_BITS;
  static constexpr int MAX_DEPTH = (sizeof(key_type) * __CHAR_BIT__) / NUM_BITS;

  enum class node_type_t : uint8_t { LEAF, NODE4, NODE16, NODE48, NODE256 };

  struct node_t {
    const node_type_t node_type;
    uint8_t keylen;
    int num_children;

    const key_type key;

    node_t(node_type_t a_node_type, key_type a_key, uint8_t a_keylen)
        : node_type(a_node_type), keylen(a_keylen), num_children(0),
          key(a_key) {}

    static constexpr int get_ind(key_type key, int depth) {
      int rdepth = MAX_DEPTH - depth - 1;
      key_type mask = ((key_type)MAX_CHILDREN - 1) << (rdepth * NUM_BITS);

      return (key & mask) >> (rdepth * NUM_BITS);
    }

    static constexpr key_type rshift(key_type k, int v) {
      return static_cast<std::size_t>(v) >= (sizeof(key_type) * __CHAR_BIT__)
                 ? 0
                 : k >> v;
    }

    constexpr int longest_common_prefix_length(key_type otherkey) const {
      return utils::leading_zeroes(this->key ^ otherkey) / __CHAR_BIT__;
    }

    static constexpr key_type extract_common_prefix(key_type key, int lcpl) {
      key_type mask =
          ~rshift(std::numeric_limits<key_type>::max(), lcpl * __CHAR_BIT__);

      return key & mask;
    }

    constexpr bool is_leaf() const { return node_type == node_type_t::LEAF; }

    bool equals(const node_t *other) const {
      for (int i = 0; i < MAX_CHILDREN; i++) {
        if (find(i) != other->find(i))
          return false;
      }

      return true;
    }

    node_t *find(uint8_t ind) const {
      switch (node_type) {
      case node_type_t::NODE4:
        return static_cast<const node4_t *>(this)->find(ind);

      case node_type_t::NODE16:
        return static_cast<const node16_t *>(this)->find(ind);

      case node_type_t::NODE48:
        return static_cast<const node48_t *>(this)->find(ind);

      case node_type_t::NODE256:
        return static_cast<const node256_t *>(this)->find(ind);

      case node_type_t::LEAF:
        ART_DEBUG_ASSERT("find called for leaf");
      }

      return nullptr;
    }

    bool add(node_t *child, uint8_t ind) {
      switch (node_type) {
      case node_type_t::NODE4:
        return static_cast<node4_t *>(this)->add(child, ind);

      case node_type_t::NODE16:
        return static_cast<node16_t *>(this)->add(child, ind);

      case node_type_t::NODE48:
        return static_cast<node48_t *>(this)->add(child, ind);

      case node_type_t::NODE256:
        return static_cast<node256_t *>(this)->add(child, ind);

      case node_type_t::LEAF:
        ART_DEBUG_ASSERT("add called for leaf");
      }

      return false;
    }

    node_t *update(node_t *child, uint8_t ind) {
      switch (node_type) {
      case node_type_t::NODE4:
        return static_cast<node4_t *>(this)->update(child, ind);

      case node_type_t::NODE16:
        return static_cast<node16_t *>(this)->update(child, ind);

      case node_type_t::NODE48:
        return static_cast<node48_t *>(this)->update(child, ind);

      case node_type_t::NODE256:
        return static_cast<node256_t *>(this)->update(child, ind);

      case node_type_t::LEAF:
        ART_DEBUG_ASSERT("add called for leaf");
      }

      return nullptr;
    }

    void remove(uint8_t ind) {
      switch (node_type) {
      case node_type_t::NODE4:
        static_cast<node4_t *>(this)->remove(ind);
        break;

      case node_type_t::NODE16:
        static_cast<node16_t *>(this)->remove(ind);
        break;

      case node_type_t::NODE48:
        static_cast<node48_t *>(this)->remove(ind);
        break;

      case node_type_t::NODE256:
        static_cast<node256_t *>(this)->remove(ind);
        break;

      case node_type_t::LEAF:
        ART_DEBUG_ASSERT("add called for leaf");
      }
    }

    node_t *expand() const {
      switch (node_type) {
      case node_type_t::NODE4:
        return new node16_t(static_cast<const node4_t *>(this));

      case node_type_t::NODE16:
        return new node48_t(static_cast<const node16_t *>(this));

      case node_type_t::NODE48:
        return new node256_t(static_cast<const node48_t *>(this));

      case node_type_t::NODE256:
        ART_DEBUG_ASSERT("expand called for NODE256");
      case node_type_t::LEAF:
        ART_DEBUG_ASSERT("expand called for leaf");
      }

      return nullptr;
    }

    bool is_underfull() const {
      switch (node_type) {
      case node_type_t::NODE4:
        return static_cast<const node4_t *>(this)->is_underfull();

      case node_type_t::NODE16:
        return static_cast<const node16_t *>(this)->is_underfull();

      case node_type_t::NODE48:
        return static_cast<const node48_t *>(this)->is_underfull();

      case node_type_t::NODE256:
        return static_cast<const node256_t *>(this)->is_underfull();

      case node_type_t::LEAF:
        ART_DEBUG_ASSERT("is_underfull called for leaf");
      }

      return false;
    }

    node_t *shrink() const {
      switch (node_type) {
      case node_type_t::NODE4:
        return static_cast<const node4_t *>(this)->shrink();

      case node_type_t::NODE16:
        return new node4_t(static_cast<const node16_t *>(this));

      case node_type_t::NODE48:
        return new node16_t(static_cast<const node48_t *>(this));

      case node_type_t::NODE256:
        return new node48_t(static_cast<const node256_t *>(this));

      case node_type_t::LEAF:
        ART_DEBUG_ASSERT("shrink called for leaf");
      }

      return nullptr;
    }

    void free() {
      switch (node_type) {
      case node_type_t::NODE4:
        delete static_cast<node4_t *>(this);
        break;

      case node_type_t::NODE16:
        delete static_cast<node16_t *>(this);
        break;

      case node_type_t::NODE48:
        delete static_cast<node48_t *>(this);
        break;

      case node_type_t::NODE256:
        delete static_cast<node256_t *>(this);
        break;

      case node_type_t::LEAF:
        delete static_cast<leaf_t *>(this);
        break;
      }
    }

    template <typename Cont> void get_children(Cont &nodes) const {
      if (!is_leaf()) {
        for (int i = 0; i < MAX_CHILDREN; i++) {
          node_t *child = find(i);

          if (child) {
            nodes.push_back(child);
          }
        }
      }
    }

    ART_NODE_DUMP_METHODS
  };

  struct node4_t;
  struct node16_t;
  struct node48_t;
  struct node256_t;
  struct leaf_t;

  struct leaf_t : node_t {
    value_type value;

    leaf_t(key_type key, value_type a_value, int keylen)
        : node_t(node_type_t::LEAF, key, keylen), value(a_value) {
      ART_DEBUG_ASSERT(keylen > 0);
    }
  };

  struct node4_t : node_t {
    static constexpr int MAX_CHILDREN = 4;
    static constexpr int MAX_KEYS = 4;

    uint8_t keys[MAX_CHILDREN];
    node_t *children[MAX_CHILDREN];

    node4_t(key_type key, uint8_t keylen)
        : node_t(node_type_t::NODE4, key, keylen) {
      std::fill(keys, keys + MAX_CHILDREN, 0);
      std::fill(children, children + MAX_CHILDREN, nullptr);
    }

    node4_t(const node16_t *node) : node4_t(node->key, node->keylen) {
      ART_DEBUG_ASSERT(node->num_children <= MAX_CHILDREN);
      this->num_children = node->num_children;
      std::copy(node->keys, node->keys + this->num_children, keys);
      std::copy(node->children, node->children + this->num_children, children);

      ART_DEBUG_ASSERT(this->equals(node));
    }

    static void add(uint8_t *keys, node_t **children, int &num_children,
                    node_t *node, uint8_t ind) {
      uint8_t pos;

      for (pos = 0; pos < num_children; pos++) {
        if (keys[pos] > ind)
          break;
      }

      std::copy_backward(keys + pos, keys + num_children,
                         keys + num_children + 1);
      std::copy_backward(children + pos, children + num_children,
                         children + num_children + 1);
      keys[pos] = ind;
      children[pos] = node;
      num_children++;
    }

    static node_t *find(const uint8_t *keys, node_t *const *children,
                        int num_children, uint8_t ind) {
      for (uint8_t i = 0; i < num_children && keys[i] <= ind; i++) {
        if (ind == keys[i])
          return children[i];
      }

      return nullptr;
    }

    static void remove(uint8_t *keys, node_t **children, int &num_children,
                       uint8_t ind) {
      uint8_t pos;

      for (pos = 0; pos < num_children && keys[pos] <= ind; pos++) {
        if (ind == keys[pos])
          break;
      }

      ART_DEBUG_ASSERT(pos < num_children);

      std::copy(keys + pos + 1, keys + num_children, keys + pos);
      std::copy(children + pos + 1, children + num_children, children + pos);
      num_children--;
    }

    static node_t *update(uint8_t *keys, node_t **children, int num_children,
                          uint8_t ind, node_t *new_child) {
      for (uint8_t i = 0; i < num_children && keys[i] <= ind; i++) {
        if (ind == keys[i])
          return std::exchange(children[i], new_child);
      }

      return nullptr;
    }

    bool add(node_t *node, uint8_t ind) {
      if (this->num_children != MAX_CHILDREN) {
        add(keys, children, this->num_children, node, ind);
        return true;
      }

      return false;
    }

    node_t *find(uint8_t ind) const {
      return find(keys, children, this->num_children, ind);
    }

    node_t *update(node_t *new_child, uint8_t ind) {
      return update(keys, children, this->num_children, ind, new_child);
    }

    void remove(uint8_t ind) {
      remove(keys, children, this->num_children, ind);
    }

    bool is_underfull() const { return this->num_children <= 1; }

    node_t *shrink() const {
      node_t *child = this->children[0];
      child->keylen += this->keylen;

      return child;
    }
  };

  struct node16_t : node_t {
    static constexpr int MAX_CHILDREN = 16;
    static constexpr int MAX_KEYS = 16;

    uint8_t keys[MAX_CHILDREN];
    node_t *children[MAX_CHILDREN];

    node16_t(key_type key, uint8_t keylen)
        : node_t(node_type_t::NODE16, key, keylen) {
      std::fill(keys, keys + MAX_CHILDREN, 0);
      std::fill(children, children + MAX_CHILDREN, nullptr);
    }

    node16_t(const node4_t *node) : node16_t(node->key, node->keylen) {
      ART_DEBUG_ASSERT(node->num_children <= MAX_CHILDREN);
      this->num_children = node->num_children;
      std::copy(node->keys, node->keys + this->num_children, keys);
      std::copy(node->children, node->children + this->num_children, children);

      ART_DEBUG_ASSERT(this->equals(node));
    }

    node16_t(const node48_t *node) : node16_t(node->key, node->keylen) {
      for (int i = 0, pos = 0; i < node48_t::MAX_KEYS; i++) {
        if (node->keys[i]) {
          keys[pos] = i;
          children[pos] = node->children[node->keys[i] - 1];
          pos++;
        }
      }

      this->num_children = node->num_children;

      ART_DEBUG_ASSERT(this->equals(node));
    }

    bool add(node_t *node, uint8_t ind) {
      if (this->num_children != MAX_CHILDREN) {
        node4_t::add(keys, children, this->num_children, node, ind);
        return true;
      }

      return false;
    }

    node_t *find(uint8_t ind) const {
      return node4_t::find(keys, children, this->num_children, ind);
    }

    node_t *update(node_t *new_child, uint8_t ind) {
      return node4_t::update(keys, children, this->num_children, ind,
                             new_child);
    }

    void remove(uint8_t ind) {
      node4_t::remove(keys, children, this->num_children, ind);
    }

    bool is_underfull() const {
      return this->num_children == node4_t::MAX_CHILDREN;
    }
  };

  struct node48_t : node_t {
    static constexpr int MAX_CHILDREN = 48;
    static constexpr int MAX_KEYS = 256;

    uint8_t keys[MAX_KEYS];
    node_t *children[MAX_CHILDREN];

    node48_t(key_type key, uint8_t keylen)
        : node_t(node_type_t::NODE48, key, keylen) {
      std::fill(keys, keys + MAX_KEYS, 0);
      std::fill(children, children + MAX_CHILDREN, nullptr);
    }

    node48_t(const node16_t *node) : node48_t(node->key, node->keylen) {
      this->num_children = node->num_children;
      std::copy(node->children, node->children + this->num_children, children);

      for (int i = 0; i < this->num_children; i++) {
        keys[node->keys[i]] = i + 1;
      }

      ART_DEBUG_ASSERT(this->equals(node));
    }

    node48_t(const node256_t *node) : node48_t(node->key, node->keylen) {
      ART_DEBUG_ASSERT(node->num_children <= MAX_CHILDREN);
      this->num_children = node->num_children;

      for (int i = 0, pos = 0; i < node256_t::MAX_CHILDREN; i++) {
        if (node->children[i]) {
          keys[i] = pos + 1;
          children[pos] = node->children[i];
          pos++;
        }
      }

      ART_DEBUG_ASSERT(this->equals(node));
    }

    bool add(node_t *node, uint8_t ind) {
      if (this->num_children == MAX_CHILDREN)
        return false;

      int pos = 0;

      while (children[pos]) {
        pos++;
        ART_DEBUG_ASSERT(pos < MAX_CHILDREN);
      }

      keys[ind] = pos + 1;
      children[pos] = node;
      this->num_children++;

      return true;
    }

    node_t *find(uint8_t ind) const {
      return keys[ind] ? children[keys[ind] - 1] : nullptr;
    }

    node_t *update(node_t *new_child, uint8_t ind) {
      return keys[ind] ? std::exchange(children[keys[ind] - 1], new_child)
                       : nullptr;
    }

    void remove(uint8_t ind) {
      ART_DEBUG_ASSERT(keys[ind] != 0);
      ART_DEBUG_ASSERT(children[keys[ind] - 1] != nullptr);

      children[keys[ind] - 1] = nullptr;
      keys[ind] = 0;
      this->num_children--;
    }

    bool is_underfull() const {
      return this->num_children == node16_t::MAX_CHILDREN;
    }
  };

  struct node256_t : node_t {
    static constexpr int MAX_CHILDREN = 256;
    static constexpr int MAX_KEYS = 256;

    node_t *children[MAX_CHILDREN];

    node256_t(key_type key, uint8_t keylen)
        : node_t(node_type_t::NODE256, key, keylen) {
      std::fill(children, children + MAX_CHILDREN, nullptr);
    }

    node256_t(const node48_t *node) : node256_t(node->key, node->keylen) {
      this->num_children = node->num_children;

      for (int i = 0; i < node48_t::MAX_KEYS; i++) {
        if (node->keys[i])
          children[i] = node->children[node->keys[i] - 1];
      }

      ART_DEBUG_ASSERT(this->equals(node));
    }

    bool add(node_t *node, uint8_t ind) {
      ART_DEBUG_ASSERT(children[ind] == nullptr);
      ART_DEBUG_ASSERT(this->num_children < MAX_CHILDREN);

      children[ind] = node;
      this->num_children++;
      return true;
    }

    node_t *find(uint8_t ind) const { return children[ind]; }

    node_t *update(node_t *new_child, uint8_t ind) {
      return std::exchange(children[ind], new_child);
    }

    void remove(uint8_t ind) {
      children[ind] = nullptr;
      this->num_children--;
    }

    bool is_underfull() const {
      return this->num_children == node48_t::MAX_CHILDREN;
    }
  };

  enum class UpdateOp {
    UOP_Insert,
    UOP_Update,
    UOP_Upsert,
  };

  template <UpdateOp UOp>
  static std::optional<value_type> update_leaf(node_t *node, value_type value) {
    auto leaf = static_cast<leaf_t *>(node);
    if constexpr (UOp != UpdateOp::UOP_Insert)
      return std::exchange(leaf->value, value);
    else
      return leaf->value;
  }

  void add_to_parent(node_t *parent, node_t *grand_parent, node_t *node,
                     int depth) {
    if (parent) {
      uint8_t ind = node_t::get_ind(node->key, depth);

      if (!parent->add(node, ind)) {
        node_t *old = parent;
        parent = old->expand();

        parent->add(node, ind);

        if (grand_parent)
          grand_parent->update(parent, node_t::get_ind(node->key, depth - 1));
        else
          root = parent;

        old->free();
      }
    } else {
      if (root) {
        node_t *newroot = new node4_t(0, 0);

        newroot->add(node, node_t::get_ind(node->key, depth));
        newroot->add(root, node_t::get_ind(root->key, depth));
        root = newroot;
      } else {
        root = node;
      }
    }
  }

  void replace_node(key_type key, value_type value, int depth, int lcpl,
                    node_t *node, node_t *parent) {
    int common_prefix_len = std::min<int>(lcpl - depth, node->keylen);
    auto leaf =
        new leaf_t(key, value, sizeof(key_type) - depth - common_prefix_len);
    key_type prefix = node_t::extract_common_prefix(key, lcpl);
    auto inner = new node4_t(prefix, common_prefix_len);

    inner->add(node, node_t::get_ind(node->key, depth + common_prefix_len));
    inner->add(leaf, node_t::get_ind(leaf->key, depth + common_prefix_len));

    node->keylen -= common_prefix_len;

    if (parent) {
      parent->update(inner, node_t::get_ind(inner->key, depth));
    } else {
      root = inner;
    }
  }

  template <UpdateOp UOp>
  void insert_leaf(key_type key, value_type value, int depth, int lcpl,
                   node_t *node, node_t *parent, node_t *grand_parent) {
    if constexpr (UOp != UpdateOp::UOP_Update) {
      int common_prefix_len = std::min<int>(lcpl - depth, node->keylen);

      if (common_prefix_len && (node->is_leaf() || node->keylen)) {
        replace_node(key, value, depth, lcpl, node, parent);
      } else {
        add_to_parent(parent, grand_parent,
                      new leaf_t(key, value, sizeof(key_type) - depth), depth);
      }
    }
  }

  template <UpdateOp UOp>
  std::optional<value_type> insert(key_type key, value_type value) {
    node_t *grand_parent = nullptr;
    node_t *parent = nullptr;
    node_t *node = root;
    int depth = 0;

    while (node) {
      int lcpl = node->longest_common_prefix_length(key);
      int common_prefix_len = std::min<int>(lcpl - depth, node->keylen);

      if (node->is_leaf()) {
        if (node->key == key) {
          return update_leaf<UOp>(node, value);
        }

        insert_leaf<UOp>(key, value, depth, lcpl, node, parent, grand_parent);
        return {};
      }

      if (common_prefix_len == node->keylen) {
        depth += common_prefix_len;

        ART_DEBUG_ASSERT(depth < MAX_DEPTH);

        grand_parent = parent;
        parent = node;
        node = node->find(node_t::get_ind(key, depth));
      } else {
        insert_leaf<UOp>(key, value, depth, lcpl, node, parent, grand_parent);
        return {};
      }
    }

    if constexpr (UOp != UpdateOp::UOP_Update) {
      add_to_parent(parent, grand_parent,
                    new leaf_t(key, value, sizeof(key_type) - depth), depth);
    }

    return {};
  }

  void shrink_node(node_t *node, node_t *parent, key_type key, int depth) {
    node_t *replacement = node->shrink();

    ART_DEBUG_ASSERT(replacement != nullptr);

    if (parent) {
      auto old = parent->update(replacement, node_t::get_ind(key, depth));
      ART_DEBUG_ASSERT(old == node);
      ART_DEBUG_ONLY(old);
    } else {
      root = replacement;
    }

    node->free();
  }

  std::optional<value_type> erase(node_t *node, node_t *parent, key_type key,
                                  int depth) {
    if (!node)
      return {};

    if (node->is_leaf()) {
      auto leaf = static_cast<leaf_t *>(node);

      if (leaf->key == key) {
        value_type oldval = leaf->value;
        int leaf_ind = node_t::get_ind(key, depth);

        if (parent) {
          parent->remove(leaf_ind);
        } else {
          root = nullptr;
        }

        leaf->free();
        return oldval;
      } else {
        return {};
      }
    }

    int lcpl = node->longest_common_prefix_length(key);
    int common_prefix_len = std::min<int>(lcpl - depth, node->keylen);

    if (node->keylen &&
        (common_prefix_len == 0 || common_prefix_len != node->keylen)) {
      return {};
    }

    depth += node->keylen;

    ART_DEBUG_ASSERT(depth < MAX_DEPTH);

    node_t *child = node->find(node_t::get_ind(key, depth));
    auto val = erase(child, node, key, depth);

    if (val && node->is_underfull()) {
      shrink_node(node, parent, key, depth - node->keylen);
    }

    return val;
  }

  static constexpr bool INSERT = false;
  static constexpr bool UPSERT = true;

  node_t *root;
  std::size_t m_size;

public:
  map() : root(nullptr), m_size(0) {}
  map(const map &) = delete;
  map(map &&other)
      : root(std::exchange(other.root, nullptr)),
        m_size(std::exchange(other.m_size, 0)) {}

  std::optional<value_type> Search(key_type key) const {
    int depth = 0;
    node_t *node = root;

    while (node) {
      if (node->is_leaf()) {
        auto leaf = static_cast<const leaf_t *>(node);
        if (leaf->key == key) {
          return leaf->value;
        }

        break;
      }

      int lcpl = node->longest_common_prefix_length(key);
      int common_prefix_len = std::min<int>(lcpl - depth, node->keylen);

      if (node->keylen &&
          (common_prefix_len == 0 || common_prefix_len != node->keylen)) {
        break;
      }

      depth += node->keylen;

      ART_DEBUG_ASSERT(depth < MAX_DEPTH);

      node = node->find(node_t::get_ind(key, depth));
    }

    return {};
  }

  bool Insert(key_type key, value_type value) {
    auto &&old = insert<UpdateOp::UOP_Insert>(key, value);

    if (!old) {
      m_size++;
    }

    return !old;
  }

  std::optional<value_type> Upsert(key_type key, value_type value) {
    auto &&old = insert<UpdateOp::UOP_Upsert>(key, value);

    if (!old) {
      m_size++;
    }

    return old;
  }

  std::optional<value_type> Update(key_type key, value_type value) {
    return insert<UpdateOp::UOP_Update>(key, value);
  }

  std::optional<value_type> Delete(key_type key) {
    auto &&old = erase(root, nullptr, key, 0);

    if (old) {
      m_size--;
    }

    return old;
  }

  std::size_t size() const { return m_size; }

  ~map() {
    std::deque<node_t *> children;

    if (root) {
      children.push_back(root);
    }

    while (children.size()) {
      node_t *node = children[0];

      children.pop_front();

      node->get_children(children);
      node->free();
    }

    root = nullptr;
    m_size = 0;
  }

  ART_DUMP_METHODS
};
} // namespace indexes::art
