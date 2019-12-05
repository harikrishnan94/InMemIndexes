// include/btree/map.h
// B+Tree implementation

#pragma once

#include "common.h"

namespace indexes::btree {
template <typename Key, typename Value, typename Traits = btree_traits_default,
          typename Compare = std::less<Key>,
          typename Stats = std::conditional_t<Traits::STAT, btree_stats_t,
                                              btree_empty_stats_t>>
class map {
public:
  using key_type = Key;
  using mapped_type = Value;
  using value_type = std::pair<const Key, Value>;
  using size_type = std::size_t;
  using difference_type = std::ptrdiff_t;
  using key_compare = Compare;
  using reference = value_type &;
  using const_reference = const value_type &;

private:
  static constexpr auto comp = Compare();

  enum class NodeType : bool { LEAF, INNER };

  template <typename KeyT1, typename KeyT2>
  static inline bool key_less(const KeyT1 &k1, const KeyT2 &k2) {
    return k1 < k2;
  }
  template <typename KeyT1, typename KeyT2>
  static inline bool key_greater(const KeyT1 &k1, const KeyT2 &k2) {
    return k2 < k1;
  }
  template <typename KeyT1, typename KeyT2>
  static inline bool key_equal(const KeyT1 &k1, const KeyT2 &k2) {
    return k1 == k2;
  }

  struct node_t {
    int logical_pagesize = 0;
    int num_values = 0;
    int next_slot_offset = 0;
    int last_value_offset = Traits::NODE_SIZE;

    const NodeType node_type;
    const int height;
    int num_dead_values = 0;

    const std::optional<Key> lowkey;
    const std::optional<Key> highkey;

    inline node_t(NodeType ntype, int initialsize, int a_height,
                  const std::optional<Key> &a_lowkey,
                  const std::optional<Key> &a_highkey)
        : next_slot_offset(initialsize), node_type(ntype), height(a_height),
          lowkey(a_lowkey), highkey(a_highkey) {}

    inline bool isLeaf() const { return node_type == NodeType::LEAF; }

    inline bool isInner() const { return node_type == NodeType::INNER; }

    inline bool haveEnoughSpace(int size) const {
      return static_cast<int>(this->next_slot_offset + sizeof(int)) <=
             (this->last_value_offset - size);
    }

    inline bool isUnderfull() const {
      BTREE_DEBUG_ASSERT(this->logical_pagesize <= Traits::NODE_SIZE);

      return (this->logical_pagesize * 100) / Traits::NODE_SIZE <
             Traits::NODE_MERGE_THRESHOLD;
    }

    inline bool canTrim() const { return this->num_dead_values > 1; }

    inline bool canSplit() const { return this->num_values > 2; }

    static void free(node_t *node) {
      if (node->isLeaf())
        delete ASLEAF(node);
      else
        delete ASINNER(node);
    }
  };

  template <typename Node> struct NodeSplitInfo {
    Node *left;
    Node *right;
    const Key split_key;
  };

  enum class InsertStatus {
    // In Windows OVERFLOW is defined as macro internally, so use..
    OVFLOW,
    DUPLICATE,
    INSERTED
  };

  template <typename ValueType> struct align_helper {
    static constexpr auto align =
        std::max(alignof(std::pair<Key, ValueType>), alignof(std::max_align_t));
  };
  template <typename ValueType, NodeType NType>
  struct alignas(align_helper<ValueType>::align) inherited_node_t : node_t {
    using value_t = ValueType;
    using key_value_t = std::pair<Key, value_t>;

    static constexpr enum NodeType NODETYPE = NType;

    static constexpr bool IsLeaf() { return NType == NodeType::LEAF; }

    static constexpr bool IsInner() { return NType == NodeType::INNER; }

    inline inherited_node_t(const std::optional<Key> &lowkey,
                            const std::optional<Key> &highkey, int height)
        : node_t(NType, sizeof(inherited_node_t), height, lowkey, highkey) {}

    inline ~inherited_node_t() {
      for (int slot = IsInner() ? 1 : 0; slot < this->num_values; slot++) {
        get_key_value(slot)->~key_value_t();
      }
    }

    static inherited_node_t *alloc(const std::optional<Key> &lowkey,
                                   const std::optional<Key> &highkey,
                                   int height) {
      return new (new char[Traits::NODE_SIZE])
          inherited_node_t(lowkey, highkey, height);
    }

    inline bool canMerge(const node_t *other) const {
      return (this->logical_pagesize + other->logical_pagesize +
              (IsInner() ? sizeof(key_value_t) : 0)) +
                 sizeof(inherited_node_t) <=
             Traits::NODE_SIZE;
    }

    inline char *opaque() const {
      return reinterpret_cast<char *>(reinterpret_cast<intptr_t>(this));
    }

    inline int *get_slots() const {
      return reinterpret_cast<int *>(opaque() + sizeof(inherited_node_t));
    }

    inline key_value_t *get_key_value_for_offset(int offset) const {
      return reinterpret_cast<key_value_t *>(opaque() + offset);
    }

    inline key_value_t *get_key_value(int slot) const {
      const int *slots = get_slots();

      return get_key_value_for_offset(slots[slot]);
    }

    inline const Key &get_key(int slot) const {
      if constexpr (IsInner())
        BTREE_DEBUG_ASSERT(slot != 0);

      return get_key_value(slot)->first;
    }

    INNER_ONLY
    inline value_t get_child(int slot) const {
      return slot == 0 ? get_first_child() : get_key_value(slot)->second;
    }

    inline const Key &get_first_key() const {
      if constexpr (IsInner())
        return get_key_value(1)->first;
      else
        return get_key_value(0)->first;
    }

    INNER_ONLY
    inline value_t get_first_child() const {
      value_t *valptr = reinterpret_cast<value_t *>(&get_key_value(0)->first);
      return *valptr;
    }

    INNER_ONLY
    inline value_t get_last_child() const {
      int slot = this->num_values - 1;

      return slot == 0 ? get_first_child() : get_key_value(slot)->second;
    }

    template <typename KeyType> int lower_bound_pos(const KeyType &key) const {
      int firstslot = IsLeaf() ? 0 : 1;
      int *slots = get_slots();

      return std::lower_bound(slots + firstslot, slots + this->num_values, key,
                              [this](int slot, const KeyType &key) {
                                return key_less(
                                    get_key_value_for_offset(slot)->first, key);
                              }) -
             slots;
    }

    template <typename KeyType> int upper_bound_pos(const KeyType &key) const {
      int firstslot = IsLeaf() ? 0 : 1;
      int *slots = get_slots();
      int pos =
          std::upper_bound(slots + firstslot, slots + this->num_values, key,
                           [this](const KeyType &key, int slot) {
                             return key_less(
                                 key, get_key_value_for_offset(slot)->first);
                           }) -
          slots;

      return IsInner() ? std::min(pos - 1, this->num_values - 1) : pos;
    }

    inline void copy_from(const inherited_node_t *src, int start_pos,
                          int end_pos) {
      for (int slot = start_pos; slot < end_pos; slot++) {
        const key_value_t *val = src->get_key_value(slot);

        this->append(val->first, val->second);
      }
    }

    inline void update_meta_after_insert(int datasize) {
      this->num_values++;
      this->last_value_offset -= datasize;
      this->next_slot_offset += sizeof(int);
      this->logical_pagesize += datasize + sizeof(int);

      BTREE_DEBUG_ASSERT(this->next_slot_offset <= this->last_value_offset);
    }

    INNER_ONLY
    inline void insert_neg_infinity(const value_t &val) {
      BTREE_DEBUG_ASSERT(this->isInner() && this->num_values == 0);

      int *slots = get_slots();
      int current_value_offset = this->last_value_offset - sizeof(value_t);

      new (opaque() + current_value_offset) value_t{val};
      slots[0] = current_value_offset;

      update_meta_after_insert(sizeof(value_t));
    }

    inline void append(const Key &key, const value_t &val) {
      int *slots = get_slots();
      int current_value_offset = this->last_value_offset - sizeof(key_value_t);
      int pos = this->num_values;

      new (opaque() + current_value_offset) key_value_t{key, val};
      slots[pos] = current_value_offset;

      update_meta_after_insert(sizeof(key_value_t));
    }

    inline InsertStatus insert_into_pos(const Key &key, int pos) {
      if (this->haveEnoughSpace(sizeof(key_value_t))) {
        int *slots = get_slots();
        int current_value_offset =
            this->last_value_offset - sizeof(key_value_t);

        new (opaque() + current_value_offset) key_type{key};

        std::copy_backward(slots + pos, slots + this->num_values,
                           slots + this->num_values + 1);

        slots[pos] = current_value_offset;

        update_meta_after_insert(sizeof(key_value_t));

        return InsertStatus::INSERTED;
      }

      return InsertStatus::OVFLOW;
    }

    INNER_ONLY
    inline void update(int pos, const value_t &val) {
      value_t *oldval =
          pos ? &get_key_value(pos)->second
              : reinterpret_cast<value_t *>(&get_key_value(0)->first);

      *oldval = val;
    }

    template <typename KeyType>
    std::pair<int, bool> lower_bound(const KeyType &key) const {
      int pos = lower_bound_pos(key);
      bool present = pos < this->num_values &&
                     !key_greater(get_key_value(pos)->first, key);

      return std::make_pair(pos, present);
    }

    INNER_ONLY_WITH_KEY
    int search_inner(const KeyType &key) const {
      auto [pos, key_present] = lower_bound(key);

      return !key_present ? pos - 1 : pos;
    }

    INNER_ONLY_WITH_KEY
    value_t get_value_lower_than(const KeyType &key) const {
      int pos = search_inner(key);

      if (pos == 0)
        return get_first_child();

      return get_child(key_equal(key, get_key(pos)) ? pos - 1 : pos);
    }

    std::pair<InsertStatus, int> insert(const Key &key, int pos = -1) {
      bool key_present = false;

      if (pos < 0) {
        pos = 0;

        if (this->num_values) {
          std::tie(pos, key_present) = lower_bound(key);

          if (key_present)
            return {InsertStatus::DUPLICATE, -1};
        }
      }

      return {insert_into_pos(key, pos), pos};
    }

    void remove(int pos) {
      int *slots = get_slots();

      std::copy(slots + pos + 1, slots + this->num_values, slots + pos);

      this->num_values--;
      this->num_dead_values++;
      this->next_slot_offset -= sizeof(int);
      this->logical_pagesize -= sizeof(key_value_t) + sizeof(int);
    }

    INNER_ONLY_WITH_KEY
    inline value_t get_child_for_key(const KeyType &key) const {
      return get_child(search_inner(key));
    }

    inherited_node_t *trim() const {
      inherited_node_t *new_node =
          alloc(this->lowkey, this->highkey, this->height);

      if constexpr (IsLeaf()) {
        new_node->copy_from(this, 0, this->num_values);
      } else {
        new_node->insert_neg_infinity(get_first_child());
        new_node->copy_from(this, 1, this->num_values);
      }

      return new_node;
    }

    NodeSplitInfo<inherited_node_t> split() const {
      BTREE_DEBUG_ASSERT(this->canSplit());

      int split_pos =
          IsInner() ? this->num_values / 2 : (this->num_values + 1) / 2;
      const Key &split_key = get_key(split_pos);
      inherited_node_t *left = alloc(this->lowkey, split_key, this->height);
      inherited_node_t *right = alloc(split_key, this->highkey, this->height);

      if constexpr (IsInner()) {
        left->insert_neg_infinity(get_first_child());
        left->copy_from(this, 1, split_pos);

        right->insert_neg_infinity(get_child(split_pos));
        right->copy_from(this, split_pos + 1, this->num_values);
      } else {
        left->copy_from(this, 0, split_pos);
        right->copy_from(this, split_pos, this->num_values);
      }

      return {left, right, split_key};
    }

    inherited_node_t *merge(const inherited_node_t *other,
                            const Key &merge_key) const {
      inherited_node_t *mergednode = nullptr;

      if (this->canMerge(other)) {
        mergednode = alloc(this->lowkey, other->highkey, this->height);

        if constexpr (IsInner()) {
          mergednode->insert_neg_infinity(get_first_child());
          mergednode->copy_from(this, 1, this->num_values);

          mergednode->append(merge_key, other->get_first_child());
          mergednode->copy_from(other, 1, other->num_values);
        } else {
          (void)merge_key;

          mergednode->copy_from(this, 0, this->num_values);
          mergednode->copy_from(other, 0, other->num_values);
        }

        BTREE_DEBUG_ASSERT(mergednode->num_values ==
                           this->num_values + other->num_values);
      }

      return mergednode;
    }

    template <typename Cont, typename Dummy = void,
              typename = std::enable_if_t<IsInner(), Dummy>>
    void get_children(Cont &nodes) const {
      nodes.emplace_back(get_first_child());

      for (int slot = 1; slot < this->num_values; slot++) {
        nodes.emplace_back(get_child(slot));
      }
    }

    NODE_DUMP_METHODS
  };

  using leaf_node_t = inherited_node_t<Value, NodeType::LEAF>;
  using inner_node_t = inherited_node_t<node_t *, NodeType::INNER>;

  static_assert((Traits::NODE_SIZE - sizeof(leaf_node_t)) /
                        (sizeof(typename leaf_node_t::key_value_t) +
                         sizeof(int)) >=
                    4,
                "Btree leaf node must have atleast 4 slots");
  static_assert((Traits::NODE_SIZE - sizeof(inner_node_t)) /
                        (sizeof(typename inner_node_t::key_value_t) +
                         sizeof(int)) >=
                    4,
                "Btree inner node must have atleast 4 slots");
  static_assert(Traits::NODE_SIZE %
                        alignof(typename leaf_node_t::key_value_t) ==
                    0,
                "Alignment mismatch b/w pagesize and Key, Value");
  static_assert(Traits::NODE_SIZE %
                        alignof(typename inner_node_t::key_value_t) ==
                    0,
                "Alignment mismatch b/w pagesize and Key, Value");
  static_assert(sizeof(leaf_node_t) %
                        alignof(typename leaf_node_t::key_value_t) ==
                    0,
                "Alignment mismatch b/w pagesize and Key, Value");
  static_assert(sizeof(inner_node_t) %
                        alignof(typename inner_node_t::key_value_t) ==
                    0,
                "Alignment mismatch b/w pagesize and Key, Value");

  node_t *m_root = nullptr;
  std::size_t count = 0;
  int m_height = 0;
  std::unique_ptr<Stats> m_stats = std::make_unique<Stats>();

  template <typename Node>
  inline void create_root(NodeSplitInfo<Node> splitinfo) {
    inner_node_t *new_root = inner_node_t::alloc(
        splitinfo.left->lowkey, splitinfo.right->highkey, ++m_height);

    new_root->insert_neg_infinity(splitinfo.left);
    new_root->append(splitinfo.split_key, splitinfo.right);

    m_root = new_root;
  }

  template <typename Node> inner_node_t *find_parent(Node *node) const {
    const Key &key = node->get_first_key();
    inner_node_t *parent = nullptr;

    for (auto *current = m_root; current && current->isInner();
         current = ASINNER(current)->get_child_for_key(key)) {
      if (current == node)
        break;

      parent = ASINNER(current);
    }

    return parent;
  }

  struct MergeInfo {
    const Key &merge_key;
    int sibilingpos;
  };

  template <typename Node>
  std::optional<MergeInfo> get_merge_info(const Node *node,
                                          const inner_node_t *parent) const {
    if (parent == nullptr)
      return std::nullopt;

    int pos = parent->search_inner(node->get_first_key());

    if (pos == 0)
      return std::nullopt;

    BTREE_DEBUG_ASSERT(parent->get_child(pos) == node);

    return MergeInfo{parent->get_key_value(pos)->first, pos - 1};
  }

  template <typename Node> void merge_node(Node *node) {
    inner_node_t *parent = find_parent(node);
    std::optional<MergeInfo> mergeinfo = get_merge_info(node, parent);

    if (mergeinfo) {
      const Key &merge_key = mergeinfo->merge_key;
      int sibilingpos = mergeinfo->sibilingpos;
      Node *sibiling = static_cast<Node *>(parent->get_child(sibilingpos));
      Node *mergednode = sibiling->merge(node, merge_key);

      if (mergednode) {
        parent->update(sibilingpos, mergednode);

        remove_from_node(parent, sibilingpos + 1);

        BTREE_UPDATE_STAT_NODE_BASED(merge);

        node_t::free(node);
        node_t::free(sibiling);
      }
    }
  }

  template <typename Node> void remove_from_node(Node *node, int pos) {
    node->remove(pos);

    if (node->isUnderfull())
      merge_node(node);
  }

  template <typename Node>
  NodeSplitInfo<Node> split_node(Node *node, inner_node_t *parent) {
    NodeSplitInfo<Node> splitinfo = node->split();

    if (node != m_root) {
      int left_pos = parent->search_inner(node->get_first_key());
      int right_pos = left_pos + 1;

      parent->update(left_pos, splitinfo.left);
      std::tie(parent, right_pos) =
          insert_into_node(parent, splitinfo.split_key, right_pos);

      parent->get_key_value(right_pos)->second = splitinfo.right;
    } else {
      create_root(splitinfo);
    }

    BTREE_UPDATE_STAT_NODE_BASED(split);

    node_t::free(node);

    return splitinfo;
  }

  template <typename Node> Node *trim_node(Node *node, inner_node_t *parent) {
    Node *new_node = node->trim();

    if (node != m_root) {
      int pos = parent->search_inner(node->get_first_key());

      parent->update(pos, new_node);
    } else {
      m_root = new_node;
    }

    BTREE_UPDATE_STAT_NODE_BASED(trim);

    node_t::free(node);

    return new_node;
  }

  template <typename Node> Node *handle_overflow(Node *node, const Key &key) {
    inner_node_t *parent = find_parent(node);

    if (node->canTrim()) {
      return trim_node(node, parent);
    } else {
      NodeSplitInfo<Node> splitinfo = split_node(node, parent);
      const Key &split_key = splitinfo.split_key;

      return static_cast<Node *>(key_less(key, split_key) ? splitinfo.left
                                                          : splitinfo.right);
    }
  }

  template <typename Node>
  std::pair<Node *, int> insert_into_node(Node *node, const Key &key,
                                          int pos = -1) {
    InsertStatus status;

    std::tie(status, pos) =
        pos >= 0 ? node->insert(key, pos) : node->insert(key);

    if (status == InsertStatus::OVFLOW) {
      node = handle_overflow(node, key);

      std::tie(status, pos) = node->insert(key);

      BTREE_DEBUG_ASSERT(status != InsertStatus::OVFLOW);
      BTREE_DEBUG_ONLY(status);
    }

    return {node, pos};
  }

  template <typename KeyType>
  inline leaf_node_t *get_leaf_containing(const KeyType &key) const {
    node_t *current;

    for (current = m_root; current && current->isInner();) {
      current = ASINNER(current)->get_child_for_key(key);
    }

    if (current)
      BTREE_DEBUG_ASSERT(current->isLeaf());

    return ASLEAF(current);
  }

  inline leaf_node_t *get_prev_leaf(const leaf_node_t *leaf) const {
    node_t *current;
    const Key &key = leaf->lowkey.value();

    for (current = m_root; current && current->isInner();) {
      current = ASINNER(current)->get_value_lower_than(key);
    }

    if (current)
      BTREE_DEBUG_ASSERT(current->isLeaf());

    return ASLEAF(current);
  }

  inline leaf_node_t *get_next_leaf(const leaf_node_t *leaf) const {
    return get_leaf_containing(leaf->highkey.value());
  }

  inline leaf_node_t *get_last_leaf() const {
    node_t *current;

    for (current = m_root; current && current->isInner();) {
      current = ASINNER(current)->get_last_child();
    }

    if (current)
      BTREE_DEBUG_ASSERT(current->isLeaf());

    return ASLEAF(current);
  }

  inline leaf_node_t *get_first_leaf() const {
    node_t *current;

    for (current = m_root; current && current->isInner();) {
      current = ASINNER(current)->get_first_child();
    }

    if (current)
      BTREE_DEBUG_ASSERT(current->isLeaf());

    return ASLEAF(current);
  }

  template <typename KeyType>
  inline leaf_node_t *get_upper_bound_leaf(const KeyType &key) const {
    node_t *current;

    for (current = m_root; current && current->isInner();) {
      inner_node_t *inner = ASINNER(current);
      int pos = inner->upper_bound_pos(key);

      current = ASINNER(current)->get_child(pos);
    }

    if (current)
      BTREE_DEBUG_ASSERT(current->isLeaf());

    return ASLEAF(current);
  }

  enum IteratorType { REVERSE, FORWARD };

public:
  template <IteratorType IType, typename MapType, typename LeafType>
  class iterator_impl {
  public:
    // The key type of the btree. Returned by key().
    using key_type = const Key;

    // The data type of the btree. Returned by data().
    using data_type = Value;

    // The pair type of the btree.
    using pair_type = std::pair<key_type, data_type>;

    // The value type of the btree. Returned by operator*().
    using value_type = pair_type;

    // Reference to the value_type. STL required.
    using reference = value_type &;

    // Pointer to the value_type. STL required.
    using pointer = value_type *;

    // STL-magic iterator category
    using iterator_category = std::bidirectional_iterator_tag;

    // STL-magic
    using difference_type = ptrdiff_t;

  private:
    MapType *m_bt;
    LeafType *m_leaf;
    int m_curslot;

    friend class map;

    void increment() {
      if (++m_curslot >= m_leaf->num_values) {
        if (this->m_leaf->highkey) {
          m_leaf = m_bt->get_next_leaf(m_leaf);
          m_curslot = 0;
        } else {
          m_leaf = nullptr;
          m_curslot = 0;
        }
      }
    }

    void decrement() {
      if (--m_curslot < 0) {
        if (this->m_leaf->lowkey) {
          m_leaf = m_bt->get_prev_leaf(this->m_leaf);
          m_curslot = m_leaf->num_values - 1;
        } else {
          m_leaf = nullptr;
          m_curslot = 0;
        }
      }
    }

    pair_type *get_pair() const {
      return reinterpret_cast<pair_type *>(m_leaf->get_key_value(m_curslot));
    }

  public:
    inline iterator_impl(MapType *bt, LeafType *leaf, int slot)
        : m_bt(bt), m_leaf(leaf), m_curslot(slot) {}

    inline iterator_impl(const iterator_impl &it) = default;

    template <IteratorType OtherIType, typename OtherMapType,
              typename OtherLeafType>
    inline iterator_impl(
        const iterator_impl<OtherIType, OtherMapType, OtherLeafType> &it)
        : iterator_impl(it.m_bt, it.m_leaf, it.m_curslot) {}

    inline reference operator*() const { return *get_pair(); }

    inline pointer operator->() const { return get_pair(); }

    inline key_type &key() const { return get_pair()->first; }

    inline data_type &data() const { return get_pair()->second; }

    inline iterator_impl operator++() {
      if (IType == IteratorType::FORWARD)
        increment();
      else
        decrement();

      return *this;
    }

    inline iterator_impl operator++(int) {
      auto copy = *this;

      if (IType == IteratorType::FORWARD)
        increment();
      else
        decrement();

      return copy;
    }

    inline iterator_impl operator--() {
      if (IType == IteratorType::FORWARD)
        decrement();
      else
        increment();

      return *this;
    }

    inline iterator_impl operator--(int) {
      auto copy = *this;

      if (IType == IteratorType::FORWARD)
        decrement();
      else
        increment();

      return copy;
    }

    inline bool operator==(const iterator_impl &other) const {
      BTREE_DEBUG_ASSERT(m_bt == other.m_bt);

      return m_leaf == other.m_leaf && m_curslot == other.m_curslot;
    }

    inline bool operator!=(const iterator_impl &other) const {
      BTREE_DEBUG_ASSERT(m_bt == other.m_bt);

      return m_leaf != other.m_leaf || m_curslot != other.m_curslot;
    }
  };

  using iterator = iterator_impl<IteratorType::FORWARD, map, leaf_node_t>;
  using const_iterator =
      iterator_impl<IteratorType::FORWARD, const map, const leaf_node_t>;
  using reverse_iterator =
      iterator_impl<IteratorType::REVERSE, map, leaf_node_t>;
  using const_reverse_iterator =
      iterator_impl<IteratorType::REVERSE, const map, const leaf_node_t>;

  Value &operator[](const Key &key) {
    if (m_root == nullptr)
      m_root = leaf_node_t::alloc(std::nullopt, std::nullopt, 0);

    leaf_node_t *leaf = get_leaf_containing(key);
    auto [pos, key_present] = leaf->lower_bound(key);

    if (!key_present) {
      std::tie(leaf, pos) = insert_into_node(leaf, key, pos);
      count++;
    }

    return leaf->get_key_value(pos)->second;
  }

  void erase(iterator it) {
    it.m_bt->remove_from_node(it.m_leaf, it.m_curslot);
    count--;
  }

  bool erase(const Key &key) {
    auto it = find(key);

    if (it != end()) {
      erase(it);
      return true;
    }

    return false;
  }

  iterator find(const Key &key) {
    leaf_node_t *leaf = get_leaf_containing(key);

    if (leaf) {
      auto [pos, key_present] = leaf->lower_bound(key);

      if (key_present)
        return {this, leaf, pos};
    }

    return end();
  }

  const_iterator find(const Key &key) const {
    leaf_node_t *leaf = get_leaf_containing(key);

    if (leaf) {
      auto [pos, key_present] = leaf->lower_bound(key);

      if (key_present)
        return {this, leaf, pos};
    }

    return end();
  }

  inline iterator begin() { return {this, get_first_leaf(), 0}; }

  inline iterator end() { return {this, nullptr, 0}; }

  inline const_iterator cbegin() const { return {this, get_first_leaf(), 0}; }

  inline const_iterator cend() const { return end(); }

  inline const_iterator begin() const { return cbegin(); }

  inline const_iterator end() const { return cend(); }

  inline reverse_iterator rbegin() {
    auto leaf = get_last_leaf();
    return {this, leaf, leaf->num_values - 1};
  }

  inline reverse_iterator rend() { return end(); }

  inline const_reverse_iterator crbegin() const {
    auto leaf = get_last_leaf();
    return {this, leaf, leaf->num_values - 1};
  }

  inline const_reverse_iterator crend() const { return end(); }

  inline const_reverse_iterator rbegin() const { return crbegin(); }

  inline const_reverse_iterator rend() const { return crend(); }

  template <typename KeyType> inline iterator lower_bound(const KeyType &key) {
    leaf_node_t *leaf = get_leaf_containing(key);

    if (leaf) {
      int slot = leaf->lower_bound_pos(key);
      iterator it = {this, leaf, slot};

      return slot < leaf->num_values ? it : ++it;
    }

    return end();
  }

  template <typename KeyType> inline iterator upper_bound(const KeyType &key) {
    leaf_node_t *leaf = get_upper_bound_leaf(key);

    if (leaf) {
      int slot = leaf->upper_bound_pos(key);
      iterator it = {this, leaf, slot};

      return slot < leaf->num_values ? it : ++it;
    }

    return end();
  }

  template <typename KeyType>
  inline const_iterator lower_bound(const KeyType &key) const {
    leaf_node_t *leaf = get_leaf_containing(key);

    if (leaf) {
      int slot = leaf->lower_bound_pos(key);
      const_iterator it = {this, leaf, slot};

      return slot < leaf->num_values ? it : ++it;
    }

    return end();
  }

  template <typename KeyType>
  inline const_iterator upper_bound(const KeyType &key) const {
    leaf_node_t *leaf = get_upper_bound_leaf(key);

    if (leaf) {
      int slot = leaf->upper_bound_pos(key);
      const_iterator it = {this, leaf, slot};

      return slot < leaf->num_values ? it : ++it;
    }

    return end();
  }

  inline int height() const { return m_height; }

  inline std::size_t size() const { return count; }

  inline bool empty() const { return count == 0; }

  template <typename Dummy = void,
            typename = typename std::enable_if_t<Traits::STAT, Dummy>>
  inline const Stats &stats() const {
    return *m_stats;
  }

  ~map() {
    std::deque<node_t *> nodes;

    if (m_root)
      nodes.emplace_back(m_root);

    while (!nodes.empty()) {
      node_t *node = nodes.front();

      if (node->isInner())
        ASINNER(node)->get_children(nodes);

      node_t::free(node);
      nodes.pop_front();
    }
  }

  BTREE_DUMP_METHODS
};

} // namespace indexes::btree
