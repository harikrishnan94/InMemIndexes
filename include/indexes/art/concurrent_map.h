// include/art/concurrent_map.h
// Concurrent Adaptive Radix Tree implementation for Integer keys

#pragma once

#include "common.h"
#include "indexes/utils/EpochManager.h"
#include "indexes/utils/Mutex.h"
#include "indexes/utils/ThreadLocal.h"

#include <atomic>
#include <memory>

namespace indexes::art {
template <typename Value, typename Traits = art_traits_default>
class concurrent_map {
public:
  using key_type = std::uint64_t;
  using value_type = Value;

private:
  static constexpr int KEYTYPE_SIZE = sizeof(key_type);
  static constexpr int NUM_BITS = 8;
  static constexpr int MAX_CHILDREN = 1 << NUM_BITS;
  static constexpr int MAX_DEPTH = (KEYTYPE_SIZE * __CHAR_BIT__) / NUM_BITS;

  using bytea = const std::uint8_t *_RESTRICT;
  using version_t = std::uint64_t;

  template <typename T> static inline T load_aq(const std::atomic<T> &val) {
    return val.load(std::memory_order_acquire);
  }

  template <typename T, typename U>
  static inline void store_rs(std::atomic<T> &val, U &&newval) {
    val.store(std::forward<T>(newval), std::memory_order_release);
  }

  template <typename T>
  static inline void store_rs(std::atomic<T> &val, const T &newval) {
    val.store(newval, std::memory_order_release);
  }

  template <typename T, typename U>
  static inline T xchg_rs(std::atomic<T> &val, U &&newval) {
    auto old = val.load(std::memory_order_relaxed);

    store_rs(val, newval);
    return old;
  }

  template <typename T, typename SizeType>
  static inline void fill_zero_rx(std::atomic<T> *vals, SizeType len) {
    T val = {};

    for (SizeType i = 0; i < len; i++) {
      vals[i].store(val, std::memory_order_relaxed);
    }
  }

  enum class node_type_t : std::uint8_t {
    LEAF,
    NODE4,
    NODE16,
    NODE48,
    NODE256
  };

  struct node_t {
    const node_type_t node_type;
    const std::int8_t level;
    std::atomic<std::int16_t> num_children;
    std::atomic<std::int16_t> num_deleted;

    const key_type key;
    std::atomic<version_t> version;
    utils::Mutex m;

    static constexpr version_t DEAD_VERSION =
        std::numeric_limits<version_t>::max();

    node_t(node_type_t a_node_type, key_type a_key, std::int8_t a_level)
        : node_type(a_node_type), level(a_level), num_children(0),
          num_deleted(0), key(a_key), version(0), m() {}

    constexpr int get_ind(key_type key) const {
      return reinterpret_cast<bytea>(&key)[level];
    }

    constexpr int longest_common_prefix_length(key_type otherkey) const {
      auto keyvec = reinterpret_cast<bytea>(&key);
      auto otherkeyvec = reinterpret_cast<bytea>(&otherkey);
      int len = 0;

      while (len < KEYTYPE_SIZE) {
        if (keyvec[len] != otherkeyvec[len]) {
          break;
        }

        len++;
      }

      return len;
    }

    static constexpr key_type extract_common_prefix(key_type key, int lcpl) {
      key_type ret = 0;
      auto keyvec = reinterpret_cast<bytea>(&key);
      auto retvec = reinterpret_cast<std::uint8_t * _RESTRICT>(&ret);
      std::copy(keyvec, keyvec + lcpl, retvec);

      return ret;
    }

    constexpr int size() const { return num_children - num_deleted; }
    constexpr bool is_leaf() const { return node_type == node_type_t::LEAF; }

    constexpr void mark_as_deleted() {
      concurrent_map::store_rs(version, DEAD_VERSION);
    }

    constexpr bool is_deleted() const {
      return load_aq(version) == DEAD_VERSION;
    }

    bool equals(const node_t *other) const {
      for (int i = 0; i < MAX_CHILDREN; i++) {
        if (find(i) != other->find(i)) {
          return false;
        }
      }

      return true;
    }

    node_t *find(std::uint8_t ind) const {
      ART_DEBUG_ASSERT(this->size() != 0);

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

    bool add(node_t *child, std::uint8_t ind) {
      bool ret = false;

      ART_DEBUG_ASSERT(m.is_locked());

      switch (node_type) {
      case node_type_t::NODE4:
        ret = static_cast<node4_t *>(this)->add(child, ind);
        break;

      case node_type_t::NODE16:
        ret = static_cast<node16_t *>(this)->add(child, ind);
        break;

      case node_type_t::NODE48:
        ret = static_cast<node48_t *>(this)->add(child, ind);
        break;

      case node_type_t::NODE256:
        ret = static_cast<node256_t *>(this)->add(child, ind);
        break;

      case node_type_t::LEAF:
        ART_DEBUG_ASSERT("add called for leaf");
      }

      if (ret) {
        concurrent_map::store_rs(version, load_aq(version) + 1);
      }

      return ret;
    }

    node_t *find(int key) const { return find(static_cast<std::uint8_t>(key)); }
    node_t *find(key_type key) const { return find(get_ind(key)); }
    bool add(node_t *child) { return add(child, get_ind(child->key)); }

    node_t *update(node_t *child) {
      ART_DEBUG_ASSERT(this->size() != 0);
      ART_DEBUG_ASSERT(m.is_locked());

      std::uint8_t ind = get_ind(child->key);
      node_t *ret = nullptr;

      switch (node_type) {
      case node_type_t::NODE4:
        ret = static_cast<node4_t *>(this)->update(child, ind);
        break;

      case node_type_t::NODE16:
        ret = static_cast<node16_t *>(this)->update(child, ind);
        break;

      case node_type_t::NODE48:
        ret = static_cast<node48_t *>(this)->update(child, ind);
        break;

      case node_type_t::NODE256:
        ret = static_cast<node256_t *>(this)->update(child, ind);
        break;

      case node_type_t::LEAF:
        ART_DEBUG_ASSERT("add called for leaf");
      }

      concurrent_map::store_rs(version, load_aq(version) + 1);

      return ret;
    }

    void remove(key_type key) {
      ART_DEBUG_ASSERT(this->size() != 0);
      ART_DEBUG_ASSERT(m.is_locked());

      std::uint8_t ind = get_ind(key);

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

      concurrent_map::store_rs(version, load_aq(version) + 1);
    }

    node_t *expand() const {
      ART_DEBUG_ASSERT(this->size() != 0);
      ART_DEBUG_ASSERT(m.is_locked());

      switch (node_type) {
      case node_type_t::NODE4:
        if (this->num_deleted) {
          return new node4_t(static_cast<const node4_t *>(this));
        } else {
          return new node16_t(static_cast<const node4_t *>(this));
        }

      case node_type_t::NODE16:
        if (this->num_deleted) {
          return new node16_t(static_cast<const node16_t *>(this));
        } else {
          return new node48_t(static_cast<const node16_t *>(this));
        }

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
      ART_DEBUG_ASSERT(this->size() != 0);

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
      ART_DEBUG_ASSERT(this->size() != 0);
      ART_DEBUG_ASSERT(m.is_locked());

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

    static void free(node_t *node) {
      ART_DEBUG_ASSERT(node->is_leaf() || node->size() != 0);
      ART_DEBUG_ASSERT(!node->m.is_locked());

      switch (node->node_type) {
      case node_type_t::NODE4:
        delete static_cast<node4_t *>(node);
        break;

      case node_type_t::NODE16:
        delete static_cast<node16_t *>(node);
        break;

      case node_type_t::NODE48:
        delete static_cast<node48_t *>(node);
        break;

      case node_type_t::NODE256:
        delete static_cast<node256_t *>(node);
        break;

      case node_type_t::LEAF:
        delete static_cast<leaf_t *>(node);
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

    leaf_t(key_type key, value_type a_value)
        : node_t(node_type_t::LEAF, key, KEYTYPE_SIZE), value(a_value) {}
  };

  using atomic_key_t = std::atomic<std::uint8_t>;
  using atomic_node_t = std::atomic<node_t *>;

  struct node4_t : node_t {
    static constexpr int MAX_CHILDREN = 4;
    static constexpr int MAX_KEYS = 4;

    atomic_key_t keys[MAX_CHILDREN];
    atomic_node_t children[MAX_CHILDREN];

    node4_t(key_type key, std::int8_t level)
        : node_t(node_type_t::NODE4, key, level) {
      concurrent_map::fill_zero_rx(children, MAX_CHILDREN);
    }

    node4_t(const node16_t *node) : node4_t(node->key, node->level) {
      copy(this, node);
    }

    node4_t(const node4_t *node) : node4_t(node->key, node->level) {
      copy(this, node);
    }

    template <typename NodeTypeDst, typename NodeTypeSrc>
    static void copy(NodeTypeDst *dst, const NodeTypeSrc *src) {
      ART_DEBUG_ASSERT(src->size() <= NodeTypeDst::MAX_CHILDREN);

      int num_children = load_aq(src->num_children);

      for (int i = 0; i < num_children; i++) {
        if (load_aq(src->children[i])) {
          dst->add(load_aq(src->children[i]), load_aq(src->keys[i]));
        }
      }

      ART_DEBUG_ASSERT(dst->equals(src));
    }

    static void add(atomic_key_t *keys, atomic_node_t *children,
                    std::atomic<std::int16_t> &num_children, node_t *node,
                    std::uint8_t ind) {
      int num_children_loc = load_aq(num_children);

      concurrent_map::store_rs(keys[num_children_loc], ind);
      concurrent_map::store_rs(children[num_children_loc], node);
      concurrent_map::store_rs(num_children, num_children_loc + 1);
    }

    static std::uint8_t find_pos(const atomic_key_t *keys,
                                 const atomic_node_t *children,
                                 int num_children, std::uint8_t ind) {
      for (std::uint8_t pos = 0; pos < num_children; pos++) {
        if (load_aq(children[pos]) && ind == load_aq(keys[pos])) {
          return pos;
        }
      }

      return num_children;
    }

    static node_t *find(const atomic_key_t *keys, const atomic_node_t *children,
                        int num_children, std::uint8_t ind) {
      std::uint8_t pos = find_pos(keys, children, num_children, ind);

      return pos < num_children ? load_aq(children[pos]) : nullptr;
    }

    static void remove(const atomic_key_t *keys, atomic_node_t *children,
                       std::atomic<std::int16_t> &num_children,
                       std::atomic<std::int16_t> &num_deleted,
                       std::uint8_t ind) {
      std::uint8_t pos = find_pos(keys, children, load_aq(num_children), ind);

      ART_DEBUG_ASSERT(pos < num_children);

      concurrent_map::store_rs(children[pos], nullptr);
      concurrent_map::store_rs(num_deleted, load_aq(num_deleted) + 1);
    }

    static node_t *update(atomic_key_t *keys, atomic_node_t *children,
                          int num_children, std::uint8_t ind,
                          node_t *new_child) {
      std::uint8_t pos = find_pos(keys, children, num_children, ind);

      ART_DEBUG_ASSERT(pos < num_children);

      return concurrent_map::xchg_rs(children[pos], new_child);
    }

    bool add(node_t *node, std::uint8_t ind) {
      int num_children = load_aq(this->num_children);

      if (num_children != MAX_CHILDREN) {
        add(keys, children, this->num_children, node, ind);
        return true;
      }

      return false;
    }

    bool add(node_t *node) { return add(node, this->get_ind(node->key)); }

    node_t *find(std::uint8_t ind) const {
      const atomic_node_t *children = this->children;
      return find(keys, children, load_aq(this->num_children), ind);
    }

    node_t *update(node_t *new_child, std::uint8_t ind) {
      atomic_node_t *children = this->children;
      return update(keys, children, load_aq(this->num_children), ind,
                    new_child);
    }

    void remove(std::uint8_t ind) {
      remove(keys, children, this->num_children, this->num_deleted, ind);
    }

    bool is_underfull() const { return this->size() <= 1; }

    node_t *shrink() const {
      ART_DEBUG_ASSERT(this->size() == 1);

      int num_children = load_aq(this->num_children);

      for (int i = 0; i < num_children; i++) {
        node_t *child = load_aq(children[i]);

        if (child) {
          return child;
        }
      }

      return nullptr;
    }
  };

  struct node16_t : node_t {
    static constexpr int MAX_CHILDREN = 16;
    static constexpr int MAX_KEYS = 16;

    atomic_key_t keys[MAX_CHILDREN];
    atomic_node_t children[MAX_CHILDREN];

    node16_t(key_type key, std::int8_t level)
        : node_t(node_type_t::NODE16, key, level) {
      concurrent_map::fill_zero_rx(children, MAX_CHILDREN);
    }

    node16_t(const node4_t *node) : node16_t(node->key, node->level) {
      node4_t::copy(this, node);
    }

    node16_t(const node16_t *node) : node16_t(node->key, node->level) {
      node4_t::copy(this, node);
    }

    node16_t(const node48_t *node) : node16_t(node->key, node->level) {
      for (int i = 0, pos = 0; i < node48_t::MAX_KEYS; i++) {
        std::uint8_t ind = load_aq(node->keys[i]);
        if (ind) {
          concurrent_map::store_rs(keys[pos], i);
          concurrent_map::store_rs(children[pos],
                                   load_aq(node->children[ind - 1]));
          pos++;
        }
      }

      this->num_children = load_aq(node->num_children);

      ART_DEBUG_ASSERT(this->equals(node));
    }

    bool add(node_t *node, std::uint8_t ind) {
      int num_children = load_aq(this->num_children);

      if (num_children != MAX_CHILDREN) {
        node4_t::add(keys, children, this->num_children, node, ind);
        return true;
      }

      return false;
    }

    node_t *find(std::uint8_t ind) const {
      return node4_t::find(keys, children, this->num_children, ind);
    }

    node_t *update(node_t *new_child, std::uint8_t ind) {
      return node4_t::update(keys, children, this->num_children, ind,
                             new_child);
    }

    void remove(std::uint8_t ind) {
      node4_t::remove(keys, children, this->num_children, this->num_deleted,
                      ind);
    }

    bool is_underfull() const { return this->size() <= node4_t::MAX_CHILDREN; }
  };

  struct node48_t : node_t {
    static constexpr int MAX_CHILDREN = 48;
    static constexpr int MAX_KEYS = 256;
    static constexpr uint64_t ONE = 1;

    atomic_key_t keys[MAX_KEYS];
    std::uint64_t freemap;
    atomic_node_t children[MAX_CHILDREN];

    inline void mark_used(int pos) {
      freemap |= ONE << (MAX_CHILDREN - 1 - pos);
    }

    inline void mark_free(int pos) {
      freemap &= ~(ONE << (MAX_CHILDREN - 1 - pos));
    }

    inline int get_free_pos() {
      constexpr int UINT64_BITS = sizeof(uint64_t) * __CHAR_BIT__;
      constexpr std::uint64_t MASK = static_cast<std::uint64_t>(-1)
                                     << MAX_CHILDREN;
      int ind = utils::leading_zeroes(~(freemap | MASK));

      freemap |= ONE << (UINT64_BITS - 1 - ind);

      return MAX_CHILDREN - (UINT64_BITS - ind);
    }

    node48_t(key_type key, std::int8_t level)
        : node_t(node_type_t::NODE48, key, level), freemap(0) {
      concurrent_map::fill_zero_rx(keys, MAX_KEYS);
      concurrent_map::fill_zero_rx(children, MAX_CHILDREN);
    }

    node48_t(const node16_t *node) : node48_t(node->key, node->level) {
      node4_t::copy(this, node);
    }

    node48_t(const node256_t *node) : node48_t(node->key, node->level) {
      ART_DEBUG_ASSERT(node->num_children <= MAX_CHILDREN);

      auto src_num_children = load_aq(node->num_children);

      for (int i = 0, pos = 0; i < node256_t::MAX_CHILDREN; i++) {
        node_t *child = load_aq(node->children[i]);

        if (child) {
          concurrent_map::store_rs(keys[i], pos + 1);
          concurrent_map::store_rs(children[pos], child);
          mark_used(pos);
          pos++;
        }
      }

      this->num_children = src_num_children;

      ART_DEBUG_ASSERT(this->equals(node));
    }

    bool add(node_t *node, std::uint8_t ind) {
      int num_children = load_aq(this->num_children);

      if (num_children == MAX_CHILDREN) {
        return false;
      }

      int pos = get_free_pos();

      ART_DEBUG_ASSERT(pos < MAX_CHILDREN);

      concurrent_map::store_rs(children[pos], node);
      concurrent_map::store_rs(keys[ind], static_cast<std::uint8_t>(pos + 1));
      concurrent_map::store_rs(this->num_children,
                               static_cast<std::int16_t>(num_children + 1));

      return true;
    }

    node_t *find(std::uint8_t ind) const {
      return load_aq(keys[ind]) ? load_aq(children[keys[ind] - 1]) : nullptr;
    }

    node_t *update(node_t *new_child, std::uint8_t ind) {
      auto pos = load_aq(keys[ind]);
      return pos ? concurrent_map::xchg_rs(children[pos - 1], new_child)
                 : nullptr;
    }

    void remove(std::uint8_t ind) {
      ART_DEBUG_ASSERT(keys[ind] != 0);
      ART_DEBUG_ASSERT(children[keys[ind] - 1] != nullptr);

      int pos = load_aq(keys[ind]) - 1;
      concurrent_map::store_rs(children[pos], nullptr);
      concurrent_map::store_rs(keys[ind], 0);
      mark_free(pos);
      concurrent_map::store_rs(this->num_children,
                               load_aq(this->num_children) - 1);
    }

    bool is_underfull() const { return this->size() <= node16_t::MAX_CHILDREN; }
  };

  struct node256_t : node_t {
    static constexpr int MAX_CHILDREN = 256;
    static constexpr int MAX_KEYS = 256;

    atomic_node_t children[MAX_CHILDREN];

    node256_t(key_type key, std::int8_t level)
        : node_t(node_type_t::NODE256, key, level) {
      concurrent_map::fill_zero_rx(children, MAX_CHILDREN);
    }

    node256_t(const node48_t *node) : node256_t(node->key, node->level) {
      for (int i = 0; i < node48_t::MAX_KEYS; i++) {
        auto ind = load_aq(node->keys[i]);

        if (ind) {
          concurrent_map::store_rs(children[i],
                                   load_aq(node->children[ind - 1]));
        }
      }

      this->num_children = load_aq(node->num_children);

      ART_DEBUG_ASSERT(this->equals(node));
    }

    bool add(node_t *node, std::uint8_t ind) {
      ART_DEBUG_ASSERT(children[ind] == nullptr);
      ART_DEBUG_ASSERT(this->num_children < MAX_CHILDREN);

      concurrent_map::store_rs(children[ind], node);
      concurrent_map::store_rs(
          this->num_children,
          static_cast<std::int16_t>(load_aq(this->num_children) + 1));
      return true;
    }

    node_t *find(std::uint8_t ind) const { return load_aq(children[ind]); }

    node_t *update(node_t *new_child, std::uint8_t ind) {
      return xchg_rs(children[ind], new_child);
    }

    void remove(std::uint8_t ind) {
      concurrent_map::store_rs(children[ind], nullptr);
      concurrent_map::store_rs(this->num_children,
                               load_aq(this->num_children) - 1);
    }

    bool is_underfull() const { return this->size() <= node48_t::MAX_CHILDREN; }
  };

  enum class UpdateOp {
    UOP_Insert,
    UOP_Update,
    UOP_Upsert,
  };

  using LockType = std::unique_lock<utils::Mutex>;

  struct EpochGuard {
    const concurrent_map *map;

    EpochGuard(const concurrent_map *a_map) : map(a_map) {
      map->m_gc.enter_epoch();
    }

    ~EpochGuard() { map->m_gc.exit_epoch(); }
  };

  struct node_snapshot_t {
    node_t *node;
    version_t version;

    node_snapshot_t() : node(nullptr), version(0) {}
    node_snapshot_t(node_t *a_node)
        : node(a_node), version(node ? load_aq(node->version) : 0) {}
    node_snapshot_t(const node_snapshot_t &) = default;

    void load_snapshot(node_t *node) {
      this->node = node;
      this->version = node ? load_aq(node->version) : 0;
    }

    LockType lock() {
      LockType lock{node->m};

      if (load_aq(node->version) != version) {
        lock.unlock();
      }

      return lock;
    }

    LockType lock_root(concurrent_map &map) {
      LockType lock{*map.root_mtx};

      if (node != load_aq(map.root)) {
        lock.unlock();
      }

      return lock;
    }

    node_t *operator->() { return node; }
    explicit operator bool() { return node != nullptr; }
  };

  struct traverser_t {
    concurrent_map &map;
    EpochGuard guard;

    bool is_snapshot_stale;
    node_snapshot_t root_snapshot;

    traverser_t(concurrent_map &map)
        : map(map), guard(std::addressof(map)), is_snapshot_stale(false),
          root_snapshot(load_aq(map.root)) {}

    template <UpdateOp UOp>
    std::optional<value_type> update_leaf(node_snapshot_t &node,
                                          value_type value) {
      auto leaf = static_cast<leaf_t *>(node.node);

      if constexpr (UOp != UpdateOp::UOP_Insert) {
        if (auto lock = node.lock()) {
          return std::exchange(leaf->value, value);
        } else {
          is_snapshot_stale = true;
        }
      }

      return leaf->value;
    }

    bool replace_root(node_t *node) {
      if (auto lock = root_snapshot.lock_root(map)) {
        if (root_snapshot.node) {
          auto newroot = new node4_t(0, 0);

          newroot->add(node);
          newroot->add(root_snapshot.node);
          map.root = newroot;
        } else {
          map.root = node;
        }

        return true;
      }

      return false;
    }

    bool update_parent(node_t *node, node_snapshot_t &parent) {
      if (parent) {
        if (auto lock = parent.lock()) {
          parent->update(node);
          return true;
        }
      } else {
        if (auto lock = root_snapshot.lock_root(map)) {
          map.root = node;
          return true;
        }
      }

      return false;
    }

    void shrink_node(node_snapshot_t &node, node_snapshot_t &parent) {
      if (auto nodelock = node.lock()) {
        if (node->size()) {
          node_t *replacement = node->shrink();

          ART_DEBUG_ASSERT(replacement != nullptr);

          nodelock.unlock();

          LockType child_lock{replacement->m};

          if (!replacement->is_deleted()) {
            if (auto nodelock = node.lock()) {
              if (update_parent(replacement, parent)) {
                node->mark_as_deleted();
                nodelock.unlock();
                map.m_gc.retire_in_new_epoch(node_t::free, node.node);
                return;
              }
            }
          }

          if (node->node_type != node_type_t::NODE4) {
            delete replacement;
          }
        } else {
          if (parent) {
            if (auto lock = parent.lock()) {
              parent->remove(node->key);
              node->mark_as_deleted();
              nodelock.unlock();
              map.m_gc.retire_in_new_epoch(node_t::free, node.node);
            }
          } else {
            if (auto lock = root_snapshot.lock_root(map)) {
              map.root = nullptr;
              node->mark_as_deleted();
              nodelock.unlock();
              map.m_gc.retire_in_new_epoch(node_t::free, node.node);
            }
          }
        }
      }
    }

    std::optional<value_type> remove_leaf(leaf_t *leaf, node_snapshot_t &node,
                                          node_snapshot_t &parent) {
      value_type oldval = leaf->value;

      is_snapshot_stale = true;

      if (auto lock = node.lock()) {
        if (parent) {
          if (auto lock = parent.lock()) {
            parent->remove(leaf->key);
            leaf->mark_as_deleted();
            is_snapshot_stale = false;
          }
        } else {
          if (auto lock = root_snapshot.lock_root(map)) {
            map.root = nullptr;
            is_snapshot_stale = false;
          }
        }
      }

      if (!is_snapshot_stale) {
        map.m_gc.retire_in_new_epoch(node_t::free, leaf);
      }

      return oldval;
    }

    node_t *expand_node(node_t *node, node_snapshot_t &parent,
                        LockType &oldlock) {
      node_t *old = node;

      node = node->expand();

      LockType lock{node->m};

      if (update_parent(node, parent)) {
        old->mark_as_deleted();
        oldlock = std::move(lock);
        map.m_gc.retire_in_new_epoch(node_t::free, old);
      } else {
        lock.unlock();
        delete node;
        node = nullptr;
      }

      return node;
    }

    bool add_to_parent(node_t *node, node_snapshot_t &parent,
                       node_snapshot_t &grand_parent) {
      if (parent) {
        if (auto lock = parent.lock()) {
          if (!parent->add(node)) {
            node_t *newparent = expand_node(parent.node, grand_parent, lock);

            if (newparent) {
              newparent->add(node);
              return true;
            } else {
              return false;
            }
          }

          return true;
        }
      } else {
        return replace_root(node);
      }

      return false;
    }

    node4_t *decompress_node(node_t *node, int lcpl) {
      key_type prefix = node_t::extract_common_prefix(node->key, lcpl);
      auto decomped_node = new node4_t(prefix, lcpl);

      decomped_node->add(node);

      return decomped_node;
    }

    template <UpdateOp UOp>
    bool insert_leaf(key_type key, value_type value, int depth, int lcpl,
                     node_snapshot_t &node, node_snapshot_t &parent,
                     node_snapshot_t &grand_parent) {
      if constexpr (UOp != UpdateOp::UOP_Update) {
        int keylen = node->level - depth;
        int common_prefix_len = std::min(lcpl - depth, keylen);
        auto leaf = new leaf_t(key, value);

        if (common_prefix_len && (node->is_leaf() || keylen)) {
          if (auto lock = node.lock()) {
            node4_t *decomped_node = decompress_node(node.node, lcpl);

            decomped_node->add(leaf);

            if (update_parent(decomped_node, parent)) {
              return true;
            } else {
              delete decomped_node;
            }
          }
        } else {
          if (add_to_parent(leaf, parent, grand_parent)) {
            return true;
          }
        }

        delete leaf;
        return false;
      }

      return true;
    }

    template <UpdateOp UOp>
    std::optional<value_type> insert(key_type key, value_type value) {
      int depth = 0;
      node_snapshot_t node, parent, grand_parent;

      node = root_snapshot;

      while (node) {
        int lcpl = node->longest_common_prefix_length(key);
        int keylen = node->level - depth;
        int common_prefix_len = std::min(lcpl - depth, keylen);

        if (node->is_leaf()) {
          if (node->key == key) {
            return update_leaf<UOp>(node, value);
          }

          if (!insert_leaf<UOp>(key, value, depth, lcpl, node, parent,
                                grand_parent)) {
            is_snapshot_stale = true;
          }

          return {};
        }

        if (common_prefix_len == keylen) {
          depth += common_prefix_len;

          ART_DEBUG_ASSERT(depth < MAX_DEPTH);

          grand_parent = parent;
          parent = node;
          node.load_snapshot(node->find(key));
        } else {
          if (!insert_leaf<UOp>(key, value, depth, lcpl, node, parent,
                                grand_parent)) {
            is_snapshot_stale = true;
          }

          return {};
        }
      }

      if constexpr (UOp != UpdateOp::UOP_Update) {
        if (!add_to_parent(new leaf_t(key, value), parent, grand_parent)) {
          is_snapshot_stale = true;
        }
      }

      return {};
    }

    std::optional<value_type> erase(key_type key) {
      int depth = 0;
      node_snapshot_t node, parent, grand_parent;

      node = root_snapshot;

      while (node) {
        if (node->is_leaf()) {
          auto leaf = static_cast<leaf_t *>(node.node);

          if (leaf->key == key) {
            if (auto old = remove_leaf(leaf, node, parent)) {
              if (parent && parent->is_underfull()) {
                shrink_node(parent, grand_parent);
              }

              return old;
            }
          }

          break;
        }

        int keylen = node->level - depth;

        if (keylen) {
          int lcpl = node->longest_common_prefix_length(key);
          int common_prefix_len = std::min(lcpl - depth, keylen);

          if (common_prefix_len != keylen) {
            break;
          }

          depth += keylen;
        }

        ART_DEBUG_ASSERT(depth < MAX_DEPTH);

        grand_parent = parent;
        parent = node;
        node.load_snapshot(node->find(key));
      }

      return {};
    }
  };

  template <UpdateOp UOp>
  std::optional<value_type> insert(key_type key, value_type value) {
    while (true) {
      traverser_t traverser{*this};
      auto old = traverser.template insert<UOp>(key, value);

      if (!traverser.is_snapshot_stale) {
        return old;
      }
    }
  }

  std::optional<value_type> erase(key_type key) {
    while (true) {
      traverser_t traverser{*this};
      auto value = traverser.erase(key);

      if (!traverser.is_snapshot_stale) {
        return value;
      }
    }
  }

  static constexpr bool INSERT = false;
  static constexpr bool UPSERT = true;

  struct alignas(128) values_count_t {
    std::atomic<size_t> num_inserts;
    std::atomic<size_t> num_deletes;
  };

  std::unique_ptr<utils::Mutex> root_mtx;
  atomic_node_t root;
  std::unique_ptr<values_count_t[]> count;

  mutable indexes::utils::EpochManager<uint64_t, node_t> m_gc;

public:
  concurrent_map()
      : root_mtx(std::make_unique<utils::Mutex>()), root(nullptr),
        count(std::make_unique<values_count_t[]>(
            utils::ThreadLocal::MAX_THREADS)) {}

  concurrent_map(const concurrent_map &) = delete;

  concurrent_map(concurrent_map &&other)
      : root(other.root.exchange(nullptr)),
        count(std::exchange(other.count, nullptr)) {}

  std::optional<value_type> Search(key_type key) const {
    EpochGuard eg{this};

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

      int keylen = node->level - depth;

      if (keylen) {
        int lcpl = node->longest_common_prefix_length(key);
        int common_prefix_len = std::min(lcpl - depth, keylen);

        if (common_prefix_len != keylen) {
          break;
        }

        depth += keylen;
      }

      ART_DEBUG_ASSERT(depth < MAX_DEPTH);

      node = node->find(key);
    }

    return {};
  }

  bool Insert(key_type key, value_type value) {
    auto &&old = insert<UpdateOp::UOP_Insert>(key, value);

    if (!old) {
      count[utils::ThreadLocal::ThreadID()].num_inserts++;
    }

    return !old;
  }

  std::optional<value_type> Upsert(key_type key, value_type value) {
    auto &&old = insert<UpdateOp::UOP_Upsert>(key, value);

    if (!old) {
      count[utils::ThreadLocal::ThreadID()].num_inserts++;
    }

    return old;
  }

  std::optional<value_type> Update(key_type key, value_type value) {
    return insert<UpdateOp::UOP_Update>(key, value);
  }

  std::optional<value_type> Delete(key_type key) {
    auto &&old = erase(key);

    if (old) {
      count[utils::ThreadLocal::ThreadID()].num_deletes++;
    }

    return old;
  }

  std::size_t size() const {
    std::size_t size = 0;

    for (int i = 0, n = utils::ThreadLocal::MAX_THREADS; i < n; i++) {
      size += count[i].num_inserts - count[i].num_deletes;
    }

    return size;
  }

  ~concurrent_map() {
    std::deque<node_t *> children;

    if (root) {
      children.push_back(root);
    }

    while (children.size()) {
      node_t *node = children[0];

      children.pop_front();

      node->get_children(children);
      node_t::free(node);
    }

    root = nullptr;
  }

  void reserve(size_t) {
    // No-op
  }

  ART_DUMP_METHODS
};
} // namespace indexes::art
