// include/indexes/btree/concurrent_map.h
// B+Tree implementation

#pragma once

#include "common.h"
#include "indexes/utils/EpochManager.h"
#include "sync_prim/Mutex.h"

#include <atomic>
#include <bitset>
#include <boost/container/small_vector.hpp>
#include <chrono>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#define ENABLE_IF(cond)                                                        \
  typename Dummy = void, typename = std::enable_if_t<cond, Dummy>
#define ENABLE_IF_DYNAMIC_KEY ENABLE_IF(is_dynamic_key)
#define ENABLE_IF_STATIC_KEY ENABLE_IF(!is_dynamic_key)
#define DYNAMIC_KEY_ONLY template <ENABLE_IF_DYNAMIC_KEY>
#define STATIC_KEY_ONLY template <ENABLE_IF_STATIC_KEY>

namespace indexes::btree {
struct dynamic_key_base {};
enum range_kind { INCLUSIVE, EXCLUSIVE };

namespace detail {
template <typename T>
static inline void store_relaxed(std::atomic<T> &atomicvar, T newval) {
  atomicvar.store(newval, std::memory_order_relaxed);
}
template <typename T>
static inline void store_release(std::atomic<T> &atomicvar, T newval) {
  atomicvar.store(newval, std::memory_order_release);
}
template <typename T>
static inline T load_relaxed(const std::atomic<T> &atomicvar) {
  return atomicvar.load(std::memory_order_relaxed);
}
template <typename T>
static inline T load_acquire(const std::atomic<T> &atomicvar) {
  return atomicvar.load(std::memory_order_acquire);
}

enum iter_direction { REVERSE, FORWARD };

template <typename Key, typename Value, typename Traits, typename Stats>
struct concurrent_map_base {
public:
  using key_type = Key;
  using mapped_type = Value;
  using value_type = std::pair<const key_type, mapped_type>;
  using size_type = std::size_t;
  using difference_type = std::ptrdiff_t;
  using reference = value_type &;
  using const_reference = const value_type &;

  struct dynamic_cmp {
    virtual bool less(const key_type &k1, const key_type &k2) const
        noexcept = 0;
    virtual bool equal(const key_type &k1, const key_type &k2) const
        noexcept = 0;
  };

  concurrent_map_base() = default;
  concurrent_map_base(const concurrent_map_base &) = delete;
  concurrent_map_base(concurrent_map_base &&moved)
      : m_root_mutex(std::move(moved.m_root_mutex)),
        m_root_state(detail::load_relaxed(moved.m_root_state)),
        m_root(detail::load_relaxed(moved.m_root)),
        m_height(detail::load_relaxed(moved.m_height)),
        m_stats(std::move(moved.m_stats)) {
    moved.m_root_state.store({});
    moved.m_root.store(nullptr);
    moved.m_height.store(0);
  }

  static constexpr auto is_dynamic_key =
      std::is_base_of_v<dynamic_key_base, key_type>;

protected:
  enum class NodeType : int8_t { LEAF, INNER };

  class nodestate_t {
    using bitset = std::bitset<64>;

    static constexpr int IS_LOCKED_BIT = 62;
    static constexpr int IS_DELETED_BIT = 63;

    bitset bits = {};

  public:
    inline bool operator==(const nodestate_t &other) const {
      return bits == other.bits;
    }

    inline bool operator!=(const nodestate_t &other) const {
      return bits != other.bits;
    }

    inline uint64_t version() const {
      auto t = bits;
      return t.reset(IS_LOCKED_BIT).reset(IS_DELETED_BIT).to_ullong();
    }

    inline bool is_locked() const { return bits.test(IS_LOCKED_BIT); }

    inline bool is_deleted() const { return bits.test(IS_DELETED_BIT); }

    inline nodestate_t increment_version() {
      auto version_only = version() + 1;
      bitset zero_version = {};

      zero_version.set(IS_LOCKED_BIT).set(IS_DELETED_BIT);

      bits &= zero_version;
      bits |= version_only;
      return *this;
    }

    inline nodestate_t set_locked() {
      bits.set(IS_LOCKED_BIT);
      return *this;
    }

    inline nodestate_t set_deleted() {
      bits.set(IS_DELETED_BIT);
      return *this;
    }

    inline nodestate_t reset_locked() {
      bits.reset(IS_LOCKED_BIT);
      return *this;
    }

    inline nodestate_t reset_deleted() {
      bits.reset(IS_DELETED_BIT);
      return *this;
    }
  };

  struct node_t;

  struct NodeSplitInfo {
    node_t *left;
    node_t *right;
    const key_type *split_key;
  };

  struct node_t {
    std::atomic<nodestate_t> state = {};
    std::atomic<int> num_values = 0;

    // max_slot_offset is required to guard readers from concurrent updaters.
    // concurrent updaters might overwrite slots which can be read by readers,
    // with key/value data.

    std::atomic<int> logical_pagesize = 0;
    std::atomic<int> next_slot_offset = 0;
    std::atomic<int> max_slot_offset = 0;
    int last_value_offset = Traits::NODE_SIZE;

    std::atomic<int8_t> num_dead_values = 0;
    const NodeType node_type;
    const int height;

    const std::optional<key_type> lowkey;
    const std::optional<key_type> highkey;

    sync_prim::mutex::Mutex mutex;

    inline node_t(NodeType ntype, int initialsize, int a_height,
                  const std::optional<key_type> &a_lowkey,
                  const std::optional<key_type> &a_highkey)
        : next_slot_offset(initialsize), node_type(ntype), height(a_height),
          lowkey(a_lowkey), highkey(a_highkey) {}

    inline bool isLeaf() const { return node_type == NodeType::LEAF; }

    inline bool isInner() const { return node_type == NodeType::INNER; }

    inline nodestate_t getState() const { return detail::load_acquire(state); }

    inline void setState(nodestate_t new_state) {
      detail::store_release(state, new_state);
    }

    // We only need to find out if we have more than two deleted values, to trim
    // Called this `this` mutex held
    inline void incrementNumDeadValues() {
      auto num_dead_values = detail::load_relaxed(this->num_dead_values);

      store_relaxed<int8_t>(this->num_dead_values, num_dead_values > 1
                                                       ? num_dead_values
                                                       : num_dead_values + 1);
    }

    // Called this `this` mutex held
    inline bool isUnderfull() const {
      BTREE_DEBUG_ASSERT(detail::load_relaxed(logical_pagesize) <=
                         Traits::NODE_SIZE);

      return (detail::load_relaxed(logical_pagesize) * 100) /
                 Traits::NODE_SIZE <
             Traits::NODE_MERGE_THRESHOLD;
    }

    inline char *opaque() const {
      return reinterpret_cast<char *>(reinterpret_cast<intptr_t>(this));
    }

    inline std::atomic<int> *get_slots() const {
      return reinterpret_cast<std::atomic<int> *>(opaque() + sizeof(node_t));
    }

    inline bool canTrim() const {
      return detail::load_relaxed(num_dead_values) > 1;
    }

    inline bool canSplit() const {
      return detail::load_relaxed(num_values) > 2;
    }

    static inline void free(node_t *node) {
      if (node->isLeaf())
        delete ASLEAF(node);
      else
        delete ASINNER(node);
    }

    inline const key_type &get_first_key() const {
      if (isInner())
        return static_cast<const inner_node_t *>(this)->get_first_key();
      else
        return static_cast<const leaf_node_t *>(this)->get_first_key();
    }
  };

  enum class InsertStatus {
    // In Windows OVERFLOW is defined as macro internally, so use..
    OVFLOW,
    DUPLICATE,
    INSERTED
  };

  template <typename ValueType> struct align_helper {
    static constexpr auto align = std::max(
        alignof(std::pair<key_type, ValueType>), alignof(std::max_align_t));
  };
  template <typename ValueType, NodeType NType>
  struct alignas(align_helper<ValueType>::align) inherited_node_t : node_t {
    using value_t = ValueType;
    using key_value_t = std::pair<key_type, value_t>;

    template <typename KeyType> friend class SearchOps;

    static constexpr enum NodeType NODETYPE = NType;

    static constexpr bool IsLeaf() { return NType == NodeType::LEAF; }

    static constexpr bool IsInner() { return NType == NodeType::INNER; }

    inline inherited_node_t(const std::optional<key_type> &lowkey,
                            const std::optional<key_type> &highkey, int height)
        : node_t(NType, sizeof(inherited_node_t), height, lowkey, highkey) {}

    inline ~inherited_node_t() {
      auto num_values = detail::load_relaxed(this->num_values);
      for (int slot = IsInner() ? 1 : 0; slot < num_values; slot++) {
        get_key_value(slot)->~key_value_t();
      }
    }

    static inherited_node_t *alloc(const std::optional<key_type> &lowkey,
                                   const std::optional<key_type> &highkey,
                                   int height) {
      return new (new char[Traits::NODE_SIZE])
          inherited_node_t(lowkey, highkey, height);
    }

    static void free(inherited_node_t *node) { delete node; }

    // Must be called with both this's and other's mutex held
    inline bool haveEnoughSpace() const {
      auto next_slot_offset = detail::load_relaxed(this->next_slot_offset);
      auto max_slot_offset = detail::load_relaxed(this->max_slot_offset);

      return ((next_slot_offset + sizeof(int)) <=
              (this->last_value_offset - sizeof(key_value_t))) &&
             (max_slot_offset <=
              static_cast<int>(this->last_value_offset - sizeof(key_value_t)));
    }

    // Must be called with both this's and other's mutex held
    inline bool canMerge(const node_t *other) const {
      auto logical_pagesize = detail::load_relaxed(this->logical_pagesize);
      auto other_logical_pagesize =
          detail::load_relaxed(other->logical_pagesize);

      return (logical_pagesize + other_logical_pagesize +
              (IsInner() ? sizeof(key_value_t) : 0)) +
                 sizeof(inherited_node_t) <=
             Traits::NODE_SIZE;
    }

    inline key_value_t *get_key_value_for_offset(int offset) const {
      return reinterpret_cast<key_value_t *>(this->opaque() + offset);
    }

    inline key_value_t *get_key_value(int slot) const {
      auto slots = this->get_slots();
      return get_key_value_for_offset(detail::load_acquire(slots[slot]));
    }

    inline const key_type &get_key(int slot) const {
      if constexpr (IsInner())
        BTREE_DEBUG_ASSERT(slot != 0);

      return get_key_value(slot)->first;
    }

    INNER_ONLY
    inline std::atomic<value_t> *get_child_ptr(int slot) const {
      return slot == 0 ? reinterpret_cast<std::atomic<value_t> *>(
                             &get_key_value(0)->first)
                       : reinterpret_cast<std::atomic<value_t> *>(
                             &get_key_value(slot)->second);
    }

    INNER_ONLY
    inline value_t get_child(int slot) const {
      return detail::load_acquire(*get_child_ptr(slot));
    }

    inline const key_type &get_first_key() const {
      if constexpr (IsInner())
        return get_key_value(1)->first;
      else
        return get_key_value(0)->first;
    }

    INNER_ONLY
    inline value_t get_first_child() const {
      return detail::load_acquire(*get_child_ptr(0));
    }

    INNER_ONLY
    inline value_t get_last_child() const {
      auto slot = detail::load_acquire(this->num_values) - 1;
      return get_child(slot);
    }

    // Node update helpers
    // Must be called with this's mutex held

    template <typename Updater> inline void atomic_node_update(Updater update) {
      this->setState(this->getState().set_locked());
      update();
      this->setState(this->getState().reset_locked().increment_version());
    }

    // Must be called with this's mutex held
    inline void update_meta_after_insert() {
      int next_slot_offset =
          detail::load_relaxed(this->next_slot_offset) + sizeof(int);
      int logical_pagesize = detail::load_relaxed(this->logical_pagesize) +
                             sizeof(key_value_t) + sizeof(int);
      int max_slot_offset = std::max(
          detail::load_relaxed(this->max_slot_offset), next_slot_offset);

      this->last_value_offset -= sizeof(key_value_t);
      detail::store_relaxed(this->next_slot_offset, next_slot_offset);
      detail::store_relaxed(this->logical_pagesize, logical_pagesize);
      detail::store_relaxed(this->max_slot_offset, max_slot_offset);

      BTREE_DEBUG_ASSERT(next_slot_offset <= this->last_value_offset);
    }

    // Must be called with this's mutex held
    static void copy_backward(std::atomic<int> *slots, int start_pos,
                              int end_pos, int out_end_pos) {
      BTREE_DEBUG_ASSERT(out_end_pos >= end_pos);

      while (start_pos < end_pos) {
        detail::store_release(slots[--out_end_pos],
                              detail::load_relaxed(slots[--end_pos]));
      }
    }

    // Must be called with this's mutex held
    static void copy(std::atomic<int> *slots, int start_pos, int end_pos,
                     int out_pos) {
      BTREE_DEBUG_ASSERT(out_pos < start_pos);

      while (start_pos < end_pos) {
        detail::store_release(slots[out_pos++],
                              detail::load_relaxed(slots[start_pos++]));
      }
    }

    // Must not be called on a reachable node
    INNER_ONLY
    inline void insert_neg_infinity(const value_t &val) {
      auto num_values = detail::load_relaxed(this->num_values);
      BTREE_DEBUG_ASSERT(this->isInner() && num_values == 0);

      auto slots = this->get_slots();
      int current_value_offset = this->last_value_offset - sizeof(value_t);

      new (this->opaque() + current_value_offset) value_t{val};
      detail::store_relaxed(slots[0], current_value_offset);

      detail::store_relaxed(this->num_values, num_values + 1);
      update_meta_after_insert();
    }

    // Must not be called on a reachable node
    inline void append(const key_type &key, const value_t &val) {
      auto slots = this->get_slots();
      int current_value_offset = this->last_value_offset - sizeof(key_value_t);
      auto num_values = detail::load_relaxed(this->num_values);
      auto pos = num_values;

      new (this->opaque() + current_value_offset) key_value_t{key, val};
      detail::store_relaxed(slots[pos], current_value_offset);

      detail::store_relaxed(this->num_values, num_values + 1);
      update_meta_after_insert();
    }

    // Must be called with this's mutex held and node state set as locked
    inline void insert_into_slot(int pos, int value_offset) {
      auto num_values = detail::load_relaxed(this->num_values);
      auto slots = this->get_slots();

      copy_backward(slots, pos, num_values, num_values + 1);
      detail::store_release(slots[pos], value_offset);
      detail::store_release(this->num_values, num_values + 1);
    }

    // Must be called with this's mutex held
    inline InsertStatus insert_into_pos(const key_type &key, const value_t &val,
                                        int pos) {
      if (this->haveEnoughSpace()) {
        int current_value_offset =
            this->last_value_offset - sizeof(key_value_t);

        new (this->opaque() + current_value_offset) key_value_t{key, val};

        atomic_node_update(
            [&]() { insert_into_slot(pos, current_value_offset); });
        update_meta_after_insert();

        return InsertStatus::INSERTED;
      }

      return InsertStatus::OVFLOW;
    }

    // Must not be called on a reachable node
    inline void copy_from(const inherited_node_t *src, int start_pos,
                          int end_pos) {
      for (int slot = start_pos; slot < end_pos; slot++) {
        const key_value_t *val = src->get_key_value(slot);

        this->append(val->first, val->second);
      }
    }

    // SMO helpers
    // Must be called with this's mutex held

    inherited_node_t *trim() const {
      inherited_node_t *new_node =
          alloc(this->lowkey, this->highkey, this->height);

      if constexpr (IsLeaf()) {
        new_node->copy_from(this, 0, detail::load_relaxed(this->num_values));
      } else {
        new_node->insert_neg_infinity(get_first_child());
        new_node->copy_from(this, 1, detail::load_relaxed(this->num_values));
      }

      return new_node;
    }

    // Must be called with this's mutex held
    NodeSplitInfo split() const {
      BTREE_DEBUG_ASSERT(this->canSplit());

      auto num_values = detail::load_relaxed(this->num_values);
      int split_pos = IsInner() ? num_values / 2 : (num_values + 1) / 2;
      const key_type &split_key = get_key(split_pos);
      inherited_node_t *left = alloc(this->lowkey, split_key, this->height);
      inherited_node_t *right = alloc(split_key, this->highkey, this->height);

      if constexpr (IsInner()) {
        left->insert_neg_infinity(get_first_child());
        left->copy_from(this, 1, split_pos);

        right->insert_neg_infinity(get_child(split_pos));
        right->copy_from(this, split_pos + 1, num_values);
      } else {
        left->copy_from(this, 0, split_pos);
        right->copy_from(this, split_pos, num_values);
      }

      BTREE_DEBUG_ASSERT(left->lowkey || left->highkey);
      BTREE_DEBUG_ASSERT(right->lowkey || right->highkey);

      return {left, right, &split_key};
    }

    // Must be called with this's and `other` mutex held
    inherited_node_t *merge(const inherited_node_t *other,
                            const key_type &merge_key) const {
      inherited_node_t *mergednode = nullptr;

      if (this->canMerge(other)) {
        mergednode = alloc(this->lowkey, other->highkey, this->height);

        if constexpr (IsInner()) {
          mergednode->insert_neg_infinity(get_first_child());
          mergednode->copy_from(this, 1,
                                detail::load_relaxed(this->num_values));

          mergednode->append(merge_key, other->get_first_child());
          mergednode->copy_from(other, 1,
                                detail::load_relaxed(other->num_values));
        } else {
          (void)merge_key;

          mergednode->copy_from(this, 0,
                                detail::load_relaxed(this->num_values));
          mergednode->copy_from(other, 0,
                                detail::load_relaxed(other->num_values));
        }

        BTREE_DEBUG_ASSERT(detail::load_relaxed(mergednode->num_values) ==
                           detail::load_relaxed(this->num_values) +
                               detail::load_relaxed(other->num_values));
      }

      return mergednode;
    }

    LEAF_ONLY
    void get_all_slots(std::vector<int> &slot_offsets) const {
      get_all_slots(slot_offsets, this->get_slots(),
                    detail::load_acquire(this->num_values));
    }
    LEAF_ONLY
    static inline void get_all_slots(std::vector<int> &slot_offsets,
                                     const std::atomic<int> *slots,
                                     int num_values) {
      slot_offsets.clear();
      for (int i = 0; i < num_values; i++) {
        slot_offsets.emplace_back(detail::load_acquire(slots[i]));
      }
    }

    // Must be called only from destructor or debugging routines
    // Locking doesn't matter
    template <typename Cont, ENABLE_IF(IsInner())>
    void get_children(Cont &nodes) const {
      nodes.emplace_back(get_first_child());

      auto num_values = detail::load_relaxed(this->num_values);
      for (int slot = 1; slot < num_values; slot++) {
        nodes.emplace_back(get_child(slot));
      }
    }

    NODE_DUMP_METHODS
  };

  using leaf_node_t = inherited_node_t<mapped_type, NodeType::LEAF>;
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

  static constexpr int MAXHEIGHT = 32;

  struct NodeSnapshot {
    node_t *node;
    nodestate_t state;
  };

  // Store upto `SMALL_HEIGHT` values in stack.
  static constexpr auto SMALL_HEIGHT = 10;
  using NodeSnapshotVector =
      boost::container::small_vector<NodeSnapshot, SMALL_HEIGHT>;

  struct DummyType {};

  static constexpr bool DO_UPSERT = true;
  static constexpr bool DO_INSERT = false;
  static constexpr bool OPTIMISTIC_LOCKING = true;
  static constexpr bool PESSIMISTIC_LOCKING = false;
  static constexpr bool FILL_SNAPSHOT_VECTOR = true;
  static constexpr bool NO_FILL_SNAPSHOT_VECTOR = false;
  static constexpr int OPTIMISTIC_TRY_COUNT = 3;

  enum class OpResult { SUCCESS, FAILURE, STALE_SNAPSHOT };

  inline void try_lock_pessimistic(node_t *node, nodestate_t &state) const {
    BTREE_UPDATE_STAT(pessimistic_read, ++);

    if (node) {
      node->mutex.lock();
      state = node->getState();

      if (state.is_deleted())
        node->mutex.unlock();
    } else {
      this->m_root_mutex->lock();
      state = detail::load_acquire(this->m_root_state);

      if (state.is_deleted())
        this->m_root_mutex->unlock();
    }
  }

  inline bool try_lock_optimistic(node_t *node, nodestate_t &state) const {
    int num_tries = 0;

    do {
      state =
          node ? node->getState() : detail::load_acquire(this->m_root_state);

      if (!state.is_locked())
        return true;

      std::this_thread::sleep_for(std::chrono::nanoseconds(300));
      num_tries++;
    } while (num_tries < OPTIMISTIC_TRY_COUNT);

    return false;
  }

  template <bool UseOptimisticLocking>
  inline bool lock_node_or_restart(node_t *node, nodestate_t &state) const {
    if constexpr (UseOptimisticLocking) {
      if (!try_lock_optimistic(node, state)) {
        BTREE_UPDATE_STAT(optimistic_fail, ++);
        try_lock_pessimistic(node, state);

        if (!state.is_deleted())
          node->mutex.unlock();
      }
    } else {
      try_lock_pessimistic(node, state);
    }

    return state.is_deleted();
  }

  template <bool UseOptimisticLocking>
  inline bool unlock_node_or_restart(node_t *node, nodestate_t &state) const {
    if constexpr (UseOptimisticLocking) {
      if (node)
        return state != node->getState();
      else
        return state != detail::load_acquire(this->m_root_state);
    } else {
      if (node)
        node->mutex.unlock();
      else
        this->m_root_mutex->unlock();
    }

    return false;
  }

  template <
      bool UseOptimisticLocking, bool FillSnapshotVector, typename GetChild,
      typename SnapshotVectorType =
          std::conditional_t<FillSnapshotVector, NodeSnapshotVector, DummyType>,
      typename NodeSnaphotType =
          std::conditional_t<FillSnapshotVector, DummyType, NodeSnapshot>>
  OpResult traverse(const GetChild &get_children, SnapshotVectorType &snapshots,
                    NodeSnaphotType &leaf_snapshot) const {
    node_t *parent = nullptr;
    node_t *current = nullptr;
    nodestate_t parent_state = {};
    nodestate_t current_state = {};

    if constexpr (FillSnapshotVector)
      snapshots.clear();

    if (lock_node_or_restart<UseOptimisticLocking>(nullptr, parent_state))
      return OpResult::STALE_SNAPSHOT;

    if constexpr (FillSnapshotVector)
      snapshots.push_back({nullptr, parent_state});

    for (current = detail::load_acquire(this->m_root);
         current && current->isInner();) {
      if (lock_node_or_restart<UseOptimisticLocking>(current, current_state) ||
          unlock_node_or_restart<UseOptimisticLocking>(parent, parent_state)) {
        return OpResult::STALE_SNAPSHOT;
      }

      if constexpr (FillSnapshotVector)
        snapshots.push_back({current, current_state});

      parent = current;
      parent_state = current_state;
      current = get_children(ASINNER(current));

      if (is_snapshot_stale({parent, parent_state}))
        return OpResult::STALE_SNAPSHOT;
    }

    if ((current &&
         lock_node_or_restart<UseOptimisticLocking>(current, current_state)) ||
        unlock_node_or_restart<UseOptimisticLocking>(parent, parent_state)) {
      return OpResult::STALE_SNAPSHOT;
    }

    if (current) {
      if constexpr (FillSnapshotVector)
        snapshots.push_back({current, current_state});
      else
        leaf_snapshot = {current, current_state};
    }

    return OpResult::SUCCESS;
  }

  template <bool FillSnapshotVector, typename GetChild, int MaxRestarts = 2,
            typename SnapshotVectorType = std::conditional_t<
                FillSnapshotVector, NodeSnapshotVector, DummyType>,
            typename NodeSnaphotType =
                std::conditional_t<FillSnapshotVector, DummyType, NodeSnapshot>>
  bool traverse_to_leaf(GetChild &&get_child, SnapshotVectorType &&snapshots,
                        NodeSnaphotType &&leaf_snapshot) const {
    int restart_count = 0;
    auto opt_trav_res = OpResult::FAILURE;

    do {
      opt_trav_res = traverse<OPTIMISTIC_LOCKING, FillSnapshotVector>(
          get_child, snapshots, leaf_snapshot);
      restart_count++;
    } while (opt_trav_res != OpResult::SUCCESS && restart_count < MaxRestarts);

    if (opt_trav_res != OpResult::SUCCESS) {
      auto res = traverse<PESSIMISTIC_LOCKING, FillSnapshotVector>(
          get_child, snapshots, leaf_snapshot);

      BTREE_DEBUG_ASSERT(res == OpResult::SUCCESS);
      BTREE_DEBUG_ONLY(res);
    }

    return opt_trav_res != OpResult::SUCCESS;
  }

  // Root mutex must be held
  inline void store_root(node_t *new_root) {
    detail::store_release(
        this->m_root_state,
        detail::load_relaxed(this->m_root_state).set_locked());
    detail::store_release(this->m_root, new_root);
    detail::store_release(this->m_root_state,
                          detail::load_relaxed(this->m_root_state)
                              .reset_locked()
                              .increment_version());
    detail::store_release(this->m_height,
                          detail::load_relaxed(this->m_height) + 1);
  }

  // Root mutex must be held
  inline void create_root(NodeSplitInfo splitinfo) {
    auto new_root =
        inner_node_t::alloc(splitinfo.left->lowkey, splitinfo.right->highkey,
                            detail::load_relaxed(this->m_height) + 1);
    new_root->insert_neg_infinity(splitinfo.left);
    new_root->append(*splitinfo.split_key, splitinfo.right);
    store_root(new_root);
  }

  inline bool update_root(nodestate_t rootstate, node_t *new_root) {
    std::lock_guard lock{*this->m_root_mutex};

    if (detail::load_acquire(this->m_root_state) != rootstate)
      return false;

    store_root(new_root);

    return true;
  }

  inline void ensure_root() {
    while (detail::load_acquire(this->m_root) == nullptr) {
      auto new_root = leaf_node_t::alloc(std::nullopt, std::nullopt,
                                         detail::load_acquire(this->m_height));

      if (!update_root({}, new_root))
        delete new_root;
    }
  }

  inline bool is_snapshot_stale(const NodeSnapshot &snapshot) const {
    return snapshot.node
               ? snapshot.node->getState() != snapshot.state
               : detail::load_acquire(this->m_root_state) != snapshot.state;
  }

  static inline DummyType dummy_snap_vec() noexcept { return {}; }

  std::unique_ptr<sync_prim::mutex::Mutex> m_root_mutex =
      std::make_unique<sync_prim::mutex::Mutex>();
  std::atomic<nodestate_t> m_root_state = {};
  std::atomic<node_t *> m_root = nullptr;

  std::atomic<int> m_height = 0;
  std::unique_ptr<Stats> m_stats = std::make_unique<Stats>();

  mutable indexes::utils::EpochManager<uint64_t, node_t> m_gc;
};

template <typename Key, typename Value, typename Traits, typename Stats>
struct concurrent_map_access
    : public detail::concurrent_map_base<Key, Value, Traits, Stats> {
protected:
  using base = detail::concurrent_map_base<Key, Value, Traits, Stats>;
  using node_t = typename base::node_t;
  using nodestate_t = typename base::nodestate_t;
  using NodeSplitInfo = typename base::NodeSplitInfo;
  using dynamic_cmp = typename base::dynamic_cmp;
  using key_type = typename base::key_type;
  using inner_node_t = typename base::inner_node_t;
  using leaf_node_t = typename base::leaf_node_t;
  using InsertStatus = typename base::InsertStatus;
  using mapped_type = typename base::mapped_type;
  using NodeSnapshotVector = typename base::NodeSnapshotVector;
  using NodeSnapshot = typename base::NodeSnapshot;
  using OpResult = typename base::OpResult;

  struct EpochGuard {
    const concurrent_map_access *map = nullptr;

    EpochGuard() = default;
    EpochGuard(const concurrent_map_access *a_map) : map(a_map) {
      map->m_gc.enter_epoch();
    }
    EpochGuard(const EpochGuard &o) : map(o.map) {
      if (map)
        map->m_gc.enter_epoch();
    }
    EpochGuard(EpochGuard &&o) : map(std::exchange(o.map, nullptr)) {}
    ~EpochGuard() {
      if (map)
        map->m_gc.exit_epoch();
    }

    void release() {
      if (map)
        map->m_gc.exit_epoch();
      map = nullptr;
    }
    void refresh() {
      if (map) {
        map->m_gc.exit_epoch();
        map->m_gc.enter_epoch();
      }
    }
  };

  static constexpr auto is_dynamic_key = base::is_dynamic_key;

  template <typename KeyT1, typename KeyT2> struct static_cmp {
    inline bool less(const KeyT1 &k1, const KeyT2 &k2) const noexcept {
      return k1 < k2;
    }
    template <ENABLE_IF((!std::is_same_v<KeyT1, KeyT2>))>
    inline bool less(const KeyT2 &k1, const KeyT1 &k2) const noexcept {
      return k1 < k2;
    }

    inline bool equal(const KeyT1 &k1, const KeyT2 &k2) const noexcept {
      return k1 == k2;
    }
  };

  template <typename KeyType> class search_ops_t {
  public:
    using cmp_key_type = KeyType;
    using comparator_t = std::conditional_t<is_dynamic_key, dynamic_cmp *,
                                            static_cmp<key_type, KeyType>>;

    STATIC_KEY_ONLY
    search_ops_t(Stats *stats) : m_stats(stats) {}
    DYNAMIC_KEY_ONLY
    search_ops_t(Stats *stats, const dynamic_cmp *cmp)
        : m_cmp(cmp), m_stats(stats) {}

    template <typename OthKeyType>
    search_ops_t(const search_ops_t<OthKeyType> &o) {
      if constexpr (is_dynamic_key)
        m_cmp = o.m_cmp;
      m_stats = o.m_stats;
    }

    inline bool less(const key_type &k1, const KeyType &k2) const noexcept {
      if constexpr (is_dynamic_key)
        return m_cmp->less(k1, k2);
      else
        return m_cmp.less(k1, k2);
    }
    template <ENABLE_IF((!std::is_same_v<KeyType, key_type>))>
    inline bool less(const KeyType &k1, const key_type &k2) const noexcept {
      if constexpr (is_dynamic_key)
        return m_cmp->less(k1, k2);
      else
        return m_cmp.less(k1, k2);
    }

    inline bool equal(const key_type &k1, const KeyType &k2) const noexcept {
      if constexpr (is_dynamic_key)
        return m_cmp->equal(k1, k2);
      else
        return m_cmp.equal(k1, k2);
    }

    template <typename Key1Type, typename Key2Type>
    inline bool less_equal(const Key1Type &k1, const Key2Type &k2) const
        noexcept {
      return !(k2 < k1);
    }
    template <typename Key1Type, typename Key2Type>
    inline bool greater(const Key1Type &k1, const Key2Type &k2) const noexcept {
      return k2 < k1;
    }
    template <typename Key1Type, typename Key2Type>
    inline bool greater_equal(const Key1Type &k1, const Key2Type &k2) const
        noexcept {
      return !(k1 < k2);
    }

    template <typename NodeType> class lower_bound_cmp {
    public:
      lower_bound_cmp(const search_ops_t *ops, const NodeType *node)
          : ops(ops), node(node) {}
      inline bool operator()(const std::atomic<int> &slot,
                             const KeyType &key) const noexcept {
        return (*this)(detail::load_acquire(slot), key);
      }
      inline bool operator()(int slot, const KeyType &key) const noexcept {
        return ops->less(node->get_key_value_for_offset(slot)->first, key);
      }

    private:
      const search_ops_t *ops;
      const NodeType *node;
    };

    template <typename NodeType> class upper_bound_cmp {
    public:
      upper_bound_cmp(const search_ops_t *ops, const NodeType *node)
          : ops(ops), node(node) {}
      inline bool operator()(const KeyType &key,
                             const std::atomic<int> &slot) const noexcept {
        return (*this)(key, detail::load_acquire(slot));
      }
      inline bool operator()(const KeyType &key, int slot) const noexcept {
        return ops->less(key, node->get_key_value_for_offset(slot)->first);
      }

    private:
      const search_ops_t *ops;
      const NodeType *node;
    };

    template <typename NodeType>
    int lower_bound_pos(const NodeType *node, const KeyType &key,
                        int num_values) const noexcept {
      int firstslot = node->IsLeaf() ? 0 : 1;
      auto slots = node->get_slots();

      return std::lower_bound(slots + firstslot, slots + num_values, key,
                              lower_bound_cmp<NodeType>{this, node}) -
             slots;
    }

    template <typename NodeType>
    int upper_bound_pos(const NodeType *node, const KeyType &key,
                        int num_values) const noexcept {
      int firstslot = node->IsLeaf() ? 0 : 1;
      auto slots = node->get_slots();
      int pos = std::upper_bound(slots + firstslot, slots + num_values, key,
                                 upper_bound_cmp<NodeType>{this, node}) -
                slots;

      return node->IsInner() ? std::min(pos - 1, num_values - 1) : pos;
    }

    template <typename NodeType>
    std::tuple<int, bool, int> lower_bound(const NodeType *node,
                                           const KeyType &key) const noexcept {
      auto num_values = detail::load_acquire(node->num_values);
      auto pos = lower_bound_pos(node, key, num_values);
      auto present =
          pos < num_values && equal(node->get_key_value(pos)->first, key);

      return {pos, present, num_values};
    }

    int search_inner(const inner_node_t *inner, const KeyType &key) const
        noexcept {
      auto [pos, key_present, _] = lower_bound(inner, key);
      return !key_present ? pos - 1 : pos;
    }

    node_t *get_value_lower_than(const inner_node_t *inner,
                                 const KeyType &key) const {
      auto pos = search_inner(inner, key);

      if (pos == 0)
        return inner->get_first_child();

      return inner->get_child(equal(key, inner->get_key(pos)) ? pos - 1 : pos);
    }

    inline node_t *get_child_for_key(const inner_node_t *inner,
                                     const KeyType &key) const noexcept {
      return inner->get_child(search_inner(inner, key));
    }

    int get_slots_greater_than(const leaf_node_t *leaf, const KeyType &key,
                               std::vector<int> &slot_offsets) const noexcept {
      auto num_values = detail::load_acquire(leaf->num_values);
      auto pos = upper_bound_pos(leaf, key, num_values);

      leaf_node_t::get_all_slots(slot_offsets, leaf->get_slots(), num_values);
      return pos;
    }

    int get_slots_greater_than_eq(const leaf_node_t *leaf, const KeyType &key,
                                  std::vector<int> &slot_offsets) const
        noexcept {
      auto num_values = detail::load_acquire(leaf->num_values);
      auto pos = lower_bound_pos(leaf, key, num_values);

      leaf_node_t::get_all_slots(slot_offsets, leaf->get_slots(), num_values);
      return pos;
    }

    int get_slots_less_than(const leaf_node_t *leaf, const key_type &key,
                            std::vector<int> &slot_offsets) const noexcept {
      auto [pos, found, num_values] = lower_bound(leaf, key);

      pos = found ? pos - 1 : pos;
      leaf_node_t::get_all_slots(slot_offsets, leaf->get_slots(), num_values);
      return pos - 1;
    }

    bool get_leaf_containing(const concurrent_map_access *map,
                             const KeyType &key,
                             NodeSnapshotVector &snapshots) const {
      static_assert(std::is_same_v<KeyType, key_type>);

      auto is_leaf_locked =
          map->template traverse_to_leaf<base::FILL_SNAPSHOT_VECTOR>(
              [&](node_t *current) {
                return get_child_for_key(ASINNER(current), key);
              },
              snapshots, map->dummy_snap_vec());

      BTREE_DEBUG_ASSERT(snapshots.size() > 0);

      if (snapshots.size() > 1)
        BTREE_DEBUG_ASSERT(snapshots.back().node->isLeaf());

      return snapshots.size() > 1 && is_leaf_locked;
    }

    NodeSnapshot get_leaf_containing(const concurrent_map_access *map,
                                     const KeyType &key) const noexcept {
      NodeSnapshot leaf_snapshot{};

      auto is_leaf_locked =
          map->template traverse_to_leaf<base::NO_FILL_SNAPSHOT_VECTOR>(
              [&](node_t *current) {
                return get_child_for_key(ASINNER(current), key);
              },
              map->dummy_snap_vec(), leaf_snapshot);

      if (is_leaf_locked) {
        BTREE_DEBUG_ASSERT(leaf_snapshot.node != nullptr);
        leaf_snapshot.node->mutex.unlock();
      }

      if (leaf_snapshot.node)
        BTREE_DEBUG_ASSERT(leaf_snapshot.node->isLeaf());

      return leaf_snapshot;
    }

    std::pair<leaf_node_t *, int>
    get_next_leaf(const concurrent_map_access *map, const KeyType &highkey,
                  std::vector<int> &slots) const noexcept {
      auto [pos, leaf_snapshot, key] = get_next_leaf_util(map, highkey, slots);
      def_search_ops_t ops = *this;

      // `get_next_leaf_util` cannot return PartialKey (KeyType) key.
      // So loop until, we're done or we obtain a complete key.
      while (pos == -1 && !key) {
        std::tie(pos, leaf_snapshot, key) =
            get_next_leaf_util(map, highkey, slots);
      }

      while (pos == -1) {
        auto old = key;
        std::tie(pos, leaf_snapshot, key) =
            ops.get_next_leaf_util(map, *key, slots);
        key = key ? key : old;
      }

      BTREE_DEBUG_ASSERT(pos >= 0);
      return {ASLEAF(leaf_snapshot.node), pos};
    }

    std::pair<leaf_node_t *, int>
    get_prev_leaf(const concurrent_map_access *map, const key_type &lowkey,
                  std::vector<int> &slots) const {
      leaf_node_t *leaf;
      int pos;
      const key_type *key = std::addressof(lowkey);

      do {
        NodeSnapshot leaf_snapshot = get_prev_leaf_containing(map, *key);

        leaf = ASLEAF(leaf_snapshot.node);
        if (leaf) {
          pos = get_slots_less_than(leaf, *key, slots);

          if (map->is_snapshot_stale(leaf_snapshot)) {
            BTREE_UPDATE_STAT(retry, ++);
            continue;
          }

          if (leaf && pos == -1) {
            if (leaf->lowkey) {
              key = std::addressof(leaf->lowkey.value());
              continue;
            } else {
              leaf = nullptr;
              pos = 0;
            }
          }
          break;
        }
      } while (leaf);

      BTREE_DEBUG_ASSERT(pos >= 0);
      return {leaf, pos};
    }

    inline NodeSnapshot get_upper_bound_leaf(const concurrent_map_access *map,
                                             const KeyType &key) const {
      NodeSnapshot leaf_snapshot{};

      bool is_leaf_locked =
          map->template traverse_to_leaf<base::NO_FILL_SNAPSHOT_VECTOR>(
              [&](node_t *current) {
                auto inner = ASINNER(current);
                auto num_values = detail::load_acquire(inner->num_values);
                int pos = upper_bound_pos(inner, key, num_values);

                return ASINNER(current)->get_child(pos);
              },
              map->dummy_snap_vec(), leaf_snapshot);

      if (is_leaf_locked) {
        BTREE_DEBUG_ASSERT(leaf_snapshot.node != nullptr);
        leaf_snapshot.node->mutex.unlock();
      }

      if (leaf_snapshot.node)
        BTREE_DEBUG_ASSERT(leaf_snapshot.node->isLeaf());

      return leaf_snapshot;
    }

  private:
    template <typename OthKeyType> friend class search_ops_t;

    inline std::tuple<int, NodeSnapshot, const key_type *>
    get_next_leaf_util(const concurrent_map_access *map, const KeyType &key,
                       std::vector<int> &slots) const noexcept {
      NodeSnapshot leaf_snapshot = get_leaf_containing(map, key);
      auto leaf = ASLEAF(leaf_snapshot.node);

      if (leaf) {
        auto pos = get_slots_greater_than_eq(leaf, key, slots);

        if (map->is_snapshot_stale(leaf_snapshot)) {
          BTREE_UPDATE_STAT(retry, ++);
          return {-1, leaf_snapshot, nullptr};
        }

        if (leaf && pos == slots.size()) {
          if (leaf->highkey)
            return {-1, leaf_snapshot, std::addressof(leaf->highkey.value())};
          else
            leaf_snapshot.node = nullptr;
        }

        return {pos, leaf_snapshot, nullptr};
      }

      return {0, leaf_snapshot, nullptr};
    }

    inline NodeSnapshot
    get_prev_leaf_containing(const concurrent_map_access *map,
                             const key_type &key) const {
      NodeSnapshot leaf_snapshot{};

      bool is_leaf_locked =
          map->template traverse_to_leaf<base::NO_FILL_SNAPSHOT_VECTOR>(
              [&](node_t *current) {
                return get_value_lower_than(ASINNER(current), key);
              },
              map->dummy_snap_vec(), leaf_snapshot);

      if (is_leaf_locked) {
        BTREE_DEBUG_ASSERT(leaf_snapshot.node != nullptr);
        leaf_snapshot.node->mutex.unlock();
      }

      if (leaf_snapshot.node)
        BTREE_DEBUG_ASSERT(leaf_snapshot.node->isLeaf());

      return leaf_snapshot;
    }

    const comparator_t m_cmp = {};
    Stats *m_stats = {};
  };

  struct update_ops_t : search_ops_t<key_type> {
  public:
    using search_ops_t<key_type>::search_ops_t;

    // Must be called with leaf's mutex held
    InsertStatus insert(leaf_node_t *leaf, const key_type &key,
                        const mapped_type &val) const noexcept {
      auto key_present = false;
      int pos = 0;

      if (detail::load_relaxed(leaf->num_values)) {
        std::tie(pos, key_present, std::ignore) = this->lower_bound(leaf, key);

        if (key_present)
          return InsertStatus::DUPLICATE;
      }

      return leaf->insert_into_pos(key, val, pos);
    }

    // Must be called with leaf's mutex held
    std::pair<InsertStatus, std::optional<mapped_type>>
    upsert(leaf_node_t *leaf, const key_type &key, const mapped_type &val) const
        noexcept {
      auto key_present = false;
      int pos = 0;
      std::optional<mapped_type> oldval = std::nullopt;

      if (detail::load_relaxed(leaf->num_values)) {
        std::tie(pos, key_present, std::ignore) = this->lower_bound(leaf, key);

        if (key_present) {
          auto oldval_ptr = &leaf->get_key_value(pos)->second;

          oldval = *oldval_ptr;
          leaf->atomic_node_update([&]() { *oldval_ptr = val; });

          return {InsertStatus::DUPLICATE, oldval};
        }
      }

      return {leaf->insert_into_pos(key, val, pos), oldval};
    }

    // Must be called with node's mutex held
    template <typename Node>
    void remove_pos(Node *node, int pos) const noexcept {
      auto slots = node->get_slots();

      node->atomic_node_update([&]() {
        int num_values = detail::load_relaxed(node->num_values);

        Node::copy(slots, pos + 1, num_values, pos);
        detail::store_release(node->num_values, num_values - 1);
      });

      int next_slot_offset =
          detail::load_relaxed(node->next_slot_offset) - sizeof(int);
      int logical_pagesize = detail::load_relaxed(node->logical_pagesize) -
                             (sizeof(typename Node::key_value_t) + sizeof(int));

      node->incrementNumDeadValues();
      detail::store_relaxed(node->next_slot_offset, next_slot_offset);
      detail::store_relaxed(node->logical_pagesize, logical_pagesize);
    }

    // Must be called with node's mutex held
    inline std::optional<mapped_type>
    update_leaf(leaf_node_t *leaf, const key_type &key,
                const mapped_type &new_value) const noexcept {
      std::optional<mapped_type> old_value = std::nullopt;
      auto [pos, found, _] = this->lower_bound(leaf, key);

      if (found) {
        auto oldval_ptr = &leaf->get_key_value(pos)->second;

        old_value = *oldval_ptr;
        leaf->atomic_node_update([&]() { *oldval_ptr = new_value; });
      } else {
        return {};
      }

      return old_value;
    }

    // Must be called with node's mutex held
    inline void update_inner_for_trim(inner_node_t *inner, const key_type &key,
                                      node_t *child) const noexcept {
      auto pos = this->search_inner(inner, key);
      auto oldchild = inner->get_child_ptr(pos);
      inner->atomic_node_update(
          [&]() { detail::store_release(*oldchild, child); });
    }

    // Must be called with node's mutex held
    inline InsertStatus
    update_inner_for_split(inner_node_t *inner,
                           const NodeSplitInfo &splitinfo) const noexcept {
      auto left_child = splitinfo.left;
      auto right_child = splitinfo.right;
      auto &split_key = *splitinfo.split_key;

      if (inner->haveEnoughSpace()) {
        int split_pos;
        bool found;
        int current_value_offset = inner->last_value_offset -
                                   sizeof(typename inner_node_t::key_value_t);
        std::tie(split_pos, found, std::ignore) =
            this->lower_bound(inner, split_key);

        BTREE_DEBUG_ASSERT(found == false);
        BTREE_DEBUG_ONLY(found);

        auto old_child = inner->get_child_ptr(split_pos - 1);

        new (inner->opaque() + current_value_offset)
            typename inner_node_t::key_value_t{split_key, right_child};

        inner->atomic_node_update([&]() {
          detail::store_release(*old_child, left_child);
          inner->insert_into_slot(split_pos, current_value_offset);
        });

        inner->update_meta_after_insert();
        return InsertStatus::INSERTED;
      }
      return InsertStatus::OVFLOW;
    }

    // Must be called with inner's mutex held
    inline void update_inner_for_merge(inner_node_t *inner, int merged_pos,
                                       node_t *merged_child) const noexcept {
      auto slots = inner->get_slots();
      auto deleted_pos = merged_pos + 1;
      auto old_child = inner->get_child_ptr(merged_pos);

      inner->atomic_node_update([&]() {
        int num_values = detail::load_relaxed(inner->num_values);

        inner->copy(slots, deleted_pos + 1, num_values, deleted_pos);
        detail::store_release(inner->num_values, num_values - 1);

        detail::store_release(*old_child, merged_child);
      });

      inner->incrementNumDeadValues();
      detail::store_relaxed<int>(inner->next_slot_offset,
                                 detail::load_relaxed(inner->next_slot_offset) -
                                     sizeof(int));
      detail::store_relaxed<int>(
          inner->logical_pagesize,
          detail::load_relaxed(inner->logical_pagesize) -
              (sizeof(typename inner_node_t::key_value_t) + sizeof(int)));
    }
  };

  using def_search_ops_t = search_ops_t<key_type>;

  static inline void
  insert_into_splitnode(update_ops_t ops, const NodeSplitInfo &parent_splitinfo,
                        const NodeSplitInfo &child_splitinfo) {
    auto parent = ASINNER(
        ops.less(*child_splitinfo.split_key, *parent_splitinfo.split_key)
            ? parent_splitinfo.left
            : parent_splitinfo.right);

    ops.update_inner_for_split(parent, child_splitinfo);
  }

  template <typename Update>
  OpResult replace_subtree_on_version_match(const NodeSnapshotVector &snapshots,
                                            int from_node, Update &&update) {
    boost::container::small_vector<node_t *, base::SMALL_HEIGHT> deleted_nodes;

    auto res = [&]() {
      std::vector<std::unique_lock<sync_prim::mutex::Mutex>> locks;
      for (int node_idx = from_node;
           node_idx < static_cast<int>(snapshots.size()); node_idx++) {
        const NodeSnapshot &snapshot = snapshots[node_idx];

        locks.emplace_back(snapshot.node->mutex);

        if (this->is_snapshot_stale(snapshot))
          return OpResult::STALE_SNAPSHOT;
      }

      if (update()) {
        for (int node_idx = from_node;
             node_idx < static_cast<int>(snapshots.size()); node_idx++) {
          const NodeSnapshot &snapshot = snapshots[node_idx];

          snapshot.node->setState(
              snapshot.node->getState().set_deleted().increment_version());

          deleted_nodes.push_back(snapshot.node);
        }

        return OpResult::SUCCESS;
      }

      return OpResult::FAILURE;
    }();

    if (res == OpResult::SUCCESS)
      this->m_gc.retire_in_new_epoch(node_t::free, deleted_nodes);

    return res;
  }

  template <typename Node>
  std::pair<OpResult, NodeSplitInfo>
  split_node(update_ops_t ops, int node_idx,
             const NodeSnapshotVector &snapshots,
             NodeSplitInfo &prev_split_info) {
    const NodeSnapshot &node_snapshot = snapshots[node_idx];
    const NodeSnapshot &parent_snapshot = snapshots[node_idx - 1];
    Node *node = static_cast<Node *>(node_snapshot.node);
    inner_node_t *parent = static_cast<inner_node_t *>(parent_snapshot.node);
    NodeSplitInfo splitinfo;

    std::lock_guard parentlock{parent ? parent->mutex : *this->m_root_mutex};

    if (this->is_snapshot_stale(parent_snapshot))
      return {OpResult::STALE_SNAPSHOT, {}};

    {
      std::lock_guard lock{node->mutex};

      if (this->is_snapshot_stale(node_snapshot))
        return {OpResult::STALE_SNAPSHOT, {}};

      splitinfo = node->split();
    }

    BTREE_UPDATE_STAT_NODE_BASED(split);

    auto res = replace_subtree_on_version_match(snapshots, node_idx, [&]() {
      if constexpr (Node::IsInner())
        insert_into_splitnode(ops, splitinfo, prev_split_info);

      if (parent) {
        auto ret = ops.update_inner_for_split(parent, splitinfo);

        BTREE_DEBUG_ASSERT(ret != InsertStatus::DUPLICATE);

        return ret == InsertStatus::INSERTED;
      } else {
        this->create_root(splitinfo);

        return true;
      }
    });

    return {res, splitinfo};
  }

  template <typename Node>
  std::pair<OpResult, NodeSplitInfo>
  trim_node(update_ops_t ops, int node_idx, const key_type &key,
            const NodeSnapshotVector &snapshots,
            NodeSplitInfo &prev_split_info) {
    const NodeSnapshot &node_snapshot = snapshots[node_idx];
    const NodeSnapshot &parent_snapshot = snapshots[node_idx - 1];
    Node *node = static_cast<Node *>(node_snapshot.node);
    inner_node_t *parent = static_cast<inner_node_t *>(parent_snapshot.node);
    Node *trimmed_node;

    std::lock_guard lock{parent ? parent->mutex : *this->m_root_mutex};

    if (this->is_snapshot_stale(parent_snapshot))
      return {OpResult::STALE_SNAPSHOT, {}};

    {
      std::lock_guard lock{node->mutex};

      if (this->is_snapshot_stale(node_snapshot))
        return {OpResult::STALE_SNAPSHOT, {}};

      trimmed_node = node->trim();
    }

    BTREE_UPDATE_STAT_NODE_BASED(trim);

    return {replace_subtree_on_version_match(
                snapshots, node_idx,
                [&]() {
                  if constexpr (Node::IsInner()) {
                    ops.update_inner_for_split(
                        reinterpret_cast<inner_node_t *>(trimmed_node),
                        prev_split_info);
                  }

                  if (parent)
                    ops.update_inner_for_trim(parent, key, trimmed_node);
                  else
                    this->store_root(trimmed_node);

                  return true;
                }),
            {trimmed_node, nullptr, nullptr}};
  }

  std::pair<OpResult, NodeSplitInfo>
  handle_node_overflow(update_ops_t ops, int node_idx, const key_type &key,
                       const NodeSnapshotVector &snapshots,
                       NodeSplitInfo &prev_split_info) {
    node_t *node = snapshots[node_idx].node;

    if (node->canTrim()) {
      if (node->isLeaf()) {
        return trim_node<leaf_node_t>(ops, node_idx, key, snapshots,
                                      prev_split_info);
      } else {
        return trim_node<inner_node_t>(ops, node_idx, key, snapshots,
                                       prev_split_info);
      }
    } else {
      if (node->isLeaf()) {
        return split_node<leaf_node_t>(ops, node_idx, snapshots,
                                       prev_split_info);
      } else {
        return split_node<inner_node_t>(ops, node_idx, snapshots,
                                        prev_split_info);
      }
    }
  }

  void handle_overflow(update_ops_t ops, const NodeSnapshotVector &snapshots,
                       const key_type &key) {
    int node_idx = snapshots.size() - 1;
    NodeSplitInfo top_splitinfo{};
    boost::container::small_vector<NodeSplitInfo, base::SMALL_HEIGHT>
        failed_splitinfos;

    auto free_failed_splits = [&failed_splitinfos] {
      // Delete allocated nodes
      for (auto splitinfo : failed_splitinfos) {
        if (splitinfo.left)
          node_t::free(splitinfo.left);

        if (splitinfo.right)
          node_t::free(splitinfo.right);
      }
    };

    BTREE_DEBUG_ASSERT(snapshots[node_idx].node &&
                       snapshots[node_idx].node->isLeaf());

    while (node_idx > 0) {
      auto res =
          handle_node_overflow(ops, node_idx, key, snapshots, top_splitinfo);

      switch (res.first) {
      case OpResult::FAILURE:
        top_splitinfo = res.second;
        failed_splitinfos.emplace_back(res.second);
        node_idx--;
        break;

      case OpResult::STALE_SNAPSHOT:
        failed_splitinfos.emplace_back(res.second);
        free_failed_splits();

      case OpResult::SUCCESS:
        return;
      }
    }

    free_failed_splits();

    BTREE_DEBUG_ASSERT(false && "Shallnot come here");
  }

  template <bool DoUpsert, typename OutputType = std::conditional_t<
                               DoUpsert, std::optional<mapped_type>, bool>>
  inline std::pair<OpResult, OutputType>
  insert_or_upsert_leaf(update_ops_t ops, const NodeSnapshotVector &snapshots,
                        bool is_leaf_locked, const key_type &key,
                        const mapped_type &val) {
    InsertStatus status;
    std::optional<mapped_type> oldval{};
    NodeSnapshot leaf_snapshot = snapshots.back();
    leaf_node_t *leaf = ASLEAF(leaf_snapshot.node);

    if constexpr (!DoUpsert)
      (void)oldval;

    if (is_leaf_locked) {
      BTREE_DEBUG_ASSERT(!this->is_snapshot_stale(leaf_snapshot));

      if constexpr (DoUpsert)
        std::tie(status, oldval) = ops.upsert(leaf, key, val);
      else
        status = ops.insert(leaf, key, val);

      leaf->mutex.unlock();
    } else {
      std::lock_guard lock{leaf->mutex};

      if (this->is_snapshot_stale(leaf_snapshot))
        return {OpResult::STALE_SNAPSHOT, {}};

      if constexpr (DoUpsert)
        std::tie(status, oldval) = ops.upsert(leaf, key, val);
      else
        status = ops.insert(leaf, key, val);
    }

    if (status == InsertStatus::OVFLOW) {
      handle_overflow(ops, snapshots, key);
      return {OpResult::STALE_SNAPSHOT, {}};
    }

    if (status == InsertStatus::INSERTED)
      BTREE_UPDATE_STAT(element, ++);

    if constexpr (DoUpsert)
      return {OpResult::SUCCESS, oldval};
    else
      return {OpResult::SUCCESS, status != InsertStatus::DUPLICATE};
  }

  inline std::pair<OpResult, std::optional<mapped_type>>
  update_leaf(update_ops_t ops, const NodeSnapshot &leaf_snapshot,
              const key_type &key, const mapped_type &val) {
    leaf_node_t *leaf = ASLEAF(leaf_snapshot.node);
    std::lock_guard lock{leaf->mutex};

    if (this->is_snapshot_stale(leaf_snapshot))
      return {OpResult::STALE_SNAPSHOT, std::nullopt};

    return {OpResult::SUCCESS, ops.update_leaf(leaf, key, val)};
  }

  template <bool DoUpsert>
  auto insert_or_upsert(update_ops_t ops, const key_type &key,
                        const mapped_type &val) {
    NodeSnapshotVector snapshots;

    this->ensure_root();
    while (true) {
      EpochGuard eg(this);
      bool is_leaf_locked = ops.get_leaf_containing(this, key, snapshots);

      BTREE_DEBUG_ASSERT(snapshots.size() > 1);

      if (auto res = this->insert_or_upsert_leaf<DoUpsert>(
              ops, snapshots, is_leaf_locked, key, val);
          res.first != OpResult::STALE_SNAPSHOT) {
        return res.second;
      }

      BTREE_UPDATE_STAT(retry, ++);
    }
  }

  struct MergeInfo {
    const key_type &merge_key;
    int sibilingpos;
  };

  template <typename Node>
  std::optional<MergeInfo> get_merge_info(update_ops_t ops, const Node *node,
                                          const inner_node_t *parent,
                                          const key_type &key) const {
    int pos = ops.search_inner(parent, key);

    if (pos == 0)
      return {};

    return MergeInfo{parent->get_key_value(pos)->first, pos - 1};
  }

  template <typename Node>
  void merge_node(update_ops_t ops, int node_idx,
                  const NodeSnapshotVector &snapshots, const key_type &key) {
    if (node_idx == 1)
      return;

    const NodeSnapshot &node_snapshot = snapshots[node_idx];
    const NodeSnapshot &parent_snapshot = snapshots[node_idx - 1];
    auto node = static_cast<Node *>(node_snapshot.node);
    auto parent = static_cast<inner_node_t *>(parent_snapshot.node);
    std::optional<MergeInfo> mergeinfo = get_merge_info(ops, node, parent, key);
    Node *mergednode = nullptr;
    Node *sibiling = nullptr;
    bool par_und_full = false;

    if (mergeinfo) {
      std::lock_guard parent_lock{parent->mutex};

      if (this->is_snapshot_stale(parent_snapshot))
        return;

      const key_type &merge_key = mergeinfo->merge_key;
      int sibilingpos = mergeinfo->sibilingpos;
      sibiling = static_cast<Node *>(parent->get_child(sibilingpos));

      std::lock_guard sibiling_lock{sibiling->mutex};
      std::lock_guard node_lock{node->mutex};

      if (this->is_snapshot_stale(node_snapshot))
        return;

      mergednode = sibiling->merge(node, merge_key);

      if (mergednode) {
        BTREE_UPDATE_STAT_NODE_BASED(merge);

        ops.update_inner_for_merge(parent, sibilingpos, mergednode);

        sibiling->setState(
            sibiling->getState().set_deleted().increment_version());
        node->setState(node->getState().set_deleted().increment_version());
      }

      par_und_full = parent->isUnderfull();
    }

    if (mergednode) {
      this->m_gc.retire_in_current_epoch(node_t::free, sibiling);
      this->m_gc.retire_in_current_epoch(node_t::free, node);
      this->m_gc.switch_epoch();
    }

    if (par_und_full)
      merge_node<inner_node_t>(ops, node_idx - 1, snapshots, key);
  }

  std::pair<OpResult, std::optional<mapped_type>>
  delete_from_leaf(update_ops_t ops, const key_type &key, bool is_leaf_locked,
                   NodeSnapshotVector &snapshots) {
    NodeSnapshot &leaf_snapshot = snapshots.back();
    leaf_node_t *leaf = ASLEAF(leaf_snapshot.node);
    std::pair<OpResult, std::optional<mapped_type>> ret{};

    {
      bool is_deleted = false;

      auto do_delete = [&]() {
        if (this->is_snapshot_stale(leaf_snapshot)) {
          ret = {OpResult::STALE_SNAPSHOT, std::nullopt};
          return;
        }

        auto [pos, key_present, _] = ops.lower_bound(leaf, key);

        if (!key_present) {
          ret = {OpResult::SUCCESS, std::nullopt};
          return;
        }

        ret = {OpResult::SUCCESS, leaf->get_key_value(pos)->second};

        ops.remove_pos(leaf, pos);
        is_deleted = true;

        leaf_snapshot = {leaf, leaf->getState()};
      };

      if (is_leaf_locked) {
        do_delete();
        leaf->mutex.unlock();
      } else {
        std::lock_guard lock{leaf->mutex};

        do_delete();
      }

      if (is_deleted)
        BTREE_UPDATE_STAT(element, --);
    }

    if (leaf->isUnderfull())
      merge_node<leaf_node_t>(ops, snapshots.size() - 1, snapshots, key);

    return ret;
  }

  inline NodeSnapshot get_last_leaf() const {
    NodeSnapshot leaf_snapshot{};

    bool is_leaf_locked =
        this->template traverse_to_leaf<base::NO_FILL_SNAPSHOT_VECTOR>(
            [](node_t *current) { return ASINNER(current)->get_last_child(); },
            this->dummy_snap_vec(), leaf_snapshot);

    if (is_leaf_locked) {
      BTREE_DEBUG_ASSERT(leaf_snapshot.node != nullptr);
      leaf_snapshot.node->mutex.unlock();
    }

    if (leaf_snapshot.node)
      BTREE_DEBUG_ASSERT(leaf_snapshot.node->isLeaf());

    return leaf_snapshot;
  }

  inline NodeSnapshot get_first_leaf() const {
    NodeSnapshot leaf_snapshot{};

    bool is_leaf_locked =
        this->template traverse_to_leaf<base::NO_FILL_SNAPSHOT_VECTOR>(
            [](node_t *current) { return ASINNER(current)->get_first_child(); },
            this->dummy_snap_vec(), leaf_snapshot);

    if (is_leaf_locked) {
      BTREE_DEBUG_ASSERT(leaf_snapshot.node != nullptr);
      leaf_snapshot.node->mutex.unlock();
    }

    if (leaf_snapshot.node)
      BTREE_DEBUG_ASSERT(leaf_snapshot.node->isLeaf());

    return leaf_snapshot;
  }

  std::optional<mapped_type> update(update_ops_t ops, const key_type &key,
                                    const mapped_type &val) {
    while (true) {
      EpochGuard eg(this);
      NodeSnapshot leaf_snapshot = ops.get_leaf_containing(this, key);

      if (leaf_snapshot.node == nullptr)
        return {};

      if (auto res = update_leaf(ops, leaf_snapshot, key, val);
          res.first != OpResult::STALE_SNAPSHOT) {
        return res.second;
      }

      BTREE_UPDATE_STAT(retry, ++);
    }
  }

  template <typename KeyType>
  std::optional<mapped_type> search(search_ops_t<KeyType> ops,
                                    const KeyType &key) {
    while (true) {
      EpochGuard eg(this);
      NodeSnapshot leaf_snapshot = ops.get_leaf_containing(this, key);

      if (leaf_snapshot.node) {
        leaf_node_t *leaf = ASLEAF(leaf_snapshot.node);
        auto [pos, key_present, _] = ops.lower_bound(leaf, key);

        std::optional<mapped_type> val =
            key_present
                ? std::optional<mapped_type>{leaf->get_key_value(pos)->second}
                : std::nullopt;

        if (this->is_snapshot_stale(leaf_snapshot)) {
          BTREE_UPDATE_STAT(retry, ++);
          continue;
        }

        return val;
      } else {
        return {};
      }
    };
  }

  std::optional<mapped_type> remove(update_ops_t ops, const key_type &key) {
    NodeSnapshotVector snapshots;

    snapshots.clear();
    while (true) {
      EpochGuard eg(this);
      bool is_leaf_locked = ops.get_leaf_containing(this, key, snapshots);

      if (snapshots.size() > 1) {
        if (auto res = delete_from_leaf(ops, key, is_leaf_locked, snapshots);
            res.first != OpResult::STALE_SNAPSHOT) {
          return res.second;
        }
      } else {
        return {};
      }

      BTREE_UPDATE_STAT(retry, ++);
    }
  }
};

template <typename Key, typename Value, typename Traits, typename Stats>
struct concurrent_map_iter
    : public concurrent_map_access<Key, Value, Traits, Stats> {
private:
  using base = concurrent_map_access<Key, Value, Traits, Stats>;
  using key_type = typename base::key_type;
  using inner_node_t = typename base::inner_node_t;
  using leaf_node_t = typename base::leaf_node_t;
  using mapped_type = typename base::mapped_type;
  using def_search_ops_t = typename base::def_search_ops_t;
  using EpochGuard = typename base::EpochGuard;

protected:
  // Used for iterating till end.
  // XXX: Don't compare two iterators except when one of them is end()
  template <iter_direction IDir> class iterator_impl {
  public:
    // The key type of the btree. Returned by key().
    using key_type = const key_type;

    // The data type of the btree. Returned by data().
    using data_type = mapped_type;

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
    const def_search_ops_t m_ops;
    const concurrent_map_iter *m_bt;
    EpochGuard m_eg;
    const leaf_node_t *m_leaf;
    std::vector<int> m_slots;
    int m_curpos = 0;

    template <typename PreTravHook, typename PostTravHook>
    void next(PreTravHook &&pre, PostTravHook &&post) {
      if constexpr (IDir == iter_direction::FORWARD)
        increment(pre, post);
      else
        decrement(pre, post);
    }
    template <typename PreTravHook, typename PostTravHook>
    void prev(PreTravHook &&pre, PostTravHook &&post) {
      if constexpr (IDir == iter_direction::FORWARD)
        decrement(pre, post);
      else
        increment(pre, post);
    }

    template <typename PreTravHook, typename PostTravHook>
    void increment(PreTravHook &&pre, PostTravHook &&post) {
      if (++m_curpos >= static_cast<int>(m_slots.size())) {
        if (this->m_leaf->highkey) {
          if (pre(*this->m_leaf->highkey)) {
            auto highkey = *this->m_leaf->highkey;
            m_eg.refresh();
            std::tie(m_leaf, m_curpos) =
                m_ops.get_next_leaf(m_bt, highkey, m_slots);

            if (m_leaf && post(*this))
              return;
          }
        }
        clear();
      }
    }

    template <typename PreTravHook, typename PostTravHook>
    void decrement(PreTravHook &&pre, PostTravHook &&post) {
      if (--m_curpos < 0) {
        if (this->m_leaf->lowkey) {
          if (pre(*this->m_leaf->lowkey)) {
            auto lowkey = *this->m_leaf->lowkey;
            m_eg.refresh();
            std::tie(m_leaf, m_curpos) =
                m_ops.get_prev_leaf(m_bt, lowkey, m_slots);

            if (m_leaf && post(*this))
              return;
          }
        }
        clear();
      }
    }

    void clear() noexcept {
      m_leaf = nullptr;
      m_curpos = 0;
      m_slots.clear();
      m_eg.release();
    }

    pair_type *get_pair() const {
      return reinterpret_cast<pair_type *>(
          m_leaf->get_key_value_for_offset(m_slots[m_curpos]));
    }

    friend struct concurrent_map_iter;
    template <range_kind LRKind, range_kind RRKind, typename DefOps,
              typename MinkeyOps, typename MaxkeyOps, iter_direction OtherIDir>
    friend class range_iterator;

  public:
    inline iterator_impl(def_search_ops_t ops, const concurrent_map_iter *bt,
                         EpochGuard &&eg, const leaf_node_t *leaf,
                         std::vector<int> &&slots, int curpos)
        : m_ops(ops), m_bt(bt), m_eg(std::move(eg)), m_leaf(leaf),
          m_slots(std::move(slots)), m_curpos(curpos) {}

    inline iterator_impl(def_search_ops_t ops, const concurrent_map_iter *bt,
                         const EpochGuard &eg, const leaf_node_t *leaf,
                         const std::vector<int> &slots, int curpos)
        : m_ops(ops), m_bt(bt), m_eg(eg), m_leaf(leaf), m_slots(slots),
          m_curpos(curpos) {}

    inline iterator_impl(const iterator_impl &it) = default;
    inline iterator_impl(iterator_impl &&) = default;

    template <iter_direction OtherIDir>
    inline iterator_impl(const iterator_impl<OtherIDir> &it)
        : iterator_impl(it.m_ops, it.m_bt, it.m_eg, it.m_leaf, it.m_slots,
                        it.m_curpos) {}

    inline reference operator*() const { return *get_pair(); }
    inline pointer operator->() const { return get_pair(); }
    inline key_type &key() const { return get_pair()->first; }
    inline data_type &data() const { return get_pair()->second; }

    inline iterator_impl operator++() {
      next([](auto) { return true; }, [](auto) { return true; });
      return *this;
    }

    inline iterator_impl operator++(int) {
      auto copy = *this;
      next([](auto) { return true; }, [](auto) { return true; });
      return copy;
    }

    inline iterator_impl operator--() {
      prev([](auto) { return true; }, [](auto) { return true; });
      return *this;
    }

    inline iterator_impl operator--(int) {
      auto copy = *this;
      prev([](auto) { return true; }, [](auto) { return true; });
      return copy;
    }

    inline bool operator==(const iterator_impl &other) const {
      BTREE_DEBUG_ASSERT(m_bt == other.m_bt);

      return m_leaf == other.m_leaf &&
             (m_leaf ? m_slots[m_curpos] == other.m_slots[other.m_curpos]
                     : true);
    }

    inline bool operator!=(const iterator_impl &other) const {
      return !(*this == other);
    }
  };

  template <range_kind LRKind, range_kind RRKind, typename DefOps,
            typename MinkeyOps, typename MaxkeyOps, iter_direction IDir>
  class range_iterator {
  public:
    using minkey_type = typename MinkeyOps::cmp_key_type;
    using maxkey_type = typename MaxkeyOps::cmp_key_type;
    using endkey_type = std::conditional_t<IDir == iter_direction::FORWARD,
                                           minkey_type, maxkey_type>;
    using endkey_ops_type = std::conditional_t<IDir == iter_direction::FORWARD,
                                               MinkeyOps, MaxkeyOps>;
    using iterator = iterator_impl<IDir>;

    range_iterator(const concurrent_map_iter *map, DefOps defops,
                   MinkeyOps min_ops, MaxkeyOps max_ops,
                   const minkey_type &min_key, const maxkey_type &max_key)
        : m_defops(defops), m_min_ops(min_ops), m_max_ops(max_ops),
          m_endkey_ops(get_end_ops(min_ops, max_ops)),
          m_endkey(get_end_key(min_key, max_key)),
          m_it(get_iter(map, min_key, max_key)) {}

    bool operator++() noexcept {
      m_it->next([this](auto &key) { return need_trav(key); },
                 [this](auto &it) { return ceil_iter(it); });

      if (m_it->m_leaf == nullptr)
        m_it = std::nullopt;
      return m_it.has_value();
    }
    operator bool() const noexcept { return m_it.has_value(); }

    inline auto &operator*() const { return m_it->operator*(); }
    inline auto *operator-> () const { return m_it->operator->(); }
    inline auto &key() const { return m_it->key(); }
    inline auto &data() const { return m_it->data(); }

  private:
    static auto get_end_ops(MinkeyOps min_ops, MaxkeyOps max_ops) {
      if constexpr (IDir == iter_direction::FORWARD)
        return max_ops;
      else
        return min_ops;
    }

    template <typename MinkeyType, typename MaxkeyType>
    static auto get_end_key(const MinkeyType &min_key,
                            const MaxkeyType &max_key) {
      if constexpr (IDir == iter_direction::FORWARD)
        return std::cref(max_key);
      else
        return std::cref(min_key);
    }

    bool need_trav(const key_type &key) const noexcept {
      if constexpr (IDir == iter_direction::FORWARD) {
        if constexpr (RRKind == range_kind::INCLUSIVE)
          return m_endkey_ops.less_equal(key, m_endkey);
        else
          return m_endkey_ops.less(key, m_endkey);
      } else {
        if constexpr (LRKind == range_kind::INCLUSIVE)
          return m_endkey_ops.greater_equal(key, m_endkey);
        else
          return m_endkey_ops.greater(key, m_endkey);
      }
    }

    bool ceil_iter(iterator &it) noexcept {
      if constexpr (IDir == iter_direction::FORWARD)
        return ceil_forward_iter(it);
      else
        return ceil_reverse_iter(it);
    }

    bool ceil_forward_iter(iterator &it) noexcept {
      auto &slots = it.m_slots;
      if constexpr (RRKind == range_kind::INCLUSIVE) {
        auto cmp =
            typename endkey_ops_type::template upper_bound_cmp<leaf_node_t>{
                &m_endkey_ops, it.m_leaf};
        auto start = std::upper_bound(slots.begin() + it.m_curpos, slots.end(),
                                      m_endkey, cmp);
        slots.erase(start, slots.end());
      } else {
        auto cmp =
            typename endkey_ops_type::template lower_bound_cmp<leaf_node_t>{
                &m_endkey_ops, it.m_leaf};
        auto start = std::lower_bound(slots.begin() + it.m_curpos, slots.end(),
                                      m_endkey, cmp);
        slots.erase(start, slots.end());
      }

      if (it.m_curpos == static_cast<int>(slots.size()))
        return false;
      return true;
    }

    bool ceil_reverse_iter(iterator &it) noexcept {
      auto &slots = it.m_slots;
      BTREE_DEBUG_ASSERT(it.m_curpos != slots.size());
      auto off = slots.size() - it.m_curpos;
      auto last = slots.begin() + it.m_curpos + 1;
      if constexpr (LRKind == range_kind::INCLUSIVE) {
        auto cmp =
            typename endkey_ops_type::template lower_bound_cmp<leaf_node_t>{
                &m_endkey_ops, it.m_leaf};
        auto end = std::lower_bound(slots.begin(), last, m_endkey, cmp);
        slots.erase(slots.begin(), end);
      } else {
        auto cmp =
            typename endkey_ops_type::template upper_bound_cmp<leaf_node_t>{
                &m_endkey_ops, it.m_leaf};
        auto end = std::upper_bound(slots.begin(), last, m_endkey, cmp);
        slots.erase(slots.begin(), end);
      }

      it.m_curpos = slots.size() - off;
      if (it.m_curpos < 0)
        return false;
      return true;
    }

    template <typename MinkeyType, typename MaxkeyType>
    std::optional<iterator> get_iter(const concurrent_map_iter *map,
                                     const MinkeyType &min_key,
                                     const MaxkeyType &max_key) noexcept {
      auto it = [&] {
        if constexpr (IDir == iter_direction::FORWARD)
          return get_forward_iter(map, min_key, max_key);
        else
          return get_reverse_iter(map, min_key, max_key);
      }();

      if (it && !ceil_iter(*it))
        it = std::nullopt;

      return std::move(it);
    }

    template <typename MinkeyType, typename MaxkeyType>
    std::optional<iterator> get_forward_iter(const concurrent_map_iter *map,
                                             const MinkeyType &min_key,
                                             const MaxkeyType &max_key) {
      std::optional<iterator> it;

      if constexpr (LRKind == range_kind::INCLUSIVE)
        it.emplace(map->lower_bound(min_key, m_min_ops, m_defops));
      else
        it.emplace(map->upper_bound(min_key, m_min_ops, m_defops));

      if (it->m_leaf)
        return std::move(it);
      else
        return {};
    }

    template <typename MinkeyType, typename MaxkeyType>
    std::optional<iterator> get_reverse_iter(const concurrent_map_iter *map,
                                             const MinkeyType &min_key,
                                             const MaxkeyType &max_key) {
      auto it = map->lower_bound(max_key, m_max_ops, m_defops);

      if (it.m_leaf == nullptr)
        return {};

      if (m_max_ops.equal(it->first, max_key)) {
        if constexpr (RRKind == range_kind::EXCLUSIVE)
          --it;
      } else {
        --it;
      }

      if (it.m_leaf != nullptr)
        return {std::move(it)};
      else
        return {};
    }

    const DefOps m_defops;
    const MinkeyOps m_min_ops;
    const MaxkeyOps m_max_ops;
    const endkey_ops_type m_endkey_ops;
    const endkey_type &m_endkey;
    std::optional<iterator> m_it;
  };

  using const_iterator = iterator_impl<iter_direction::FORWARD>;
  using const_reverse_iterator = iterator_impl<iter_direction::REVERSE>;

  template <range_kind LRKind, range_kind RRKind, typename DefOps,
            typename MinkeyOps, typename MaxkeyOps>
  using const_range_iterator =
      range_iterator<LRKind, RRKind, DefOps, MinkeyOps, MaxkeyOps,
                     iter_direction::FORWARD>;
  template <range_kind LRKind, range_kind RRKind, typename DefOps,
            typename MinkeyOps, typename MaxkeyOps>
  using const_reverse_range_iterator =
      range_iterator<LRKind, RRKind, DefOps, MinkeyOps, MaxkeyOps,
                     iter_direction::REVERSE>;

  inline const_iterator cbegin(def_search_ops_t ops) const {
    leaf_node_t *leaf;
    int pos = 0;
    std::vector<int> slots;
    EpochGuard eg{this};

    do {
      auto leaf_snapshot = this->get_first_leaf();
      leaf = ASLEAF(leaf_snapshot.node);

      if (leaf) {
        leaf->get_all_slots(slots);

        if (!this->is_snapshot_stale(leaf_snapshot))
          break;
      }
      eg.refresh();
    } while (leaf);

    if (leaf && slots.empty())
      std::tie(leaf, pos) =
          ops.get_next_leaf(this, leaf->highkey.value(), slots);

    return leaf ? const_iterator{ops,
                                 this,
                                 std::move(eg),
                                 leaf,
                                 std::move(slots),
                                 pos}
                : cend(ops);
  }

  inline const_iterator cend(def_search_ops_t ops) const {
    return {ops, this, {}, nullptr, std::vector<int>{}, 0};
  }

  inline const_iterator begin(def_search_ops_t ops) const {
    return cbegin(ops);
  }

  inline const_iterator end(def_search_ops_t ops) const { return cend(ops); }

  inline const_reverse_iterator crbegin(def_search_ops_t ops) const {
    leaf_node_t *leaf;
    int pos;
    std::vector<int> slots;
    EpochGuard eg{this};

    do {
      auto leaf_snapshot = this->get_last_leaf();
      leaf = ASLEAF(leaf_snapshot.node);

      if (leaf) {
        leaf->get_all_slots(slots);

        if (!this->is_snapshot_stale(leaf_snapshot))
          break;
      }
      eg.refresh();
    } while (leaf);

    if (leaf && slots.empty()) {
      std::tie(leaf, pos) =
          ops.get_prev_leaf(this, leaf->lowkey.value(), slots);
    } else {
      pos = slots.size() - 1;
    }

    return leaf ? const_reverse_iterator{ops,
                                         this,
                                         std::move(eg),
                                         leaf,
                                         std::move(slots),
                                         pos}
                : crend(ops);
  }

  inline const_reverse_iterator crend(def_search_ops_t ops) const {
    return end(ops);
  }

  inline const_reverse_iterator rbegin(def_search_ops_t ops) const {
    return crbegin(ops);
  }

  inline const_reverse_iterator rend(def_search_ops_t ops) const {
    return crend(ops);
  }

  template <typename KeyType, typename KeyTypeOps, typename DefOps>
  inline const_iterator lower_bound(const KeyType &key, KeyTypeOps kops,
                                    DefOps ops) const {
    std::vector<int> slots;
    EpochGuard eg{this};
    auto [leaf, pos] = kops.get_next_leaf(this, key, slots);

    return leaf ? const_iterator{ops,
                                 this,
                                 std::move(eg),
                                 leaf,
                                 std::move(slots),
                                 pos}
                : end(ops);
  }

  template <typename KeyType, typename KeyTypeOps, typename DefOps>
  inline const_iterator upper_bound(const KeyType &key, KeyTypeOps kops,
                                    DefOps ops) const {
    std::vector<int> slots;
    leaf_node_t *leaf;
    EpochGuard eg{this};
    int pos = -1;

    do {
      auto leaf_snapshot = kops.get_upper_bound_leaf(this, key);
      leaf = ASLEAF(leaf_snapshot.node);

      if (leaf) {
        pos = kops.get_slots_greater_than(leaf, key, slots);

        if (!this->is_snapshot_stale(leaf_snapshot))
          break;
      }
      eg.refresh();
    } while (leaf);

    bool node_empty = pos == slots.size();
    auto it =
        const_iterator{ops, this, std::move(eg), leaf, std::move(slots), pos};

    if (leaf && node_empty)
      it++;

    return leaf ? std::move(it) : end({this->m_stats.get()});
  }
};
} // namespace detail

template <typename Key, typename Value, typename Traits = btree_traits_default,
          typename Stats = std::conditional_t<Traits::STAT, btree_stats_t,
                                              btree_empty_stats_t>>
class concurrent_map
    : public detail::concurrent_map_iter<Key, Value, Traits, Stats> {
private:
  using base = detail::concurrent_map_base<Key, Value, Traits, Stats>;
  using access = detail::concurrent_map_access<Key, Value, Traits, Stats>;
  using iter = detail::concurrent_map_iter<Key, Value, Traits, Stats>;

public:
  using dynamic_cmp = typename base::dynamic_cmp;
  using const_iterator = typename iter::const_iterator;
  using const_reverse_iterator = typename iter::const_reverse_iterator;

  concurrent_map() = default;
  concurrent_map(const concurrent_map &) = default;
  concurrent_map(concurrent_map &&) = default;

private:
  using key_type = typename base::key_type;
  using mapped_type = typename base::mapped_type;
  using EpochGuard = typename access::EpochGuard;
  using inner_node_t = typename base::inner_node_t;
  using leaf_node_t = typename base::leaf_node_t;
  using node_t = typename base::node_t;
  using def_search_ops_t = typename access::def_search_ops_t;
  template <typename KeyType>
  using search_ops_t = typename access::template search_ops_t<KeyType>;

  static constexpr auto is_dynamic_key = base::is_dynamic_key;

public:
  void reserve(size_t) {
    // No-op
  }

  DYNAMIC_KEY_ONLY
  bool Insert(const key_type &key, const mapped_type &val,
              const dynamic_cmp *cmp) {
    static_assert(is_dynamic_key == true);
    return this->template insert_or_upsert<base::DO_INSERT>(
        {cmp, this->m_stats.get()}, key, val);
  }

  STATIC_KEY_ONLY
  bool Insert(const key_type &key, const mapped_type &val) {
    static_assert(is_dynamic_key == false);
    return this->template insert_or_upsert<base::DO_INSERT>(
        {this->m_stats.get()}, key, val);
  }

  DYNAMIC_KEY_ONLY
  std::optional<mapped_type> Upsert(const key_type &key, const mapped_type &val,
                                    const dynamic_cmp *cmp) {
    static_assert(is_dynamic_key == true);
    return this->template insert_or_upsert<base::DO_UPSERT>(
        {cmp, this->m_stats.get()}, key, val);
  }

  STATIC_KEY_ONLY
  std::optional<mapped_type> Upsert(const key_type &key,
                                    const mapped_type &val) {
    static_assert(is_dynamic_key == false);
    return this->template insert_or_upsert<base::DO_UPSERT>(
        {this->m_stats.get()}, key, val);
  }

  DYNAMIC_KEY_ONLY
  std::optional<mapped_type> Update(const key_type &key, const mapped_type &val,
                                    const dynamic_cmp *cmp) {
    static_assert(is_dynamic_key == true);
    return this->update({cmp, this->m_stats.get()}, key, val);
  }

  STATIC_KEY_ONLY
  std::optional<mapped_type> Update(const key_type &key,
                                    const mapped_type &val) {
    static_assert(is_dynamic_key == false);
    return this->update({this->m_stats.get()}, key, val);
  }

  DYNAMIC_KEY_ONLY
  std::optional<mapped_type> Search(const key_type &key,
                                    const dynamic_cmp *cmp) {
    static_assert(is_dynamic_key == true);
    return this->search({cmp, this->m_stats.get()}, key);
  }

  template <typename KeyType, ENABLE_IF_STATIC_KEY>
  std::optional<mapped_type> Search(const KeyType &key) {
    static_assert(is_dynamic_key == false);
    return this->search({this->m_stats.get()}, key);
  }

  DYNAMIC_KEY_ONLY
  std::optional<mapped_type> Delete(const key_type &key,
                                    const dynamic_cmp *cmp) {
    static_assert(is_dynamic_key == true);
    return this->remove({cmp, this->m_stats.get()}, key);
  }

  STATIC_KEY_ONLY
  std::optional<mapped_type> Delete(const key_type &key) {
    static_assert(is_dynamic_key == false);
    return this->remove({this->m_stats.get()}, key);
  }

  STATIC_KEY_ONLY
  inline const_iterator cbegin() const {
    static_assert(is_dynamic_key == false);
    return this->iter::cbegin({this->m_stats.get()});
  }
  DYNAMIC_KEY_ONLY
  inline const_iterator cbegin(const dynamic_cmp *cmp) const {
    static_assert(is_dynamic_key == true);
    return this->iter::cbegin({cmp, this->m_stats.get()});
  }

  STATIC_KEY_ONLY
  inline const_iterator cend() const {
    static_assert(is_dynamic_key == false);
    return this->iter::cend({this->m_stats.get()});
  }
  DYNAMIC_KEY_ONLY
  inline const_iterator cend(const dynamic_cmp *cmp) const {
    static_assert(is_dynamic_key == true);
    return this->iter::cend({cmp, this->m_stats.get()});
  }

  STATIC_KEY_ONLY
  inline const_iterator begin() const {
    static_assert(is_dynamic_key == false);
    return this->iter::begin({this->m_stats.get()});
  }
  DYNAMIC_KEY_ONLY
  inline const_iterator begin(const dynamic_cmp *cmp) const {
    static_assert(is_dynamic_key == true);
    return this->iter::begin({cmp, this->m_stats.get()});
  }

  STATIC_KEY_ONLY
  inline const_iterator end() const {
    static_assert(is_dynamic_key == false);
    return this->iter::end({this->m_stats.get()});
  }
  DYNAMIC_KEY_ONLY
  inline const_iterator end(const dynamic_cmp *cmp) const {
    static_assert(is_dynamic_key == true);
    return this->iter::end({cmp, this->m_stats.get()});
  }

  STATIC_KEY_ONLY
  inline const_reverse_iterator crbegin() const {
    static_assert(is_dynamic_key == false);
    return this->iter::crbegin({this->m_stats.get()});
  }
  DYNAMIC_KEY_ONLY
  inline const_reverse_iterator crbegin(const dynamic_cmp *cmp) const {
    static_assert(is_dynamic_key == true);
    return this->iter::crbegin({cmp, this->m_stats.get()});
  }

  STATIC_KEY_ONLY
  inline const_reverse_iterator crend() const {
    static_assert(is_dynamic_key == false);
    return this->iter::crend({this->m_stats.get()});
  }
  DYNAMIC_KEY_ONLY
  inline const_reverse_iterator crend(const dynamic_cmp *cmp) const {
    static_assert(is_dynamic_key == true);
    return this->iter::crend({cmp, this->m_stats.get()});
  }

  STATIC_KEY_ONLY
  inline const_reverse_iterator rbegin() const {
    static_assert(is_dynamic_key == false);
    return this->iter::rbegin({this->m_stats.get()});
  }
  DYNAMIC_KEY_ONLY
  inline const_reverse_iterator rbegin(const dynamic_cmp *cmp) const {
    static_assert(is_dynamic_key == true);
    return this->iter::rbegin({cmp, this->m_stats.get()});
  }

  STATIC_KEY_ONLY
  inline const_reverse_iterator rend() const {
    static_assert(is_dynamic_key == false);
    return this->iter::rend({this->m_stats.get()});
  }
  DYNAMIC_KEY_ONLY
  inline const_reverse_iterator rend(const dynamic_cmp *cmp) const {
    static_assert(is_dynamic_key == true);
    return this->iter::rend({cmp, this->m_stats.get()});
  }

  template <typename KeyType, ENABLE_IF_STATIC_KEY>
  inline const_iterator lower_bound(const KeyType &key) const {
    static_assert(is_dynamic_key == false);
    return this->iter::lower_bound(key,
                                   search_ops_t<KeyType>{this->m_stats.get()},
                                   def_search_ops_t{this->m_stats.get()});
  }
  DYNAMIC_KEY_ONLY
  inline const_iterator lower_bound(const key_type &key,
                                    const dynamic_cmp *cmp) const {
    static_assert(is_dynamic_key == true);
    auto ops = def_search_ops_t{cmp, this->m_stats.get()};
    return this->iter::lower_bound(key, ops, ops);
  }

  template <typename KeyType, ENABLE_IF_STATIC_KEY>
  inline const_iterator upper_bound(const KeyType &key) const {
    static_assert(is_dynamic_key == false);
    return this->iter::upper_bound(key,
                                   search_ops_t<KeyType>{this->m_stats.get()},
                                   def_search_ops_t{this->m_stats.get()});
  }
  DYNAMIC_KEY_ONLY
  inline const_iterator upper_bound(const key_type &key,
                                    const dynamic_cmp *cmp) const {
    static_assert(is_dynamic_key == true);
    auto ops = def_search_ops_t{cmp, this->m_stats.get()};
    return this->iter::upper_bound(key, ops, ops);
  }

  template <range_kind LeftR, range_kind RightR, typename KeyT1, typename KeyT2,
            ENABLE_IF_STATIC_KEY>
  inline auto range_iter(const KeyT1 &min, const KeyT2 &max) {
    static_assert(is_dynamic_key == false);
    return typename iter::template const_range_iterator<
        LeftR, RightR, def_search_ops_t, search_ops_t<KeyT1>,
        search_ops_t<KeyT2>>{this,
                             {this->m_stats.get()},
                             {this->m_stats.get()},
                             {this->m_stats.get()},
                             min,
                             max};
  }
  template <range_kind LeftR, range_kind RightR, ENABLE_IF_DYNAMIC_KEY>
  inline auto range_iter(const key_type &min, const key_type &max,
                         const dynamic_cmp *cmp) {
    static_assert(is_dynamic_key == true);
    def_search_ops_t ops = {cmp, this->m_stats.get()};
    return typename iter::template const_range_iterator<
        LeftR, RightR, def_search_ops_t, def_search_ops_t, def_search_ops_t>{
        this, ops, ops, ops, min, max};
  }

  template <range_kind LeftR, range_kind RightR, typename KeyT1, typename KeyT2,
            ENABLE_IF_STATIC_KEY>
  inline auto rrange_iter(const KeyT1 &min, const KeyT2 &max) {
    static_assert(is_dynamic_key == false);
    return typename iter::template const_reverse_range_iterator<
        LeftR, RightR, def_search_ops_t, search_ops_t<KeyT1>,
        search_ops_t<KeyT2>>{this,
                             {this->m_stats.get()},
                             {this->m_stats.get()},
                             {this->m_stats.get()},
                             min,
                             max};
  }
  template <range_kind LeftR, range_kind RightR, ENABLE_IF_DYNAMIC_KEY>
  inline auto rrange_iter(const key_type &min, const key_type &max,
                          const dynamic_cmp *cmp) {
    static_assert(is_dynamic_key == true);
    def_search_ops_t ops = {cmp, this->m_stats.get()};
    return typename iter::template const_reverse_range_iterator<
        LeftR, RightR, def_search_ops_t, def_search_ops_t, def_search_ops_t>{
        this, ops, ops, ops, min, max};
  }

  inline int height() const { return this->m_height; }

  inline void reclaim_all() { this->m_gc.reclaim_all(); }

  template <ENABLE_IF(Traits::STAT)> inline std::size_t size() const {
    return this->m_stats->num_elements;
  }

  template <ENABLE_IF(Traits::STAT)> inline bool empty() const {
    return size() == 0;
  }

  template <ENABLE_IF(Traits::STAT)> inline const Stats &stats() const {
    return this->m_stats;
  }

  ~concurrent_map() {
    std::deque<node_t *> nodes;

    if (this->m_root)
      nodes.emplace_back(this->m_root);

    while (!nodes.empty()) {
      node_t *node = nodes.front();

      if (node->isInner())
        ASINNER(node)->get_children(nodes);

      node_t::free(node);
      nodes.pop_front();
    }
  }

  BTREE_DUMP_METHODS
}; // namespace indexes::btree

} // namespace indexes::btree
