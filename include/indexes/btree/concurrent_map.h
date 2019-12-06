// include/indexes/btree/concurrent_map.h
// B+Tree implementation

#pragma once

#include "common.h"
#include "indexes/utils/EpochManager.h"
#include "sync_prim/Mutex.h"

#include <atomic>
#include <bitset>
#include <chrono>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#define DYNAMIC_KEY_ONLY                                                       \
  template <typename Dummy = void,                                             \
            typename = std::enable_if_t<is_dynamic, Dummy>>
#define STATIC_KEY_ONLY                                                        \
  template <typename Dummy = void,                                             \
            typename = std::enable_if_t<!is_dynamic, Dummy>>

namespace indexes::btree {
struct dynamic_key_base {};

template <typename Key, typename Value, typename Traits = btree_traits_default,
          typename Stats = std::conditional_t<Traits::STAT, btree_stats_t,
                                              btree_empty_stats_t>>
class concurrent_map {
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

  static constexpr auto is_dynamic =
      std::is_base_of_v<dynamic_key_base, key_type>;

private:
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

  using lock_guard = std::lock_guard<sync_prim::mutex::Mutex>;

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

    inline nodestate_t getState() const { return state; }

    inline void setState(nodestate_t new_state) { state = new_state; }

    // We only need to find out if we have more than two deleted values, to trim
    inline void incrementNumDeadValues() {
      auto num_dead_values = load_relaxed(this->num_dead_values);

      store_relaxed<int8_t>(this->num_dead_values, num_dead_values > 1
                                                       ? num_dead_values
                                                       : num_dead_values + 1);
    }

    inline bool isUnderfull() const {
      BTREE_DEBUG_ASSERT(logical_pagesize <= Traits::NODE_SIZE);

      return (logical_pagesize * 100) / Traits::NODE_SIZE <
             Traits::NODE_MERGE_THRESHOLD;
    }

    inline char *opaque() const {
      return reinterpret_cast<char *>(reinterpret_cast<intptr_t>(this));
    }

    inline std::atomic<int> *get_slots() const {
      return reinterpret_cast<std::atomic<int> *>(opaque() + sizeof(node_t));
    }

    inline bool canTrim() const { return load_relaxed(num_dead_values) > 1; }

    inline bool canSplit() const { return num_values > 2; }

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
      for (int slot = IsInner() ? 1 : 0; slot < this->num_values; slot++) {
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
      auto next_slot_offset = load_relaxed(this->next_slot_offset);
      auto max_slot_offset = load_relaxed(this->max_slot_offset);

      return ((next_slot_offset + sizeof(int)) <=
              (this->last_value_offset - sizeof(key_value_t))) &&
             (max_slot_offset <=
              static_cast<int>(this->last_value_offset - sizeof(key_value_t)));
    }

    // Must be called with both this's and other's mutex held
    inline bool canMerge(const node_t *other) const {
      auto logical_pagesize = load_relaxed(this->logical_pagesize);
      auto other_logical_pagesize = load_relaxed(other->logical_pagesize);

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
      return get_key_value_for_offset(load_acquire(slots[slot]));
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
      return load_acquire(*get_child_ptr(slot));
    }

    inline const key_type &get_first_key() const {
      if constexpr (IsInner())
        return get_key_value(1)->first;
      else
        return get_key_value(0)->first;
    }

    INNER_ONLY
    inline value_t get_first_child() const { return get_child_ptr(0)->load(); }

    INNER_ONLY
    inline value_t get_last_child() const {
      auto slot = this->num_values - 1;
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
      int next_slot_offset = load_relaxed(this->next_slot_offset) + sizeof(int);
      int logical_pagesize = load_relaxed(this->logical_pagesize) +
                             sizeof(key_value_t) + sizeof(int);
      int max_slot_offset =
          std::max(load_relaxed(this->max_slot_offset), next_slot_offset);

      this->last_value_offset -= sizeof(key_value_t);
      store_relaxed(this->next_slot_offset, next_slot_offset);
      store_relaxed(this->logical_pagesize, logical_pagesize);
      store_relaxed(this->max_slot_offset, max_slot_offset);

      BTREE_DEBUG_ASSERT(next_slot_offset <= this->last_value_offset);
    }

    // Must be called with this's mutex held
    static void copy_backward(std::atomic<int> *slots, int start_pos,
                              int end_pos, int out_end_pos) {
      BTREE_DEBUG_ASSERT(out_end_pos >= end_pos);

      while (start_pos < end_pos) {
        store_release(slots[--out_end_pos], load_relaxed(slots[--end_pos]));
      }
    }

    // Must be called with this's mutex held
    static void copy(std::atomic<int> *slots, int start_pos, int end_pos,
                     int out_pos) {
      BTREE_DEBUG_ASSERT(out_pos < start_pos);

      while (start_pos < end_pos) {
        store_release(slots[out_pos++], load_relaxed(slots[start_pos++]));
      }
    }

    // Must not be called on a reachable node
    INNER_ONLY
    inline void insert_neg_infinity(const value_t &val) {
      int num_values = this->num_values;
      BTREE_DEBUG_ASSERT(this->isInner() && num_values == 0);

      auto slots = this->get_slots();
      int current_value_offset = this->last_value_offset - sizeof(value_t);

      new (this->opaque() + current_value_offset) value_t{val};
      store_relaxed(slots[0], current_value_offset);

      store_relaxed(this->num_values, num_values + 1);
      update_meta_after_insert();
    }

    // Must not be called on a reachable node
    inline void append(const key_type &key, const value_t &val) {
      std::atomic<int> *slots = this->get_slots();
      int current_value_offset = this->last_value_offset - sizeof(key_value_t);
      int pos = this->num_values;
      int num_values = this->num_values;

      new (this->opaque() + current_value_offset) key_value_t{key, val};
      store_relaxed(slots[pos], current_value_offset);

      store_relaxed(this->num_values, num_values + 1);
      update_meta_after_insert();
    }

    // Must be called with this's mutex held and node state set as locked
    inline void insert_into_slot(int pos, int value_offset) {
      int num_values = this->num_values;
      std::atomic<int> *slots = this->get_slots();

      copy_backward(slots, pos, num_values, num_values + 1);
      store_release(slots[pos], value_offset);
      store_release(this->num_values, num_values + 1);
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
        new_node->copy_from(this, 0, this->num_values);
      } else {
        new_node->insert_neg_infinity(get_first_child());
        new_node->copy_from(this, 1, this->num_values);
      }

      return new_node;
    }

    // Must be called with this's mutex held
    NodeSplitInfo split() const {
      BTREE_DEBUG_ASSERT(this->canSplit());

      int split_pos =
          IsInner() ? this->num_values / 2 : (this->num_values + 1) / 2;
      const key_type &split_key = get_key(split_pos);
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

      BTREE_DEBUG_ASSERT(left->lowkey || left->highkey);
      BTREE_DEBUG_ASSERT(right->lowkey || right->highkey);

      return {left, right, &split_key};
    }

    // Must be called with this's mutex held
    inherited_node_t *merge(const inherited_node_t *other,
                            const key_type &merge_key) const {
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

    LEAF_ONLY
    void get_all_slots(std::vector<int> &slot_offsets) const {
      int num_values = this->num_values;
      std::atomic<int> *slots = this->get_slots();

      for (int i = 0; i < num_values; i++) {
        slot_offsets.emplace_back(load_relaxed(slots[i]));
      }
    }

    // Must be called only from destructor or debugging routines
    // Locking doesn't matter
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

  std::unique_ptr<sync_prim::mutex::Mutex> m_root_mutex =
      std::make_unique<sync_prim::mutex::Mutex>();
  std::atomic<nodestate_t> m_root_state = {};
  std::atomic<node_t *> m_root = nullptr;

  std::atomic<int> m_height = 0;
  std::unique_ptr<Stats> m_stats = std::make_unique<Stats>();

  indexes::utils::EpochManager<uint64_t, node_t> m_gc;

  struct EpochGuard {
    concurrent_map *map;

    EpochGuard(concurrent_map *a_map) : map(a_map) { map->m_gc.enter_epoch(); }

    ~EpochGuard() { map->m_gc.exit_epoch(); }
  };

  struct NodeSnapshot {
    node_t *node;
    nodestate_t state;
  };

  using NodeSnapshotVector = std::vector<NodeSnapshot>;

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
      m_root_mutex->lock();
      state = m_root_state;

      if (state.is_deleted())
        m_root_mutex->unlock();
    }
  }

  inline bool try_lock_optimistic(node_t *node, nodestate_t &state) const {
    int num_tries = 0;

    do {
      state = node ? node->getState() : m_root_state.load();

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
        return state != m_root_state;
    } else {
      if (node)
        node->mutex.unlock();
      else
        m_root_mutex->unlock();
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

    for (current = m_root; current && current->isInner();) {
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
  bool traverse_to_leaf(GetChild &&get_child, SnapshotVectorType &snapshots,
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
    store_release(m_root_state, load_relaxed(m_root_state).set_locked());
    store_release(m_root, new_root);
    store_release(
        m_root_state,
        load_relaxed(m_root_state).reset_locked().increment_version());
    store_release(m_height, load_relaxed(m_height) + 1);
  }

  // Root mutex must be held
  inline void create_root(NodeSplitInfo splitinfo) {
    auto new_root = inner_node_t::alloc(splitinfo.left->lowkey,
                                        splitinfo.right->highkey, m_height + 1);
    new_root->insert_neg_infinity(splitinfo.left);
    new_root->append(*splitinfo.split_key, splitinfo.right);
    store_root(new_root);
  }

  inline bool update_root(nodestate_t rootstate, node_t *new_root) {
    lock_guard lock{*m_root_mutex};

    if (m_root_state.load() != rootstate)
      return false;

    store_root(new_root);

    return true;
  }

  inline void ensure_root() {
    while (m_root == nullptr) {
      auto new_root = leaf_node_t::alloc(std::nullopt, std::nullopt, m_height);

      if (!update_root({}, new_root))
        delete new_root;
    }
  }

  inline bool is_snapshot_stale(const NodeSnapshot &snapshot) const {
    return snapshot.node ? snapshot.node->getState() != snapshot.state
                         : m_root_state.load() != snapshot.state;
  }

  template <typename KeyT1, typename KeyT2> struct static_cmp {
    inline bool less(const KeyT1 &k1, const KeyT2 &k2) const noexcept {
      return k1 < k2;
    }
    template <typename Dummy = void,
              typename = std::enable_if_t<!std::is_same_v<KeyT1, KeyT2>, Dummy>>
    inline bool less(const KeyT2 &k1, const KeyT1 &k2) const noexcept {
      return k1 < k2;
    }

    inline bool equal(const KeyT1 &k1, const KeyT2 &k2) const noexcept {
      return k1 == k2;
    }
  };

  template <typename KeyType> class search_ops_t {
  public:
    using comparator_t = std::conditional_t<is_dynamic, dynamic_cmp *,
                                            static_cmp<key_type, KeyType>>;

    search_ops_t(Stats *stats) : m_stats(stats) {}
    template <typename Dummy = search_ops_t,
              typename = std::enable_if_t<is_dynamic, Dummy>>
    search_ops_t(Stats *stats, const dynamic_cmp *cmp)
        : m_cmp(cmp), m_stats(stats) {}

    template <typename OthKeyType>
    search_ops_t(const search_ops_t<OthKeyType> &o) {
      if constexpr (is_dynamic)
        m_cmp = o.m_cmp;
      m_stats = o.m_stats;
    }

    inline bool less(const key_type &k1, const KeyType &k2) const noexcept {
      if constexpr (is_dynamic)
        return m_cmp->less(k1, k2);
      else
        return m_cmp.less(k1, k2);
    }
    template <
        typename Dummy = void,
        typename = std::enable_if_t<!std::is_same_v<KeyType, key_type>, Dummy>>
    inline bool less(const KeyType &k1, const key_type &k2) const noexcept {
      if constexpr (is_dynamic)
        return m_cmp->less(k1, k2);
      else
        return m_cmp.less(k1, k2);
    }

    inline bool equal(const key_type &k1, const KeyType &k2) const noexcept {
      if constexpr (is_dynamic)
        return m_cmp->equal(k1, k2);
      else
        return m_cmp.equal(k1, k2);
    }

    template <typename NodeType>
    int lower_bound_pos(const NodeType *node, const KeyType &key,
                        int num_values) const noexcept {
      int firstslot = node->IsLeaf() ? 0 : 1;
      auto slots = node->get_slots();
      auto cmp = [this, node](int slot, const KeyType &key) {
        return this->less(node->get_key_value_for_offset(slot)->first, key);
      };

      return std::lower_bound(slots + firstslot, slots + num_values, key, cmp) -
             slots;
    }

    template <typename NodeType>
    int upper_bound_pos(const NodeType *node, const KeyType &key,
                        int num_values) const noexcept {
      int firstslot = node->IsLeaf() ? 0 : 1;
      std::atomic<int> *slots = node->get_slots();
      auto cmp = [this, node](const KeyType &key, int slot) {
        return this->less(key, node->get_key_value_for_offset(slot)->first);
      };
      int pos =
          std::upper_bound(slots + firstslot, slots + num_values, key, cmp) -
          slots;

      return node->IsInner() ? std::min(pos - 1, num_values - 1) : pos;
    }

    template <typename NodeType>
    std::pair<int, bool> lower_bound(const NodeType *node,
                                     const KeyType &key) const noexcept {
      int num_values = node->num_values;
      auto pos = lower_bound_pos(node, key, num_values);
      auto present =
          pos < num_values && equal(node->get_key_value(pos)->first, key);

      return {pos, present};
    }

    int search_inner(const inner_node_t *inner, const KeyType &key) const
        noexcept {
      auto [pos, key_present] = lower_bound(inner, key);
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

    void get_slots_greater_than(const leaf_node_t *leaf, const KeyType &key,
                                std::vector<int> &slot_offsets) const noexcept {
      int num_values = leaf->num_values;
      auto pos = upper_bound_pos(leaf, key, num_values);
      auto slots = leaf->get_slots();

      slot_offsets.clear();

      for (int i = pos; i < num_values; i++) {
        slot_offsets.emplace_back(load_relaxed(slots[i]));
      }
    }

    void get_slots_greater_than_eq(const leaf_node_t *leaf, const KeyType &key,
                                   std::vector<int> &slot_offsets) const
        noexcept {
      int num_values = leaf->num_values;
      auto pos = lower_bound_pos(leaf, key, num_values);
      auto slots = leaf->get_slots();

      slot_offsets.clear();

      for (int i = pos; i < num_values; i++) {
        slot_offsets.emplace_back(load_relaxed(slots[i]));
      }
    }

    void get_slots_less_than(const leaf_node_t *leaf, const key_type &key,
                             std::vector<int> &slot_offsets) const noexcept {
      auto slots = leaf->get_slots();
      auto [pos, found] = lower_bound(leaf, key);

      pos = found ? pos - 1 : pos;
      slot_offsets.clear();

      for (int i = 0; i < pos; i++) {
        slot_offsets.emplace_back(load_relaxed(slots[i]));
      }
    }

    bool get_leaf_containing(const concurrent_map *map, const KeyType &key,
                             NodeSnapshotVector &snapshots) const {
      static_assert(std::is_same_v<KeyType, key_type>);

      auto is_leaf_locked = map->traverse_to_leaf<FILL_SNAPSHOT_VECTOR>(
          [&](node_t *current) {
            return get_child_for_key(ASINNER(current), key);
          },
          snapshots, DummyType{});

      BTREE_DEBUG_ASSERT(snapshots.size() > 0);

      if (snapshots.size() > 1)
        BTREE_DEBUG_ASSERT(snapshots.back().node->isLeaf());

      return snapshots.size() > 1 && is_leaf_locked;
    }

    NodeSnapshot get_leaf_containing(const concurrent_map *map,
                                     const KeyType &key) const noexcept {
      NodeSnapshot leaf_snapshot{};
      DummyType dummy;

      auto is_leaf_locked = map->traverse_to_leaf<NO_FILL_SNAPSHOT_VECTOR>(
          [&](node_t *current) {
            return get_child_for_key(ASINNER(current), key);
          },
          dummy, leaf_snapshot);

      if (is_leaf_locked) {
        BTREE_DEBUG_ASSERT(leaf_snapshot.node != nullptr);
        leaf_snapshot.node->mutex.unlock();
      }

      if (leaf_snapshot.node)
        BTREE_DEBUG_ASSERT(leaf_snapshot.node->isLeaf());

      return leaf_snapshot;
    }

    leaf_node_t *get_next_leaf(const concurrent_map *map,
                               const KeyType &highkey,
                               std::vector<int> &slots) const noexcept {
      auto [retry, leaf_snapshot, key] =
          get_next_leaf_util(map, highkey, slots);
      search_ops_t<key_type> ops = *this;

      while (retry) {
        std::tie(retry, leaf_snapshot, key) =
            ops.get_next_leaf_util(map, *key, slots);
      }

      return ASLEAF(leaf_snapshot.node);
    }

    leaf_node_t *get_prev_leaf(const concurrent_map *map,
                               const key_type &lowkey,
                               std::vector<int> &slots) const {
      leaf_node_t *leaf;
      const key_type *key = std::addressof(lowkey);

      do {
        NodeSnapshot leaf_snapshot = get_prev_leaf_containing(map, *key);

        leaf = ASLEAF(leaf_snapshot.node);
        if (leaf) {
          get_slots_less_than(leaf, *key, slots);

          if (map->is_snapshot_stale(leaf_snapshot)) {
            BTREE_UPDATE_STAT(retry, ++);
            continue;
          }

          if (leaf && slots.empty()) {
            if (leaf->lowkey) {
              key = std::addressof(leaf->lowkey.value());
              continue;
            } else {
              leaf = nullptr;
            }
          }
          break;
        }
      } while (leaf);

      return leaf;
    }

    inline NodeSnapshot get_upper_bound_leaf(const concurrent_map *map,
                                             const KeyType &key) const {
      NodeSnapshot leaf_snapshot{};
      DummyType dummy;

      bool is_leaf_locked = map->traverse_to_leaf<NO_FILL_SNAPSHOT_VECTOR>(
          [&](node_t *current) {
            auto inner = ASINNER(current);
            int num_values = inner->num_values;
            int pos = upper_bound_pos(inner, key, num_values);

            return ASINNER(current)->get_child(pos);
          },
          dummy, leaf_snapshot);

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

    inline std::tuple<bool, NodeSnapshot, const key_type *>
    get_next_leaf_util(const concurrent_map *map, const KeyType &key,
                       std::vector<int> &slots) const noexcept {
      NodeSnapshot leaf_snapshot = get_leaf_containing(map, key);
      auto leaf = ASLEAF(leaf_snapshot.node);

      if (leaf) {
        get_slots_greater_than_eq(leaf, key, slots);

        if (map->is_snapshot_stale(leaf_snapshot)) {
          BTREE_UPDATE_STAT(retry, ++);
          return {true, leaf_snapshot, nullptr};
        }

        if (leaf && slots.empty()) {
          if (leaf->highkey)
            return {true, leaf_snapshot, std::addressof(leaf->highkey.value())};
          else
            leaf_snapshot.node = nullptr;
        }

        return {false, leaf_snapshot, nullptr};
      }

      return {false, leaf_snapshot, nullptr};
    }

    inline NodeSnapshot get_prev_leaf_containing(const concurrent_map *map,
                                                 const key_type &key) const {
      NodeSnapshot leaf_snapshot{};
      DummyType dummy;

      bool is_leaf_locked = map->traverse_to_leaf<NO_FILL_SNAPSHOT_VECTOR>(
          [&](node_t *current) {
            return get_value_lower_than(ASINNER(current), key);
          },
          dummy, leaf_snapshot);

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

    // Must be called with this's mutex held
    InsertStatus insert(leaf_node_t *leaf, const key_type &key,
                        const mapped_type &val) const noexcept {
      auto key_present = false;
      int pos = 0;

      if (leaf->num_values) {
        std::tie(pos, key_present) = this->lower_bound(leaf, key);

        if (key_present)
          return InsertStatus::DUPLICATE;
      }

      return leaf->insert_into_pos(key, val, pos);
    }

    // Must be called with this's mutex held
    std::pair<InsertStatus, std::optional<mapped_type>>
    upsert(leaf_node_t *leaf, const key_type &key, const mapped_type &val) const
        noexcept {
      auto key_present = false;
      int pos = 0;
      std::optional<mapped_type> oldval = std::nullopt;

      if (leaf->num_values) {
        std::tie(pos, key_present) = this->lower_bound(leaf, key);

        if (key_present) {
          auto oldval_ptr = &leaf->get_key_value(pos)->second;

          oldval = *oldval_ptr;
          leaf->atomic_node_update([&]() { *oldval_ptr = val; });

          return {InsertStatus::DUPLICATE, oldval};
        }
      }

      return {leaf->insert_into_pos(key, val, pos), oldval};
    }

    // Must be called with this's mutex held
    template <typename Node>
    void remove_pos(Node *node, int pos) const noexcept {
      auto slots = node->get_slots();

      node->atomic_node_update([&]() {
        int num_values = node->num_values;

        node->copy(slots, pos + 1, num_values, pos);
        store_release(node->num_values, num_values - 1);
      });

      int next_slot_offset = load_relaxed(node->next_slot_offset) - sizeof(int);
      int logical_pagesize = load_relaxed(node->logical_pagesize) -
                             (sizeof(typename Node::key_value_t) + sizeof(int));

      node->incrementNumDeadValues();
      store_relaxed(node->next_slot_offset, next_slot_offset);
      store_relaxed(node->logical_pagesize, logical_pagesize);
    }

    // Must be called with this's mutex held
    inline std::optional<mapped_type>
    update_leaf(leaf_node_t *leaf, const key_type &key,
                const mapped_type &new_value) const noexcept {
      std::optional<mapped_type> old_value = std::nullopt;
      auto [pos, found] = this->lower_bound(leaf, key);

      if (found) {
        auto oldval_ptr = &leaf->get_key_value(pos)->second;

        old_value = *oldval_ptr;
        leaf->atomic_node_update([&]() { *oldval_ptr = new_value; });
      } else {
        return {};
      }

      return old_value;
    }

    // Must be called with this's mutex held
    inline void update_inner_for_trim(inner_node_t *inner, const key_type &key,
                                      node_t *child) const noexcept {
      auto pos = this->search_inner(inner, key);
      auto oldchild = inner->get_child_ptr(pos);
      inner->atomic_node_update([&]() { store_release(*oldchild, child); });
    }

    // Must be called with this's mutex held
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
        std::tie(split_pos, found) = this->lower_bound(inner, split_key);

        BTREE_DEBUG_ASSERT(found == false);
        BTREE_DEBUG_ONLY(found);

        auto old_child = inner->get_child_ptr(split_pos - 1);

        new (inner->opaque() + current_value_offset)
            typename inner_node_t::key_value_t{split_key, right_child};

        inner->atomic_node_update([&]() {
          store_release(*old_child, left_child);
          inner->insert_into_slot(split_pos, current_value_offset);
        });

        inner->update_meta_after_insert();
        return InsertStatus::INSERTED;
      }
      return InsertStatus::OVFLOW;
    }

    // Must be called with this's mutex held
    inline void update_inner_for_merge(inner_node_t *inner, int merged_pos,
                                       node_t *merged_child) const noexcept {
      auto slots = inner->get_slots();
      auto deleted_pos = merged_pos + 1;
      auto old_child = inner->get_child_ptr(merged_pos);

      inner->atomic_node_update([&]() {
        int num_values = inner->num_values;

        inner->copy(slots, deleted_pos + 1, num_values, deleted_pos);
        store_release(inner->num_values, num_values - 1);

        store_release(*old_child, merged_child);
      });

      inner->incrementNumDeadValues();
      inner->next_slot_offset -= sizeof(int);
      inner->logical_pagesize -=
          sizeof(typename inner_node_t::key_value_t) + sizeof(int);
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
    static thread_local std::vector<node_t *> deleted_nodes;

    auto res = [&]() {
      std::vector<std::unique_lock<sync_prim::mutex::Mutex>> locks;
      for (int node_idx = from_node;
           node_idx < static_cast<int>(snapshots.size()); node_idx++) {
        const NodeSnapshot &snapshot = snapshots[node_idx];

        locks.emplace_back(snapshot.node->mutex);

        if (is_snapshot_stale(snapshot))
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
      m_gc.retire_in_new_epoch(node_t::free, deleted_nodes);

    deleted_nodes.clear();

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

    lock_guard parentlock{parent ? parent->mutex : *m_root_mutex};

    if (is_snapshot_stale(parent_snapshot))
      return {OpResult::STALE_SNAPSHOT, {}};

    {
      lock_guard lock{node->mutex};

      if (is_snapshot_stale(node_snapshot))
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
        create_root(splitinfo);

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

    lock_guard lock{parent ? parent->mutex : *m_root_mutex};

    if (is_snapshot_stale(parent_snapshot))
      return {OpResult::STALE_SNAPSHOT, {}};

    {
      lock_guard lock{node->mutex};

      if (is_snapshot_stale(node_snapshot))
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
                    store_root(trimmed_node);

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
    static thread_local std::vector<NodeSplitInfo> failed_splitinfos;

    failed_splitinfos.clear();

    auto free_failed_splits = []() {
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
      BTREE_DEBUG_ASSERT(!is_snapshot_stale(leaf_snapshot));

      if constexpr (DoUpsert)
        std::tie(status, oldval) = ops.upsert(leaf, key, val);
      else
        status = ops.insert(leaf, key, val);

      leaf->mutex.unlock();
    } else {
      lock_guard lock{leaf->mutex};

      if (is_snapshot_stale(leaf_snapshot))
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
    lock_guard lock{leaf->mutex};

    if (is_snapshot_stale(leaf_snapshot))
      return {OpResult::STALE_SNAPSHOT, std::nullopt};

    return {OpResult::SUCCESS, ops.update_leaf(leaf, key, val)};
  }

  template <bool DoUpsert>
  auto insert_or_upsert(update_ops_t ops, const key_type &key,
                        const mapped_type &val) {
    static thread_local NodeSnapshotVector snapshots;

    snapshots.clear();
    ensure_root();

    while (true) {
      EpochGuard eg(this);
      bool is_leaf_locked = ops.get_leaf_containing(this, key, snapshots);

      BTREE_DEBUG_ASSERT(snapshots.size() > 1);

      if (auto res = insert_or_upsert_leaf<DoUpsert>(ops, snapshots,
                                                     is_leaf_locked, key, val);
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

    if (mergeinfo) {
      lock_guard parent_lock{parent->mutex};

      if (is_snapshot_stale(parent_snapshot))
        return;

      const key_type &merge_key = mergeinfo->merge_key;
      int sibilingpos = mergeinfo->sibilingpos;
      sibiling = static_cast<Node *>(parent->get_child(sibilingpos));

      lock_guard sibiling_lock{sibiling->mutex};
      lock_guard node_lock{node->mutex};

      if (is_snapshot_stale(node_snapshot))
        return;

      mergednode = sibiling->merge(node, merge_key);

      if (mergednode) {
        BTREE_UPDATE_STAT_NODE_BASED(merge);

        ops.update_inner_for_merge(parent, sibilingpos, mergednode);

        sibiling->setState(
            sibiling->getState().set_deleted().increment_version());
        node->setState(node->getState().set_deleted().increment_version());
      }
    }

    if (mergednode) {
      m_gc.retire_in_current_epoch(node_t::free, sibiling);
      m_gc.retire_in_current_epoch(node_t::free, node);
      m_gc.switch_epoch();
    }

    if (parent->isUnderfull())
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
        if (is_snapshot_stale(leaf_snapshot)) {
          ret = {OpResult::STALE_SNAPSHOT, std::nullopt};
          return;
        }

        auto [pos, key_present] = ops.lower_bound(leaf, key);

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
        lock_guard lock{leaf->mutex};

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
    DummyType dummy;

    bool is_leaf_locked = traverse_to_leaf<NO_FILL_SNAPSHOT_VECTOR>(
        [](node_t *current) { return ASINNER(current)->get_last_child(); },
        dummy, leaf_snapshot);

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
    DummyType dummy;

    bool is_leaf_locked = traverse_to_leaf<NO_FILL_SNAPSHOT_VECTOR>(
        [](node_t *current) { return ASINNER(current)->get_first_child(); },
        dummy, leaf_snapshot);

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
        auto [pos, key_present] = ops.lower_bound(leaf, key);

        std::optional<mapped_type> val =
            key_present
                ? std::optional<mapped_type>{leaf->get_key_value(pos)->second}
                : std::nullopt;

        if (is_snapshot_stale(leaf_snapshot)) {
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
    static thread_local NodeSnapshotVector snapshots;

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

  enum IteratorType { REVERSE, FORWARD };

public:
  concurrent_map() = default;
  concurrent_map(const concurrent_map &) = delete;
  concurrent_map(concurrent_map &&moved)
      : m_root_mutex(std::move(moved.m_root_mutex)),
        m_root_state(moved.m_root_state.load()), m_root(moved.m_root.load()),
        m_height(moved.m_height.load()), m_stats() {
    moved.m_root_state.store({});
    moved.m_root.store(nullptr);
    moved.m_height.store(0);
  }

  void reserve(size_t) {
    // No-op
  }

  DYNAMIC_KEY_ONLY
  bool Insert(const key_type &key, const mapped_type &val,
              const dynamic_cmp *cmp) {
    static_assert(is_dynamic == true);
    return insert_or_upsert<DO_INSERT>({cmp, m_stats.get()}, key, val);
  }

  STATIC_KEY_ONLY
  bool Insert(const key_type &key, const mapped_type &val) {
    static_assert(is_dynamic == false);
    return insert_or_upsert<DO_INSERT>({m_stats.get()}, key, val);
  }

  DYNAMIC_KEY_ONLY
  std::optional<mapped_type> Upsert(const key_type &key, const mapped_type &val,
                                    const dynamic_cmp *cmp) {
    static_assert(is_dynamic == true);
    return insert_or_upsert<DO_UPSERT>({cmp, m_stats.get()}, key, val);
  }

  STATIC_KEY_ONLY
  std::optional<mapped_type> Upsert(const key_type &key,
                                    const mapped_type &val) {
    static_assert(is_dynamic == false);
    return insert_or_upsert<DO_UPSERT>({m_stats.get()}, key, val);
  }

  DYNAMIC_KEY_ONLY
  std::optional<mapped_type> Update(const key_type &key, const mapped_type &val,
                                    const dynamic_cmp *cmp) {
    static_assert(is_dynamic == true);
    return update({cmp, m_stats.get()}, key, val);
  }

  STATIC_KEY_ONLY
  std::optional<mapped_type> Update(const key_type &key,
                                    const mapped_type &val) {
    static_assert(is_dynamic == false);
    return update({m_stats.get()}, key, val);
  }

  DYNAMIC_KEY_ONLY
  std::optional<mapped_type> Search(const key_type &key,
                                    const dynamic_cmp *cmp) {
    static_assert(is_dynamic == true);
    return search({cmp, m_stats.get()}, key);
  }

  template <typename KeyType, typename = std::enable_if_t<!is_dynamic>>
  std::optional<mapped_type> Search(const KeyType &key) {
    static_assert(is_dynamic == false);
    return search({m_stats.get()}, key);
  }

  DYNAMIC_KEY_ONLY
  std::optional<mapped_type> Delete(const key_type &key,
                                    const dynamic_cmp *cmp) {
    static_assert(is_dynamic == true);
    return remove({cmp, m_stats.get()}, key);
  }

  STATIC_KEY_ONLY
  std::optional<mapped_type> Delete(const key_type &key) {
    static_assert(is_dynamic == false);
    return remove({m_stats.get()}, key);
  }

  template <IteratorType IType> class iterator_impl {
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
    const concurrent_map *m_bt;
    const leaf_node_t *m_leaf;
    std::vector<int> m_slots;
    int m_curpos = 0;

    friend class concurrent_map;

    void increment() {
      if (++m_curpos >= static_cast<int>(m_slots.size())) {
        if (this->m_leaf->highkey) {
          m_leaf = ASLEAF(m_ops.get_next_leaf(
              m_bt, this->m_leaf->highkey.value(), m_slots));
          m_curpos = 0;
        } else {
          m_leaf = nullptr;
          m_curpos = 0;
          m_slots.clear();
        }
      }
    }

    void decrement() {
      if (--m_curpos < 0) {
        if (this->m_leaf->lowkey) {
          m_leaf = ASLEAF(
              m_ops.get_prev_leaf(m_bt, this->m_leaf->lowkey.value(), m_slots));
          m_curpos = m_leaf ? static_cast<int>(m_slots.size() - 1) : 0;
        } else {
          m_leaf = nullptr;
          m_curpos = 0;
          m_slots.clear();
        }
      }
    }

    pair_type *get_pair() const {
      return reinterpret_cast<pair_type *>(
          m_leaf->get_key_value_for_offset(m_slots[m_curpos]));
    }

  public:
    inline iterator_impl(def_search_ops_t ops, const concurrent_map *bt,
                         const leaf_node_t *leaf, std::vector<int> &&slots,
                         int curpos)
        : m_ops(ops), m_bt(bt), m_leaf(leaf), m_slots(std::move(slots)),
          m_curpos(curpos) {}

    inline iterator_impl(def_search_ops_t ops, const concurrent_map *bt,
                         const leaf_node_t *leaf, const std::vector<int> &slots,
                         int curpos)
        : m_ops(ops), m_bt(bt), m_leaf(leaf), m_slots(slots), m_curpos(curpos) {
    }

    inline iterator_impl(const iterator_impl &it) = default;

    template <IteratorType OtherIType>
    inline iterator_impl(const iterator_impl<OtherIType> &it)
        : iterator_impl(it.m_ops, it.m_bt, it.m_leaf, it.m_slots, it.m_curpos) {
    }

    inline reference operator*() const { return *get_pair(); }

    inline pointer operator->() const { return get_pair(); }

    inline key_type &key() const { return get_pair()->first; }

    inline data_type &data() const { return get_pair()->second; }

    inline iterator_impl operator++() {
      if constexpr (IType == IteratorType::FORWARD)
        increment();
      else
        decrement();

      return *this;
    }

    inline iterator_impl operator++(int) {
      auto copy = *this;

      if constexpr (IType == IteratorType::FORWARD)
        increment();
      else
        decrement();

      return copy;
    }

    inline iterator_impl operator--() {
      if constexpr (IType == IteratorType::FORWARD)
        decrement();
      else
        increment();

      return *this;
    }

    inline iterator_impl operator--(int) {
      auto copy = *this;

      if constexpr (IType == IteratorType::FORWARD)
        decrement();
      else
        increment();

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

  using const_iterator = iterator_impl<IteratorType::FORWARD>;
  using const_reverse_iterator = iterator_impl<IteratorType::REVERSE>;

private:
  inline const_iterator cbegin(def_search_ops_t ops) const {
    leaf_node_t *leaf;
    std::vector<int> slots;

    do {
      auto leaf_snapshot = get_first_leaf();
      leaf = ASLEAF(leaf_snapshot.node);

      if (leaf) {
        leaf->get_all_slots(slots);

        if (!is_snapshot_stale(leaf_snapshot))
          break;
      }
    } while (leaf);

    if (leaf && slots.empty())
      leaf = ops.get_next_leaf(this, leaf->highkey.value(), slots);

    return leaf ? const_iterator{ops, this, leaf, std::move(slots), 0} : cend();
  }

  inline const_iterator cend(def_search_ops_t ops) const {
    return {ops, this, nullptr, std::vector<int>{}, 0};
  }

  inline const_iterator begin(def_search_ops_t ops) const {
    return cbegin(ops);
  }

  inline const_iterator end(def_search_ops_t ops) const { return cend(ops); }

  inline const_reverse_iterator crbegin(def_search_ops_t ops) const {
    leaf_node_t *leaf;
    std::vector<int> slots;

    do {
      auto leaf_snapshot = get_last_leaf();
      leaf = ASLEAF(leaf_snapshot.node);

      if (leaf) {
        leaf->get_all_slots(slots);

        if (!is_snapshot_stale(leaf_snapshot))
          break;
      }
    } while (leaf);

    if (leaf && slots.empty())
      leaf = ops.get_prev_leaf(this, leaf->lowkey.value(), slots);

    return leaf ? const_reverse_iterator{ops, this, leaf, std::move(slots),
                                         static_cast<int>(slots.size() - 1)}
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
    auto leaf = kops.get_next_leaf(this, key, slots);

    return leaf ? const_iterator{ops, this, leaf, std::move(slots), 0} : end();
  }

  template <typename KeyType, typename KeyTypeOps, typename DefOps>
  inline const_iterator upper_bound(const KeyType &key, KeyTypeOps kops,
                                    DefOps ops) const {
    std::vector<int> slots;
    leaf_node_t *leaf;

    do {
      auto leaf_snapshot = kops.get_upper_bound_leaf(this, key);
      leaf = ASLEAF(leaf_snapshot.node);

      if (leaf) {
        kops.get_slots_greater_than(leaf, key, slots);

        if (!is_snapshot_stale(leaf_snapshot))
          break;
      }
    } while (leaf);

    bool node_empty = slots.empty();
    auto it = const_iterator{ops, this, leaf, std::move(slots), 0};

    if (leaf && node_empty)
      it++;

    return leaf ? it : end({m_stats.get()});
  }

public:
  STATIC_KEY_ONLY
  inline const_iterator cbegin() const {
    static_assert(is_dynamic == false);
    return cbegin({m_stats.get()});
  }
  DYNAMIC_KEY_ONLY
  inline const_iterator cbegin(const dynamic_cmp *cmp) const {
    static_assert(is_dynamic == true);
    return cbegin({cmp, m_stats.get()});
  }

  STATIC_KEY_ONLY
  inline const_iterator cend() const {
    static_assert(is_dynamic == false);
    return cend({m_stats.get()});
  }
  DYNAMIC_KEY_ONLY
  inline const_iterator cend(const dynamic_cmp *cmp) const {
    static_assert(is_dynamic == true);
    return cend({cmp, m_stats.get()});
  }

  STATIC_KEY_ONLY
  inline const_iterator begin() const {
    static_assert(is_dynamic == false);
    return begin({m_stats.get()});
  }
  DYNAMIC_KEY_ONLY
  inline const_iterator begin(const dynamic_cmp *cmp) const {
    static_assert(is_dynamic == true);
    return begin({cmp, m_stats.get()});
  }

  STATIC_KEY_ONLY
  inline const_iterator end() const {
    static_assert(is_dynamic == false);
    return end({m_stats.get()});
  }
  DYNAMIC_KEY_ONLY
  inline const_iterator end(const dynamic_cmp *cmp) const {
    static_assert(is_dynamic == true);
    return end({cmp, m_stats.get()});
  }

  STATIC_KEY_ONLY
  inline const_reverse_iterator crbegin() const {
    static_assert(is_dynamic == false);
    return crbegin({m_stats.get()});
  }
  DYNAMIC_KEY_ONLY
  inline const_reverse_iterator crbegin(const dynamic_cmp *cmp) const {
    static_assert(is_dynamic == true);
    return crbegin({cmp, m_stats.get()});
  }

  STATIC_KEY_ONLY
  inline const_reverse_iterator crend() const {
    static_assert(is_dynamic == false);
    return crend({m_stats.get()});
  }
  DYNAMIC_KEY_ONLY
  inline const_reverse_iterator crend(const dynamic_cmp *cmp) const {
    static_assert(is_dynamic == true);
    return crend({cmp, m_stats.get()});
  }

  STATIC_KEY_ONLY
  inline const_reverse_iterator rbegin() const {
    static_assert(is_dynamic == false);
    return rbegin({m_stats.get()});
  }
  DYNAMIC_KEY_ONLY
  inline const_reverse_iterator rbegin(const dynamic_cmp *cmp) const {
    static_assert(is_dynamic == true);
    return rbegin({cmp, m_stats.get()});
  }

  STATIC_KEY_ONLY
  inline const_reverse_iterator rend() const {
    static_assert(is_dynamic == false);
    return rend({m_stats.get()});
  }
  DYNAMIC_KEY_ONLY
  inline const_reverse_iterator rend(const dynamic_cmp *cmp) const {
    static_assert(is_dynamic == true);
    return rend({cmp, m_stats.get()});
  }

  template <typename KeyType, typename = std::enable_if_t<!is_dynamic>>
  inline const_iterator lower_bound(const KeyType &key) const {
    static_assert(is_dynamic == false);
    return lower_bound(key, search_ops_t<KeyType>{m_stats.get()},
                       def_search_ops_t{m_stats.get()});
  }
  DYNAMIC_KEY_ONLY
  inline const_iterator lower_bound(const key_type &key,
                                    const dynamic_cmp *cmp) const {
    static_assert(is_dynamic == true);
    auto ops = def_search_ops_t{cmp, m_stats.get()};
    return lower_bound(key, ops, ops);
  }

  template <typename KeyType, typename = std::enable_if_t<!is_dynamic>>
  inline const_iterator upper_bound(const KeyType &key) const {
    static_assert(is_dynamic == false);
    return upper_bound(key, search_ops_t<KeyType>{m_stats.get()},
                       def_search_ops_t{m_stats.get()});
  }
  DYNAMIC_KEY_ONLY
  inline const_iterator upper_bound(const key_type &key,
                                    const dynamic_cmp *cmp) const {
    static_assert(is_dynamic == true);
    auto ops = def_search_ops_t{cmp, m_stats.get()};
    return upper_bound(key, ops, ops);
  }

  inline int height() const { return m_height; }

  inline void reclaim_all() { m_gc.reclaim_all(); }

  template <typename Dummy = void,
            typename = std::enable_if_t<Traits::STAT, Dummy>>
  inline std::size_t size() const {
    return m_stats->num_elements;
  }

  template <typename Dummy = void,
            typename = std::enable_if_t<Traits::STAT, Dummy>>
  inline bool empty() const {
    return size() == 0;
  }

  template <typename Dummy = void,
            typename = typename std::enable_if_t<Traits::STAT, Dummy>>
  inline const Stats &stats() const {
    return m_stats;
  }

  ~concurrent_map() {
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
