// include/btree/concurrent_map.h
// B+Tree implementation

#pragma once

#include "common.h"
#include "utils/EpochManager.h"

#include <atomic>
#include <bitset>
#include <chrono>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace btree
{
template <typename Key,
          typename Value,
          typename Comparator,
          typename Traits = btree_traits_default,
          typename Stats  = std::conditional_t<Traits::STAT, btree_stats_t, btree_empty_stats_t>>
class concurrent_map
{
public:
	using key_type        = Key;
	using mapped_type     = Value;
	using value_type      = std::pair<const Key, Value>;
	using size_type       = std::size_t;
	using difference_type = std::ptrdiff_t;
	using reference       = value_type &;
	using const_reference = const value_type &;

private:
	static constexpr auto compare = Comparator{};

	static inline bool
	key_less(const Key &k1, const Key &k2)
	{
		return compare(k1, k2) < 0;
	}

	static inline bool
	key_greater(const Key &k1, const Key &k2)
	{
		return compare(k1, k2) > 0;
	}

	static inline bool
	key_equal(const Key &k1, const Key &k2)
	{
		return compare(k1, k2) == 0;
	}

	template <typename T>
	static inline void
	store_relaxed(std::atomic<T> &atomicvar, T newval)
	{
		atomicvar.store(newval, std::memory_order_relaxed);
	}

	template <typename T>
	static inline T
	load_relaxed(const std::atomic<T> &atomicvar)
	{
		return atomicvar.load();
	}

	using MutexLockType = std::lock_guard<std::mutex>;

	enum class NodeType : int8_t
	{
		LEAF,
		INNER
	};

	class nodestate_t
	{
		static constexpr int IS_LOCKED_BIT  = 62;
		static constexpr int IS_DELETED_BIT = 63;

		using bitset = std::bitset<64>;

		bitset bits = {};

	public:
		inline bool
		operator==(const nodestate_t &other) const
		{
			return bits == other.bits;
		}

		inline bool
		operator!=(const nodestate_t &other) const
		{
			return bits != other.bits;
		}

		inline uint64_t
		version() const
		{
			bitset t = bits;

			return t.reset(IS_LOCKED_BIT).reset(IS_DELETED_BIT).to_ullong();
		}

		inline bool
		is_locked() const
		{
			return bits.test(IS_LOCKED_BIT);
		}

		inline bool
		is_deleted() const
		{
			return bits.test(IS_DELETED_BIT);
		}

		inline nodestate_t
		increment_version()
		{
			bitset version_only = version() + 1;
			bitset zero_version = {};

			zero_version.set(IS_LOCKED_BIT).set(IS_DELETED_BIT);

			bits &= zero_version;
			bits |= version_only;
			return *this;
		}

		inline nodestate_t
		set_locked()
		{
			bits.set(IS_LOCKED_BIT);
			return *this;
		}

		inline nodestate_t
		set_deleted()
		{
			bits.set(IS_DELETED_BIT);
			return *this;
		}

		inline nodestate_t
		reset_locked()
		{
			bits.reset(IS_LOCKED_BIT);
			return *this;
		}

		inline nodestate_t
		reset_deleted()
		{
			bits.reset(IS_DELETED_BIT);
			return *this;
		}
	};

	struct node_t;

	struct NodeSplitInfo
	{
		node_t *left;
		node_t *right;
		const Key *split_key;
	};

	struct node_t
	{
		std::atomic<nodestate_t> state = {};
		std::atomic_int num_values     = 0;

		// max_slot_offset is required to guard readers from concurrent updaters.
		// concurrent updaters might overwrite slots which can be read by readers,
		// with key/value data.

		std::atomic_int logical_pagesize = 0;
		std::atomic_int next_slot_offset = 0;
		std::atomic_int max_slot_offset  = 0;
		int last_value_offset            = Traits::NODE_SIZE;

		std::atomic_int8_t num_dead_values = 0;
		const NodeType node_type;
		const int height;

		const std::optional<Key> lowkey;
		const std::optional<Key> highkey;

		std::mutex mutex;

		inline node_t(NodeType ntype,
		              int initialsize,
		              int a_height,
		              const std::optional<Key> &a_lowkey,
		              const std::optional<Key> &a_highkey)
		    : next_slot_offset(initialsize)
		    , node_type(ntype)
		    , height(a_height)
		    , lowkey(a_lowkey)
		    , highkey(a_highkey)
		{}

		inline bool
		isLeaf() const
		{
			return node_type == NodeType::LEAF;
		}

		inline bool
		isInner() const
		{
			return node_type == NodeType::INNER;
		}

		inline nodestate_t
		getState() const
		{
			return state;
		}

		inline void
		setState(nodestate_t new_state)
		{
			state = new_state;
		}

		// We only need to find out if we have more than two deleted values, to trim
		inline void
		incrementNumDeadValues()
		{
			int8_t num_dead_values = load_relaxed(this->num_dead_values);

			store_relaxed<int8_t>(this->num_dead_values,
			                      num_dead_values > 1 ? num_dead_values : num_dead_values + 1);
		}

		inline bool
		isUnderfull() const
		{
			BTREE_DEBUG_ASSERT(logical_pagesize <= Traits::NODE_SIZE);

			return (logical_pagesize * 100) / Traits::NODE_SIZE < Traits::NODE_MERGE_THRESHOLD;
		}

		inline char *
		opaque() const
		{
			return reinterpret_cast<char *>(reinterpret_cast<intptr_t>(this));
		}

		inline std::atomic_int *
		get_slots() const
		{
			return reinterpret_cast<std::atomic_int *>(opaque() + sizeof(node_t));
		}

		inline bool
		canTrim() const
		{
			return load_relaxed(num_dead_values) > 1;
		}

		inline bool
		canSplit() const
		{
			return num_values > 2;
		}

		static inline void
		free(node_t *node)
		{
			if (node->isLeaf())
				delete ASLEAF(node);
			else
				delete ASINNER(node);
		}

		inline const Key &
		get_first_key() const
		{
			if (isInner())
				return static_cast<const inner_node_t *>(this)->get_first_key();
			else
				return static_cast<const leaf_node_t *>(this)->get_first_key();
		}
	};

	enum class InsertStatus
	{
		OVERFLOW,
		DUPLICATE,
		INSERTED
	};

	template <typename ValueType, NodeType NType>
	struct inherited_node_t : node_t
	{
		using value_t     = ValueType;
		using key_value_t = std::pair<Key, value_t>;

		static constexpr enum NodeType NODETYPE = NType;

		static constexpr bool
		IsLeaf()
		{
			return NType == NodeType::LEAF;
		}

		static constexpr bool
		IsInner()
		{
			return NType == NodeType::INNER;
		}

		inline inherited_node_t(const std::optional<Key> &lowkey,
		                        const std::optional<Key> &highkey,
		                        int height)
		    : node_t(NType, sizeof(inherited_node_t), height, lowkey, highkey)
		{}

		inline ~inherited_node_t()
		{
			for (int slot = IsInner() ? 1 : 0; slot < this->num_values; slot++)
			{
				get_key_value(slot)->~key_value_t();
			}
		}

		static inherited_node_t *
		alloc(const std::optional<Key> &lowkey, const std::optional<Key> &highkey, int height)
		{
			return new (new char[Traits::NODE_SIZE]) inherited_node_t(lowkey, highkey, height);
		}

		static void
		free(inherited_node_t *node)
		{
			delete node;
		}

		// Must be called with both this's and other's mutex held
		inline bool
		haveEnoughSpace() const
		{
			int next_slot_offset = load_relaxed(this->next_slot_offset);
			int max_slot_offset  = load_relaxed(this->max_slot_offset);

			return ((next_slot_offset + sizeof(int))
			        <= (this->last_value_offset - sizeof(key_value_t)))
			       && (max_slot_offset
			           <= static_cast<int>(this->last_value_offset - sizeof(key_value_t)));
		}

		// Must be called with both this's and other's mutex held
		inline bool
		canMerge(const node_t *other) const
		{
			int logical_pagesize       = load_relaxed(this->logical_pagesize);
			int other_logical_pagesize = load_relaxed(other->logical_pagesize);

			return (logical_pagesize + other_logical_pagesize
			        + (IsInner() ? sizeof(key_value_t) : 0))
			           + sizeof(inherited_node_t)
			       <= Traits::NODE_SIZE;
		}

		inline key_value_t *
		get_key_value_for_offset(int offset) const
		{
			return reinterpret_cast<key_value_t *>(this->opaque() + offset);
		}

		inline key_value_t *
		get_key_value(int slot) const
		{
			const std::atomic_int *slots = this->get_slots();

			return get_key_value_for_offset(slots[slot].load(std::memory_order_relaxed));
		}

		inline const Key &
		get_key(int slot) const
		{
			if constexpr (IsInner())
				BTREE_DEBUG_ASSERT(slot != 0);

			return get_key_value(slot)->first;
		}

		INNER_ONLY
		inline std::atomic<value_t> *
		get_child_ptr(int slot) const
		{
			return slot == 0
			           ? reinterpret_cast<std::atomic<value_t> *>(&get_key_value(0)->first)
			           : reinterpret_cast<std::atomic<value_t> *>(&get_key_value(slot)->second);
		}

		INNER_ONLY
		inline value_t
		get_child(int slot) const
		{
			return get_child_ptr(slot)->load(std::memory_order_relaxed);
		}

		inline const Key &
		get_first_key() const
		{
			if constexpr (IsInner())
				return get_key_value(1)->first;
			else
				return get_key_value(0)->first;
		}

		INNER_ONLY
		inline value_t
		get_first_child() const
		{
			return get_child_ptr(0)->load();
		}

		INNER_ONLY
		inline value_t
		get_last_child() const
		{
			int slot = this->num_values - 1;

			return get_child(slot);
		}

		// Node search helpers

		int
		lower_bound_pos(const Key &key, int num_values) const
		{
			int firstslot          = IsLeaf() ? 0 : 1;
			std::atomic_int *slots = this->get_slots();

			return std::lower_bound(slots + firstslot,
			                        slots + num_values,
			                        key,
			                        [this](int slot, const Key &key) {
				                        return key_less(get_key_value_for_offset(slot)->first, key);
			                        })
			       - slots;
		}

		int
		upper_bound_pos(const Key &key, int num_values) const
		{
			int firstslot          = IsLeaf() ? 0 : 1;
			std::atomic_int *slots = this->get_slots();
			int pos =
			    std::upper_bound(slots + firstslot,
			                     slots + num_values,
			                     key,
			                     [this](const Key &key, int slot) {
				                     return key_less(key, get_key_value_for_offset(slot)->first);
			                     })
			    - slots;

			return IsInner() ? std::min(pos - 1, num_values - 1) : pos;
		}

		std::pair<int, bool>
		lower_bound(const Key &key) const
		{
			int num_values = this->num_values;
			int pos        = lower_bound_pos(key, num_values);
			bool present   = pos < num_values && key_equal(get_key_value(pos)->first, key);

			return std::make_pair(pos, present);
		}

		INNER_ONLY
		int
		search_inner(const Key &key) const
		{
			bool key_present;
			int pos;

			std::tie(pos, key_present) = lower_bound(key);

			return !key_present ? pos - 1 : pos;
		}

		INNER_ONLY
		value_t
		get_value_lower_than(const Key &key) const
		{
			int pos = search_inner(key);

			if (pos == 0)
				return get_first_child();

			return get_child(key_equal(key, get_key(pos)) ? pos - 1 : pos);
		}

		INNER_ONLY
		inline value_t
		get_child_for_key(const Key &key) const
		{
			return get_child(search_inner(key));
		}

		// Node update helpers
		// Must be called with this's mutex held

		template <typename Updater>
		inline void
		atomic_node_update(Updater update)
		{
			this->setState(this->getState().set_locked());
			update();
			this->setState(this->getState().reset_locked().increment_version());
		}

		// Must be called with this's mutex held
		inline void
		update_meta_after_insert()
		{
			int next_slot_offset = load_relaxed(this->next_slot_offset) + sizeof(int);
			int logical_pagesize =
			    load_relaxed(this->logical_pagesize) + sizeof(key_value_t) + sizeof(int);
			int max_slot_offset = std::max(load_relaxed(this->max_slot_offset), next_slot_offset);

			this->last_value_offset -= sizeof(key_value_t);
			store_relaxed(this->next_slot_offset, next_slot_offset);
			store_relaxed(this->logical_pagesize, logical_pagesize);
			store_relaxed(this->max_slot_offset, max_slot_offset);

			BTREE_DEBUG_ASSERT(next_slot_offset <= this->last_value_offset);
		}

		// Must be called with this's mutex held
		static void
		copy_backward(std::atomic_int *slots, int start_pos, int end_pos, int out_end_pos)
		{
			BTREE_DEBUG_ASSERT(out_end_pos >= end_pos);

			while (start_pos < end_pos)
			{
				slots[--out_end_pos].store(slots[--end_pos], std::memory_order_release);
			}
		}

		// Must be called with this's mutex held
		static void
		copy(std::atomic_int *slots, int start_pos, int end_pos, int out_pos)
		{
			BTREE_DEBUG_ASSERT(out_pos < start_pos);

			while (start_pos < end_pos)
			{
				slots[out_pos++].store(slots[start_pos++], std::memory_order_release);
			}
		}

		// Must not be called on a reachable node
		INNER_ONLY
		inline void
		insert_neg_infinity(const value_t &val)
		{
			int num_values = this->num_values;
			BTREE_DEBUG_ASSERT(this->isInner() && num_values == 0);

			std::atomic_int *slots   = this->get_slots();
			int current_value_offset = this->last_value_offset - sizeof(value_t);

			new (this->opaque() + current_value_offset) value_t{ val };
			slots[0].store(current_value_offset, std::memory_order_relaxed);

			this->num_values.store(num_values + 1, std::memory_order_relaxed);
			update_meta_after_insert();
		}

		// Must not be called on a reachable node
		inline void
		append(const Key &key, const value_t &val)
		{
			std::atomic_int *slots   = this->get_slots();
			int current_value_offset = this->last_value_offset - sizeof(key_value_t);
			int pos                  = this->num_values;
			int num_values           = this->num_values;

			new (this->opaque() + current_value_offset) key_value_t{ key, val };
			slots[pos].store(current_value_offset, std::memory_order_relaxed);

			this->num_values.store(num_values + 1, std::memory_order_relaxed);
			update_meta_after_insert();
		}

		// Must be called with this's mutex held and node state set as locked
		inline void
		insert_into_slot(int pos, int value_offset)
		{
			int num_values         = this->num_values;
			std::atomic_int *slots = this->get_slots();

			copy_backward(slots, pos, num_values, num_values + 1);
			slots[pos].store(value_offset, std::memory_order_release);
			this->num_values.store(num_values + 1, std::memory_order_release);
		}

		// Must be called with this's mutex held
		inline InsertStatus
		insert_into_pos(const Key &key, const Value &val, int pos)
		{
			if (this->haveEnoughSpace())
			{
				int current_value_offset = this->last_value_offset - sizeof(key_value_t);

				new (this->opaque() + current_value_offset) key_value_t{ key, val };

				atomic_node_update([&]() { insert_into_slot(pos, current_value_offset); });
				update_meta_after_insert();

				return InsertStatus::INSERTED;
			}

			return InsertStatus::OVERFLOW;
		}

		// Must be called with this's mutex held
		InsertStatus
		insert(const Key &key, const Value &val)
		{
			bool key_present = false;
			int pos          = 0;

			if (this->num_values)
			{
				std::tie(pos, key_present) = lower_bound(key);

				if (key_present)
					return InsertStatus::DUPLICATE;
			}

			return insert_into_pos(key, val, pos);
		}

		// Must be called with this's mutex held
		LEAF_ONLY
		std::pair<InsertStatus, std::optional<value_t>>
		upsert(const Key &key, const Value &val)
		{
			bool key_present              = false;
			int pos                       = 0;
			std::optional<value_t> oldval = std::nullopt;

			if (this->num_values)
			{
				std::tie(pos, key_present) = lower_bound(key);

				if (key_present)
				{
					value_t *oldval_ptr = &get_key_value(pos)->second;

					oldval = *oldval_ptr;
					atomic_node_update([&]() { *oldval_ptr = val; });

					return { InsertStatus::DUPLICATE, oldval };
				}
			}

			return { insert_into_pos(key, val, pos), oldval };
		}

		// Must be called with this's mutex held
		void
		remove_pos(int pos)
		{
			std::atomic_int *slots = this->get_slots();

			atomic_node_update([&]() {
				int num_values = this->num_values;

				copy(slots, pos + 1, num_values, pos);
				this->num_values.store(num_values - 1, std::memory_order_release);
			});

			int next_slot_offset = load_relaxed(this->next_slot_offset) - sizeof(int);
			int logical_pagesize =
			    load_relaxed(this->logical_pagesize) - (sizeof(key_value_t) + sizeof(int));

			this->incrementNumDeadValues();
			store_relaxed(this->next_slot_offset, next_slot_offset);
			store_relaxed(this->logical_pagesize, logical_pagesize);
		}

		// Must be called with this's mutex held
		LEAF_ONLY
		inline std::optional<value_t>
		update_leaf(const Key &key, const value_t &new_value)
		{
			int pos;
			bool found;
			std::optional<value_t> old_value = std::nullopt;

			std::tie(pos, found) = lower_bound(key);

			if (found)
			{
				value_t *oldval_ptr = &get_key_value(pos)->second;

				old_value = *oldval_ptr;
				atomic_node_update([&]() { *oldval_ptr = new_value; });
			}
			else
			{
				return {};
			}

			return old_value;
		}

		// Must be called with this's mutex held
		INNER_ONLY inline void
		update_inner_for_trim(const Key &key, value_t child)
		{
			int pos = search_inner(key);

			std::atomic<value_t> *oldchild = get_child_ptr(pos);

			atomic_node_update([&]() { oldchild->store(child, std::memory_order_release); });
		}

		// Must be called with this's mutex held
		INNER_ONLY
		inline InsertStatus
		update_inner_for_split(const NodeSplitInfo &splitinfo)
		{
			value_t left_child   = splitinfo.left;
			value_t right_child  = splitinfo.right;
			const Key &split_key = *splitinfo.split_key;

			if (this->haveEnoughSpace())
			{
				int split_pos;
				bool found;
				int current_value_offset = this->last_value_offset - sizeof(key_value_t);

				std::tie(split_pos, found) = lower_bound(split_key);

				BTREE_DEBUG_ASSERT(found == false);
				BTREE_DEBUG_ONLY(found);

				std::atomic<value_t> *old_child = get_child_ptr(split_pos - 1);

				new (this->opaque() + current_value_offset) key_value_t{ split_key, right_child };

				atomic_node_update([&]() {
					old_child->store(left_child, std::memory_order_release);
					insert_into_slot(split_pos, current_value_offset);
				});

				update_meta_after_insert();

				return InsertStatus::INSERTED;
			}

			return InsertStatus::OVERFLOW;
		}

		// Must be called with this's mutex held
		INNER_ONLY
		inline void
		update_inner_for_merge(int merged_pos, value_t merged_child)
		{
			std::atomic_int *slots          = this->get_slots();
			int deleted_pos                 = merged_pos + 1;
			std::atomic<value_t> *old_child = get_child_ptr(merged_pos);

			atomic_node_update([&]() {
				int num_values = this->num_values;

				copy(slots, deleted_pos + 1, num_values, deleted_pos);
				this->num_values.store(num_values - 1, std::memory_order_release);

				old_child->store(merged_child, std::memory_order_release);
			});

			this->incrementNumDeadValues();
			this->next_slot_offset -= sizeof(int);
			this->logical_pagesize -= sizeof(key_value_t) + sizeof(int);
		}

		// Must not be called on a reachable node
		inline void
		copy_from(const inherited_node_t *src, int start_pos, int end_pos)
		{
			for (int slot = start_pos; slot < end_pos; slot++)
			{
				const key_value_t *val = src->get_key_value(slot);

				this->append(val->first, val->second);
			}
		}

		// SMO helpers
		// Must be called with this's mutex held

		inherited_node_t *
		trim() const
		{
			inherited_node_t *new_node = alloc(this->lowkey, this->highkey, this->height);

			if constexpr (IsLeaf())
			{
				new_node->copy_from(this, 0, this->num_values);
			}
			else
			{
				new_node->insert_neg_infinity(get_first_child());
				new_node->copy_from(this, 1, this->num_values);
			}

			return new_node;
		}

		// Must be called with this's mutex held
		NodeSplitInfo
		split() const
		{
			BTREE_DEBUG_ASSERT(this->canSplit());

			int split_pos           = IsInner() ? this->num_values / 2 : (this->num_values + 1) / 2;
			const Key &split_key    = get_key(split_pos);
			inherited_node_t *left  = alloc(this->lowkey, split_key, this->height);
			inherited_node_t *right = alloc(split_key, this->highkey, this->height);

			if constexpr (IsInner())
			{
				left->insert_neg_infinity(get_first_child());
				left->copy_from(this, 1, split_pos);

				right->insert_neg_infinity(get_child(split_pos));
				right->copy_from(this, split_pos + 1, this->num_values);
			}
			else
			{
				left->copy_from(this, 0, split_pos);
				right->copy_from(this, split_pos, this->num_values);
			}

			BTREE_DEBUG_ASSERT(left->lowkey || left->highkey);
			BTREE_DEBUG_ASSERT(right->lowkey || right->highkey);

			return { left, right, &split_key };
		}

		// Must be called with this's mutex held
		inherited_node_t *
		merge(const inherited_node_t *other, const Key &merge_key) const
		{
			inherited_node_t *mergednode = nullptr;

			if (this->canMerge(other))
			{
				mergednode = alloc(this->lowkey, other->highkey, this->height);

				if constexpr (IsInner())
				{
					mergednode->insert_neg_infinity(get_first_child());
					mergednode->copy_from(this, 1, this->num_values);

					mergednode->append(merge_key, other->get_first_child());
					mergednode->copy_from(other, 1, other->num_values);
				}
				else
				{
					(void) merge_key;

					mergednode->copy_from(this, 0, this->num_values);
					mergednode->copy_from(other, 0, other->num_values);
				}

				BTREE_DEBUG_ASSERT(mergednode->num_values == this->num_values + other->num_values);
			}

			return mergednode;
		}

		LEAF_ONLY
		void
		get_slots_greater_than(const Key &key, std::vector<int> &slot_offsets) const
		{
			int num_values         = this->num_values;
			int pos                = upper_bound_pos(key, num_values);
			std::atomic_int *slots = this->get_slots();

			slot_offsets.clear();

			for (int i = pos; i < num_values; i++)
			{
				slot_offsets.emplace_back(slots[i].load(std::memory_order_relaxed));
			}
		}

		LEAF_ONLY
		void
		get_slots_greater_than_eq(const Key &key, std::vector<int> &slot_offsets) const
		{
			int num_values         = this->num_values;
			int pos                = lower_bound_pos(key, num_values);
			std::atomic_int *slots = this->get_slots();

			slot_offsets.clear();

			for (int i = pos; i < num_values; i++)
			{
				slot_offsets.emplace_back(slots[i].load(std::memory_order_relaxed));
			}
		}

		LEAF_ONLY
		void
		get_all_slots(std::vector<int> &slot_offsets) const
		{
			int num_values         = this->num_values;
			std::atomic_int *slots = this->get_slots();

			for (int i = 0; i < num_values; i++)
			{
				slot_offsets.emplace_back(slots[i].load(std::memory_order_relaxed));
			}
		}

		LEAF_ONLY
		void
		get_slots_less_than(const Key &key, std::vector<int> &slot_offsets) const
		{
			std::atomic_int *slots = this->get_slots();
			bool found;
			int pos;

			std::tie(pos, found) = lower_bound(key);
			pos                  = found ? pos - 1 : pos;

			slot_offsets.clear();

			for (int i = 0; i < pos; i++)
			{
				slot_offsets.emplace_back(slots[i].load(std::memory_order_relaxed));
			}
		}

		// Must be called only from destructor or debugging routines
		// Locking doesn't matter
		template <typename Cont,
		          typename Dummy = void,
		          typename       = std::enable_if_t<IsInner(), Dummy>>
		void
		get_children(Cont &nodes) const
		{
			nodes.emplace_back(get_first_child());

			for (int slot = 1; slot < this->num_values; slot++)
			{
				nodes.emplace_back(get_child(slot));
			}
		}

		NODE_DUMP_METHODS
	};

	using leaf_node_t  = inherited_node_t<Value, NodeType::LEAF>;
	using inner_node_t = inherited_node_t<node_t *, NodeType::INNER>;

	static_assert((Traits::NODE_SIZE - sizeof(leaf_node_t))
	                      / (sizeof(typename leaf_node_t::key_value_t) + sizeof(int))
	                  >= 4,
	              "Btree leaf node must have atleast 4 slots");
	static_assert((Traits::NODE_SIZE - sizeof(inner_node_t))
	                      / (sizeof(typename inner_node_t::key_value_t) + sizeof(int))
	                  >= 4,
	              "Btree inner node must have atleast 4 slots");
	static_assert(Traits::NODE_SIZE % alignof(typename leaf_node_t::key_value_t) == 0,
	              "Alignment mismatch b/w pagesize and Key, Value");
	static_assert(Traits::NODE_SIZE % alignof(typename inner_node_t::key_value_t) == 0,
	              "Alignment mismatch b/w pagesize and Key, Value");
	static_assert(sizeof(leaf_node_t) % alignof(typename leaf_node_t::key_value_t) == 0,
	              "Alignment mismatch b/w pagesize and Key, Value");
	static_assert(sizeof(inner_node_t) % alignof(typename inner_node_t::key_value_t) == 0,
	              "Alignment mismatch b/w pagesize and Key, Value");

	static constexpr int MAXHEIGHT = 32;

	std::unique_ptr<std::mutex> m_root_mutex = std::make_unique<std::mutex>();
	std::atomic<nodestate_t> m_root_state    = {};
	std::atomic<node_t *> m_root             = nullptr;

	std::atomic_int m_height       = 0;
	std::unique_ptr<Stats> m_stats = std::make_unique<Stats>();

	utils::EpochManager<uint64_t, node_t> m_gc;

	struct EpochGuard
	{
		concurrent_map *map;

		EpochGuard(concurrent_map *a_map) : map(a_map)
		{
			map->m_gc.enter_epoch();
		}

		~EpochGuard()
		{
			map->m_gc.exit_epoch();
		}
	};

	struct NodeSnapshot
	{
		node_t *node;
		nodestate_t state;
	};

	using NodeSnapshotVector = std::vector<NodeSnapshot>;

	struct DummyType
	{};

	static constexpr bool DO_UPSERT               = true;
	static constexpr bool DO_INSERT               = false;
	static constexpr bool OPTIMISTIC_LOCKING      = true;
	static constexpr bool PESSIMISTIC_LOCKING     = false;
	static constexpr bool FILL_SNAPSHOT_VECTOR    = true;
	static constexpr bool NO_FILL_SNAPSHOT_VECTOR = false;
	static constexpr int OPTIMISTIC_TRY_COUNT     = 3;

	enum class OpResult
	{
		SUCCESS,
		FAILURE,
		STALE_SNAPSHOT
	};

	inline void
	try_lock_pessimistic(node_t *node, nodestate_t &state) const
	{
		BTREE_UPDATE_STAT(pessimistic_read, ++);

		if (node)
		{
			node->mutex.lock();

			state = node->getState();

			if (state.is_deleted())
				node->mutex.unlock();
		}
		else
		{
			m_root_mutex->lock();

			state = m_root_state;

			if (state.is_deleted())
				m_root_mutex->unlock();
		}
	}

	inline bool
	try_lock_optimistic(node_t *node, nodestate_t &state) const
	{
		int num_tries = 0;

		do
		{
			state = node ? node->getState() : m_root_state.load();

			if (!state.is_locked())
				return true;

			std::this_thread::sleep_for(std::chrono::nanoseconds(300));
			num_tries++;
		} while (num_tries < OPTIMISTIC_TRY_COUNT);

		return false;
	}

	template <bool UseOptimisticLocking>
	inline bool
	lock_node_or_restart(node_t *node, nodestate_t &state) const
	{
		if constexpr (UseOptimisticLocking)
		{
			if (!try_lock_optimistic(node, state))
			{
				BTREE_UPDATE_STAT(optimistic_fail, ++);
				try_lock_pessimistic(node, state);

				if (!state.is_deleted())
					node->mutex.unlock();
			}
		}
		else
		{
			try_lock_pessimistic(node, state);
		}

		return state.is_deleted();
	}

	template <bool UseOptimisticLocking>
	inline bool
	unlock_node_or_restart(node_t *node, nodestate_t &state) const
	{
		if constexpr (UseOptimisticLocking)
		{
			if (node)
				return state != node->getState();
			else
				return state != m_root_state;
		}
		else
		{
			if (node)
				node->mutex.unlock();
			else
				m_root_mutex->unlock();
		}

		return false;
	}

	template <
	    bool UseOptimisticLocking,
	    bool FillSnapshotVector,
	    typename GetChild,
	    typename SnapshotVectorType =
	        std::conditional_t<FillSnapshotVector, NodeSnapshotVector, DummyType>,
	    typename NodeSnaphotType = std::conditional_t<FillSnapshotVector, DummyType, NodeSnapshot>>
	OpResult
	traverse(const GetChild &get_children,
	         SnapshotVectorType &nss_vec,
	         NodeSnaphotType &leaf_snapshot) const
	{
		node_t *parent            = nullptr;
		node_t *current           = nullptr;
		nodestate_t parent_state  = {};
		nodestate_t current_state = {};

		if constexpr (FillSnapshotVector)
			nss_vec.clear();

		if (lock_node_or_restart<UseOptimisticLocking>(nullptr, parent_state))
			return OpResult::STALE_SNAPSHOT;

		if constexpr (FillSnapshotVector)
			nss_vec.push_back({ nullptr, parent_state });

		for (current = m_root; current && current->isInner();)
		{
			if (lock_node_or_restart<UseOptimisticLocking>(current, current_state)
			    || unlock_node_or_restart<UseOptimisticLocking>(parent, parent_state))
			{
				return OpResult::STALE_SNAPSHOT;
			}

			if constexpr (FillSnapshotVector)
				nss_vec.push_back({ current, current_state });

			parent       = current;
			parent_state = current_state;
			current      = get_children(ASINNER(current));

			if (is_snapshot_stale({ parent, parent_state }))
				return OpResult::STALE_SNAPSHOT;
		}

		if ((current && lock_node_or_restart<UseOptimisticLocking>(current, current_state))
		    || unlock_node_or_restart<UseOptimisticLocking>(parent, parent_state))
		{
			return OpResult::STALE_SNAPSHOT;
		}

		if (current)
		{
			if constexpr (FillSnapshotVector)
				nss_vec.push_back({ current, current_state });
			else
				leaf_snapshot = { current, current_state };
		}

		return OpResult::SUCCESS;
	}

	template <
	    bool FillSnapshotVector,
	    typename GetChild,
	    int MaxRestarts = 2,
	    typename SnapshotVectorType =
	        std::conditional_t<FillSnapshotVector, NodeSnapshotVector, DummyType>,
	    typename NodeSnaphotType = std::conditional_t<FillSnapshotVector, DummyType, NodeSnapshot>>
	bool
	traverse_to_leaf(GetChild &&get_child,
	                 SnapshotVectorType &nss_vec,
	                 NodeSnaphotType &leaf_snapshot) const
	{
		int restart_count = 0;
		NodeSnapshot parent_snapshot;
		NodeSnapshot child_snapshot;
		OpResult opt_traversal_res = OpResult::FAILURE;

		do
		{
			opt_traversal_res =
			    traverse<OPTIMISTIC_LOCKING, FillSnapshotVector>(get_child, nss_vec, leaf_snapshot);
			restart_count++;
		} while (opt_traversal_res != OpResult::SUCCESS && restart_count < MaxRestarts);

		if (opt_traversal_res != OpResult::SUCCESS)
		{
			auto res = traverse<PESSIMISTIC_LOCKING, FillSnapshotVector>(get_child,
			                                                             nss_vec,
			                                                             leaf_snapshot);

			BTREE_DEBUG_ASSERT(res == OpResult::SUCCESS);
			BTREE_DEBUG_ONLY(res);
		}

		return opt_traversal_res != OpResult::SUCCESS;
	}

	bool
	get_leaf_containing(const Key &key, NodeSnapshotVector &nss_vec) const
	{
		DummyType dummy;

		bool is_leaf_locked = traverse_to_leaf<FILL_SNAPSHOT_VECTOR>(
		    [&key](node_t *current) { return ASINNER(current)->get_child_for_key(key); },
		    nss_vec,
		    dummy);

		BTREE_DEBUG_ASSERT(nss_vec.size() > 0);

		if (nss_vec.size() > 1)
			BTREE_DEBUG_ASSERT(nss_vec.back().node->isLeaf());

		return nss_vec.size() > 1 && is_leaf_locked;
	}

	NodeSnapshot
	get_leaf_containing(const Key &key) const
	{
		NodeSnapshot leaf_snapshot{};
		DummyType dummy;

		bool is_leaf_locked = traverse_to_leaf<NO_FILL_SNAPSHOT_VECTOR>(
		    [&key](node_t *current) { return ASINNER(current)->get_child_for_key(key); },
		    dummy,
		    leaf_snapshot);

		if (is_leaf_locked)
		{
			BTREE_DEBUG_ASSERT(leaf_snapshot.node != nullptr);
			leaf_snapshot.node->mutex.unlock();
		}

		if (leaf_snapshot.node)
			BTREE_DEBUG_ASSERT(leaf_snapshot.node->isLeaf());

		return leaf_snapshot;
	}

	// Root mutex must be held
	inline void
	store_root(node_t *new_root)
	{
		m_root_state.store(m_root_state.load().set_locked(), std::memory_order_release);
		m_root.store(new_root, std::memory_order_release);
		m_root_state.store(m_root_state.load().reset_locked().increment_version(),
		                   std::memory_order_release);
		m_height.store(m_height.load() + 1, std::memory_order_release);
	}

	// Root mutex must be held
	inline void
	create_root(NodeSplitInfo splitinfo)
	{
		inner_node_t *new_root =
		    inner_node_t::alloc(splitinfo.left->lowkey, splitinfo.right->highkey, m_height + 1);

		new_root->insert_neg_infinity(splitinfo.left);
		new_root->append(*splitinfo.split_key, splitinfo.right);

		store_root(new_root);
	}

	inline bool
	update_root(nodestate_t rootstate, node_t *new_root)
	{
		MutexLockType lock{ *m_root_mutex };

		if (m_root_state.load() != rootstate)
			return false;

		store_root(new_root);

		return true;
	}

	inline void
	ensure_root()
	{
		while (m_root == nullptr)
		{
			auto new_root = leaf_node_t::alloc(std::nullopt, std::nullopt, m_height);

			if (!update_root({}, new_root))
				delete new_root;
		}
	}

	inline bool
	is_snapshot_stale(const NodeSnapshot &snapshot) const
	{
		return snapshot.node ? snapshot.node->getState() != snapshot.state
		                     : m_root_state.load() != snapshot.state;
	}

	static inline void
	insert_into_splitnode(const NodeSplitInfo &parent_splitinfo,
	                      const NodeSplitInfo &child_splitinfo)
	{
		inner_node_t *parent =
		    ASINNER(key_less(*child_splitinfo.split_key, *parent_splitinfo.split_key)
		                ? parent_splitinfo.left
		                : parent_splitinfo.right);

		parent->update_inner_for_split(child_splitinfo);
	}

	template <typename Update>
	OpResult
	replace_subtree_on_version_match(const NodeSnapshotVector &nss_vec,
	                                 int from_ss,
	                                 Update &&update)
	{
		static thread_local std::vector<node_t *> deleted_nodes;

		auto res = [&]() {
			std::vector<std::unique_lock<std::mutex>> locks;
			for (int node_ss = from_ss; node_ss < static_cast<int>(nss_vec.size()); node_ss++)
			{
				const NodeSnapshot &snapshot = nss_vec[node_ss];

				locks.emplace_back(snapshot.node->mutex);

				if (is_snapshot_stale(snapshot))
					return OpResult::STALE_SNAPSHOT;
			}

			if (update())
			{
				for (int node_ss = from_ss; node_ss < static_cast<int>(nss_vec.size()); node_ss++)
				{
					const NodeSnapshot &snapshot = nss_vec[node_ss];

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
	split_node(int node_ss, const NodeSnapshotVector &nss_vec, NodeSplitInfo &prev_split_info)
	{
		const NodeSnapshot &node_snapshot   = nss_vec[node_ss];
		const NodeSnapshot &parent_snapshot = nss_vec[node_ss - 1];
		Node *node                          = static_cast<Node *>(node_snapshot.node);
		inner_node_t *parent                = static_cast<inner_node_t *>(parent_snapshot.node);
		NodeSplitInfo splitinfo;

		MutexLockType parentlock{ parent ? parent->mutex : *m_root_mutex };

		if (is_snapshot_stale(parent_snapshot))
			return { OpResult::STALE_SNAPSHOT, {} };

		{
			MutexLockType lock{ node->mutex };

			if (is_snapshot_stale(node_snapshot))
				return { OpResult::STALE_SNAPSHOT, {} };

			splitinfo = node->split();
		}

		BTREE_UPDATE_STAT_NODE_BASED(split);

		auto res = replace_subtree_on_version_match(nss_vec, node_ss, [&]() {
			if constexpr (Node::IsInner())
				insert_into_splitnode(splitinfo, prev_split_info);

			if (parent)
			{
				auto ret = parent->update_inner_for_split(splitinfo);

				BTREE_DEBUG_ASSERT(ret != InsertStatus::DUPLICATE);

				return ret == InsertStatus::INSERTED;
			}
			else
			{
				create_root(splitinfo);

				return true;
			}
		});

		return { res, splitinfo };
	}

	template <typename Node>
	OpResult
	trim_node(int node_ss,
	          const Key &key,
	          const NodeSnapshotVector &nss_vec,
	          NodeSplitInfo &prev_split_info)
	{
		const NodeSnapshot &node_snapshot   = nss_vec[node_ss];
		const NodeSnapshot &parent_snapshot = nss_vec[node_ss - 1];
		Node *node                          = static_cast<Node *>(node_snapshot.node);
		inner_node_t *parent                = static_cast<inner_node_t *>(parent_snapshot.node);
		Node *trimmed_node;

		MutexLockType lock{ parent ? parent->mutex : *m_root_mutex };

		if (is_snapshot_stale(parent_snapshot))
			return OpResult::STALE_SNAPSHOT;

		{
			MutexLockType lock{ node->mutex };

			if (is_snapshot_stale(node_snapshot))
				return OpResult::STALE_SNAPSHOT;

			trimmed_node = node->trim();
		}

		BTREE_UPDATE_STAT_NODE_BASED(trim);

		return replace_subtree_on_version_match(nss_vec, node_ss, [&]() {
			if constexpr (Node::IsInner())
				trimmed_node->update_inner_for_split(prev_split_info);

			if (parent)
				parent->update_inner_for_trim(key, trimmed_node);
			else
				store_root(trimmed_node);

			return true;
		});
	}

	std::pair<OpResult, NodeSplitInfo>
	handle_node_overflow(int node_ss,
	                     const Key &key,
	                     const NodeSnapshotVector &nss_vec,
	                     NodeSplitInfo &prev_split_info)
	{
		node_t *node = nss_vec[node_ss].node;

		if (node->canTrim())
		{
			if (node->isLeaf())
				return { trim_node<leaf_node_t>(node_ss, key, nss_vec, prev_split_info), {} };
			else
				return { trim_node<inner_node_t>(node_ss, key, nss_vec, prev_split_info), {} };
		}
		else
		{
			if (node->isLeaf())
				return split_node<leaf_node_t>(node_ss, nss_vec, prev_split_info);
			else
				return split_node<inner_node_t>(node_ss, nss_vec, prev_split_info);
		}
	}

	void
	handle_overflow(const NodeSnapshotVector &nss_vec, const Key &key)
	{
		int node_ss = nss_vec.size() - 1;
		NodeSplitInfo top_splitinfo{};
		static thread_local std::vector<NodeSplitInfo> splitinfos;

		splitinfos.clear();

		BTREE_DEBUG_ASSERT(nss_vec[node_ss].node && nss_vec[node_ss].node->isLeaf());

		while (node_ss > 0)
		{
			auto res = handle_node_overflow(node_ss, key, nss_vec, top_splitinfo);

			if (res.first == OpResult::STALE_SNAPSHOT || res.first == OpResult::SUCCESS)
			{
				if (res.first != OpResult::SUCCESS)
				{
					splitinfos.emplace_back(res.second);

					// Delete allocated nodes
					for (auto splitinfo : splitinfos)
					{
						if (splitinfo.left)
							node_t::free(splitinfo.left);

						if (splitinfo.right)
							node_t::free(splitinfo.right);
					}
				}

				return;
			}

			top_splitinfo = res.second;
			splitinfos.emplace_back(res.second);
			node_ss--;
		}

		BTREE_DEBUG_ASSERT(false && "Shallnot come here");
	}

	template <bool DoUpsert,
	          typename OutputType = std::conditional_t<DoUpsert, std::optional<Value>, bool>>
	inline std::pair<OpResult, OutputType>
	insert_or_upsert_leaf(const NodeSnapshotVector &nss_vec,
	                      bool is_leaf_locked,
	                      const Key &key,
	                      const Value &val)
	{
		InsertStatus status;
		std::optional<Value> oldval{};
		NodeSnapshot leaf_snapshot = nss_vec.back();
		leaf_node_t *leaf          = ASLEAF(leaf_snapshot.node);

		if constexpr (!DoUpsert)
			(void) oldval;

		if (is_leaf_locked)
		{
			BTREE_DEBUG_ASSERT(!is_snapshot_stale(leaf_snapshot));

			if constexpr (DoUpsert)
				std::tie(status, oldval) = leaf->upsert(key, val);
			else
				status = leaf->insert(key, val);

			leaf->mutex.unlock();
		}
		else
		{
			MutexLockType lock{ leaf->mutex };

			if (is_snapshot_stale(leaf_snapshot))
				return { OpResult::STALE_SNAPSHOT, {} };

			if constexpr (DoUpsert)
				std::tie(status, oldval) = leaf->upsert(key, val);
			else
				status = leaf->insert(key, val);
		}

		if (status == InsertStatus::OVERFLOW)
		{
			handle_overflow(nss_vec, key);

			return { OpResult::STALE_SNAPSHOT, {} };
		}

		if (status == InsertStatus::INSERTED)
			BTREE_UPDATE_STAT(element, ++);

		if constexpr (DoUpsert)
			return { OpResult::SUCCESS, oldval };
		else
			return { OpResult::SUCCESS, status != InsertStatus::DUPLICATE };
	}

	inline std::pair<OpResult, std::optional<Value>>
	update_leaf(const NodeSnapshot &leaf_snapshot, const Key &key, const Value &val)
	{
		leaf_node_t *leaf = ASLEAF(leaf_snapshot.node);
		MutexLockType lock{ leaf->mutex };

		if (is_snapshot_stale(leaf_snapshot))
			return { OpResult::STALE_SNAPSHOT, std::nullopt };

		return { OpResult::SUCCESS, leaf->update_leaf(key, val) };
	}

	template <bool DoUpsert>
	auto
	insert_or_upsert(const Key &key, const Value &val)
	{
		static thread_local NodeSnapshotVector nss_vec;

		nss_vec.clear();
		ensure_root();

		while (true)
		{
			EpochGuard eg(this);
			bool is_leaf_locked = get_leaf_containing(key, nss_vec);

			BTREE_DEBUG_ASSERT(nss_vec.size() > 1);

			if (auto res = insert_or_upsert_leaf<DoUpsert>(nss_vec, is_leaf_locked, key, val);
			    res.first != OpResult::STALE_SNAPSHOT)
			{
				return res.second;
			}

			BTREE_UPDATE_STAT(retry, ++);
		}
	}

	struct MergeInfo
	{
		const Key &merge_key;
		int sibilingpos;
	};

	template <typename Node>
	std::optional<MergeInfo>
	get_merge_info(const Node *node, const inner_node_t *parent, const Key &key) const
	{
		int pos = parent->search_inner(key);

		if (pos == 0)
			return {};

		return MergeInfo{ parent->get_key_value(pos)->first, pos - 1 };
	}

	template <typename Node>
	void
	merge_node(int node_ss, const NodeSnapshotVector &nss_vec, const Key &key)
	{
		if (node_ss == 1)
			return;

		const NodeSnapshot &node_snapshot   = nss_vec[node_ss];
		const NodeSnapshot &parent_snapshot = nss_vec[node_ss - 1];
		Node *node                          = static_cast<Node *>(node_snapshot.node);
		inner_node_t *parent                = static_cast<inner_node_t *>(parent_snapshot.node);

		std::optional<MergeInfo> mergeinfo = get_merge_info(node, parent, key);

		if (mergeinfo)
		{
			MutexLockType parent_lock{ parent->mutex };

			if (is_snapshot_stale(parent_snapshot))
				return;

			const Key &merge_key = mergeinfo->merge_key;
			int sibilingpos      = mergeinfo->sibilingpos;
			Node *sibiling       = static_cast<Node *>(parent->get_child(sibilingpos));

			MutexLockType sibiling_lock{ sibiling->mutex };
			MutexLockType node_lock{ node->mutex };

			if (is_snapshot_stale(node_snapshot))
				return;

			Node *mergednode = sibiling->merge(node, merge_key);

			if (mergednode)
			{
				BTREE_UPDATE_STAT_NODE_BASED(merge);

				parent->update_inner_for_merge(sibilingpos, mergednode);

				sibiling->setState(sibiling->getState().set_deleted().increment_version());
				node->setState(node->getState().set_deleted().increment_version());

				m_gc.retire_in_current_epoch(node_t::free, sibiling);
				m_gc.retire_in_current_epoch(node_t::free, node);
			}
		}

		m_gc.switch_epoch();

		if (parent->isUnderfull())
			merge_node<inner_node_t>(node_ss - 1, nss_vec, key);
	}

	std::pair<OpResult, std::optional<Value>>
	delete_from_leaf(const Key &key, bool is_leaf_locked, NodeSnapshotVector &nss_vec)
	{
		int pos;
		bool key_present;
		NodeSnapshot &leaf_snapshot = nss_vec.back();
		leaf_node_t *leaf           = ASLEAF(leaf_snapshot.node);
		std::pair<OpResult, std::optional<Value>> ret{};

		{
			auto do_delete = [&]() {
				if (is_snapshot_stale(leaf_snapshot))
				{
					ret = { OpResult::STALE_SNAPSHOT, std::nullopt };
					return;
				}

				std::tie(pos, key_present) = leaf->lower_bound(key);

				if (!key_present)
				{
					ret = { OpResult::SUCCESS, std::nullopt };
					return;
				}

				ret = { OpResult::SUCCESS, leaf->get_key_value(pos)->second };

				leaf->remove_pos(pos);
				BTREE_UPDATE_STAT(element, --);

				leaf_snapshot = { leaf, leaf->getState() };
			};

			if (is_leaf_locked)
			{
				do_delete();
				leaf->mutex.unlock();
			}
			else
			{
				MutexLockType lock{ leaf->mutex };

				do_delete();
			}
		}

		if (leaf->isUnderfull())
			merge_node<leaf_node_t>(nss_vec.size() - 1, nss_vec, key);

		return ret;
	}

	inline NodeSnapshot
	get_prev_leaf_containing(const Key &key) const
	{
		NodeSnapshot leaf_snapshot{};
		DummyType dummy;

		bool is_leaf_locked = traverse_to_leaf<NO_FILL_SNAPSHOT_VECTOR>(
		    [&](node_t *current) { return ASINNER(current)->get_value_lower_than(key); },
		    dummy,
		    leaf_snapshot);

		if (is_leaf_locked)
		{
			BTREE_DEBUG_ASSERT(leaf_snapshot.node != nullptr);
			leaf_snapshot.node->mutex.unlock();
		}

		if (leaf_snapshot.node)
			BTREE_DEBUG_ASSERT(leaf_snapshot.node->isLeaf());

		return leaf_snapshot;
	}

	leaf_node_t *
	get_prev_leaf(const Key &lowkey, std::vector<int> &slots) const
	{
		leaf_node_t *leaf;
		const Key *key = std::addressof(lowkey);

		do
		{
			NodeSnapshot leaf_snapshot = get_prev_leaf_containing(*key);

			leaf = ASLEAF(leaf_snapshot.node);

			if (leaf)
			{
				leaf->get_slots_less_than(*key, slots);

				if (is_snapshot_stale(leaf_snapshot))
				{
					BTREE_UPDATE_STAT(retry, ++);
					continue;
				}

				if (leaf && slots.empty())
				{
					if (leaf->highkey)
					{
						key = std::addressof(leaf->lowkey.value());
						continue;
					}
					else
					{
						leaf = nullptr;
					}
				}
				break;
			}

		} while (leaf);

		return leaf;
	}

	leaf_node_t *
	get_next_leaf(const Key &highkey, std::vector<int> &slots) const
	{
		leaf_node_t *leaf;
		const Key *key = std::addressof(highkey);

		do
		{
			NodeSnapshot leaf_snapshot = get_leaf_containing(*key);

			leaf = ASLEAF(leaf_snapshot.node);

			if (leaf)
			{
				leaf->get_slots_greater_than_eq(*key, slots);

				if (is_snapshot_stale(leaf_snapshot))
				{
					BTREE_UPDATE_STAT(retry, ++);
					continue;
				}

				if (leaf && slots.empty())
				{
					if (leaf->highkey)
					{
						key = std::addressof(leaf->highkey.value());
						continue;
					}
					else
					{
						leaf = nullptr;
					}
				}
				break;
			}

		} while (leaf);

		return leaf;
	}

	inline NodeSnapshot
	get_last_leaf() const
	{
		NodeSnapshot leaf_snapshot{};
		DummyType dummy;

		bool is_leaf_locked = traverse_to_leaf<
		    NO_FILL_SNAPSHOT_VECTOR>([](node_t *
		                                    current) { return ASINNER(current)->get_last_child(); },
		                             dummy,
		                             leaf_snapshot);

		if (is_leaf_locked)
		{
			BTREE_DEBUG_ASSERT(leaf_snapshot.node != nullptr);
			leaf_snapshot.node->mutex.unlock();
		}

		if (leaf_snapshot.node)
			BTREE_DEBUG_ASSERT(leaf_snapshot.node->isLeaf());

		return leaf_snapshot;
	}

	inline NodeSnapshot
	get_first_leaf() const
	{
		NodeSnapshot leaf_snapshot{};
		DummyType dummy;

		bool is_leaf_locked = traverse_to_leaf<NO_FILL_SNAPSHOT_VECTOR>(
		    [](node_t *current) { return ASINNER(current)->get_first_child(); },
		    dummy,
		    leaf_snapshot);

		if (is_leaf_locked)
		{
			BTREE_DEBUG_ASSERT(leaf_snapshot.node != nullptr);
			leaf_snapshot.node->mutex.unlock();
		}

		if (leaf_snapshot.node)
			BTREE_DEBUG_ASSERT(leaf_snapshot.node->isLeaf());

		return leaf_snapshot;
	}

	inline NodeSnapshot
	get_upper_bound_leaf(const Key &key) const
	{
		NodeSnapshot leaf_snapshot{};
		DummyType dummy;

		bool is_leaf_locked = traverse_to_leaf<NO_FILL_SNAPSHOT_VECTOR>(
		    [&key](node_t *current) {
			    inner_node_t *inner = ASINNER(current);
			    int num_values      = inner->num_values;
			    int pos             = inner->upper_bound_pos(key, num_values);

			    return ASINNER(current)->get_child(pos);
		    },
		    dummy,
		    leaf_snapshot);

		if (is_leaf_locked)
		{
			BTREE_DEBUG_ASSERT(leaf_snapshot.node != nullptr);
			leaf_snapshot.node->mutex.unlock();
		}

		if (leaf_snapshot.node)
			BTREE_DEBUG_ASSERT(leaf_snapshot.node->isLeaf());

		return leaf_snapshot;
	}

	enum IteratorType
	{
		REVERSE,
		FORWARD
	};

public:
	concurrent_map()                       = default;
	concurrent_map(const concurrent_map &) = delete;
	concurrent_map(concurrent_map &&moved)
	    : m_root_mutex(std::move(moved.m_root_mutex))
	    , m_root_state(moved.m_root_state.load())
	    , m_root(moved.m_root.load())
	    , m_height(moved.m_height.load())
	    , m_stats()
	{
		moved.m_root_state.store({});
		moved.m_root.store(nullptr);
		moved.m_height.store(0);
	}

	bool
	Insert(const Key &key, const Value &val)
	{
		return insert_or_upsert<DO_INSERT>(key, val);
	}

	std::optional<Value>
	Upsert(const Key &key, const Value &val)
	{
		return insert_or_upsert<DO_UPSERT>(key, val);
	}

	std::optional<Value>
	Update(const Key &key, const Value &val)
	{
		while (true)
		{
			EpochGuard eg(this);
			NodeSnapshot leaf_snapshot = get_leaf_containing(key);

			if (leaf_snapshot.node == nullptr)
				return {};

			if (auto res = update_leaf(leaf_snapshot, key, val);
			    res.first != OpResult::STALE_SNAPSHOT)
			{
				return res.second;
			}

			BTREE_UPDATE_STAT(retry, ++);
		}
	}

	std::optional<Value>
	Search(const Key &key)
	{
		std::optional<Value> val{};

		do
		{
			EpochGuard eg(this);
			NodeSnapshot leaf_snapshot = get_leaf_containing(key);

			if (leaf_snapshot.node)
			{
				leaf_node_t *leaf = ASLEAF(leaf_snapshot.node);
				int pos;
				bool key_present;

				std::tie(pos, key_present) = leaf->lower_bound(key);

				if (key_present)
					val = leaf->get_key_value(pos)->second;
				else
					val = std::nullopt;

				if (is_snapshot_stale(leaf_snapshot))
				{
					BTREE_UPDATE_STAT(retry, ++);
					continue;
				}
			}

		} while (false);

		return val;
	}

	std::optional<Value>
	Delete(const Key &key)
	{
		static thread_local NodeSnapshotVector nss_vec;

		nss_vec.clear();

		while (true)
		{
			EpochGuard eg(this);
			bool is_leaf_locked = get_leaf_containing(key, nss_vec);

			if (nss_vec.size() > 1)
			{
				if (auto res = delete_from_leaf(key, is_leaf_locked, nss_vec);
				    res.first != OpResult::STALE_SNAPSHOT)
				{
					return res.second;
				}
			}
			else
			{
				return {};
			}

			BTREE_UPDATE_STAT(retry, ++);
		}
	}

	template <IteratorType IType>
	class iterator_impl
	{
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
		const concurrent_map *m_bt;
		const leaf_node_t *m_leaf;
		std::vector<int> m_slots;
		int m_curpos = 0;

		friend class concurrent_map;

		void
		increment()
		{
			if (++m_curpos >= static_cast<int>(m_slots.size()))
			{
				if (this->m_leaf->highkey)
				{
					m_leaf   = ASLEAF(m_bt->get_next_leaf(this->m_leaf->highkey.value(), m_slots));
					m_curpos = 0;
				}
				else
				{
					m_leaf   = nullptr;
					m_curpos = 0;
					m_slots.clear();
				}
			}
		}

		void
		decrement()
		{
			if (--m_curpos < 0)
			{
				if (this->m_leaf->lowkey)
				{
					m_leaf   = ASLEAF(m_bt->get_prev_leaf(this->m_leaf->lowkey.value(), m_slots));
					m_curpos = m_leaf ? static_cast<int>(m_slots.size() - 1) : 0;
				}
				else
				{
					m_leaf   = nullptr;
					m_curpos = 0;
					m_slots.clear();
				}
			}
		}

		pair_type *
		get_pair() const
		{
			return reinterpret_cast<pair_type *>(
			    m_leaf->get_key_value_for_offset(m_slots[m_curpos]));
		}

	public:
		inline iterator_impl(const concurrent_map *bt,
		                     const leaf_node_t *leaf,
		                     std::vector<int> &&slots,
		                     int curpos)
		    : m_bt(bt), m_leaf(leaf), m_slots(std::move(slots)), m_curpos(curpos)
		{}

		inline iterator_impl(const concurrent_map *bt,
		                     const leaf_node_t *leaf,
		                     const std::vector<int> &slots,
		                     int curpos)
		    : m_bt(bt), m_leaf(leaf), m_slots(slots), m_curpos(curpos)
		{}

		inline iterator_impl(const iterator_impl &it) = default;

		template <IteratorType OtherIType>
		inline iterator_impl(const iterator_impl<OtherIType> &it)
		    : iterator_impl(it.m_bt, it.m_leaf, it.m_slots, it.m_curpos)
		{}

		inline reference operator*() const
		{
			return *get_pair();
		}

		inline pointer operator->() const
		{
			return get_pair();
		}

		inline key_type &
		key() const
		{
			return get_pair()->first;
		}

		inline data_type &
		data() const
		{
			return get_pair()->second;
		}

		inline iterator_impl
		operator++()
		{
			if (IType == IteratorType::FORWARD)
				increment();
			else
				decrement();

			return *this;
		}

		inline iterator_impl
		operator++(int)
		{
			auto copy = *this;

			if (IType == IteratorType::FORWARD)
				increment();
			else
				decrement();

			return copy;
		}

		inline iterator_impl
		operator--()
		{
			if (IType == IteratorType::FORWARD)
				decrement();
			else
				increment();

			return *this;
		}

		inline iterator_impl
		operator--(int)
		{
			auto copy = *this;

			if (IType == IteratorType::FORWARD)
				decrement();
			else
				increment();

			return copy;
		}

		inline bool
		operator==(const iterator_impl &other) const
		{
			BTREE_DEBUG_ASSERT(m_bt == other.m_bt);

			return m_leaf == other.m_leaf
			       && (m_leaf ? m_slots[m_curpos] == other.m_slots[other.m_curpos] : true);
		}

		inline bool
		operator!=(const iterator_impl &other) const
		{
			return !(*this == other);
		}
	};

	using const_iterator         = iterator_impl<IteratorType::FORWARD>;
	using const_reverse_iterator = iterator_impl<IteratorType::REVERSE>;

	inline const_iterator
	cbegin() const
	{
		leaf_node_t *leaf;
		std::vector<int> slots;

		do
		{
			NodeSnapshot leaf_snapshot = get_first_leaf();

			leaf = ASLEAF(leaf_snapshot.node);

			if (leaf)
			{
				leaf->get_all_slots(slots);

				if (is_snapshot_stale(leaf_snapshot))
					continue;

				break;
			}
		} while (leaf);

		if (leaf && slots.empty())
			leaf = get_next_leaf(leaf->highkey.value(), slots);

		return leaf ? const_iterator{ this, leaf, std::move(slots), 0 } : cend();
	}

	inline const_iterator
	cend() const
	{
		return { this, nullptr, std::vector<int>{}, 0 };
	}

	inline const_iterator
	begin() const
	{
		return cbegin();
	}

	inline const_iterator
	end() const
	{
		return cend();
	}

	inline const_reverse_iterator
	crbegin() const
	{
		leaf_node_t *leaf;
		std::vector<int> slots;

		do
		{
			NodeSnapshot leaf_snapshot = get_last_leaf();

			leaf = ASLEAF(leaf_snapshot.node);

			if (leaf)
			{
				leaf->get_all_slots(slots);

				if (is_snapshot_stale(leaf_snapshot))
					continue;

				break;
			}
		} while (leaf);

		if (leaf && slots.empty())
			leaf = get_prev_leaf(leaf->lowkey.value(), slots);

		return leaf ? const_reverse_iterator{ this,
			                                  leaf,
			                                  std::move(slots),
			                                  static_cast<int>(slots.size() - 1) }
		            : crend();
	}

	inline const_reverse_iterator
	crend() const
	{
		return end();
	}

	inline const_reverse_iterator
	rbegin() const
	{
		return crbegin();
	}

	inline const_reverse_iterator
	rend() const
	{
		return crend();
	}

	inline const_iterator
	lower_bound(const Key &key) const
	{
		std::vector<int> slots;
		leaf_node_t *leaf = get_next_leaf(key, slots);

		return leaf ? const_iterator{ this, leaf, std::move(slots), 0 } : end();
	}

	inline const_iterator
	upper_bound(const Key &key) const
	{
		std::vector<int> slots;
		leaf_node_t *leaf;

		do
		{
			NodeSnapshot leaf_snapshot = get_upper_bound_leaf(key);

			leaf = ASLEAF(leaf_snapshot.node);

			if (leaf)
			{
				leaf->get_slots_greater_than(key, slots);

				if (is_snapshot_stale(leaf_snapshot))
					continue;

				break;
			}
		} while (leaf);

		bool node_empty = slots.empty();
		auto it         = const_iterator{ this, leaf, std::move(slots), 0 };

		if (leaf && node_empty)
			it++;

		return leaf ? it : end();
	}

	inline int
	height() const
	{
		return m_height;
	}

	inline void
	reclaim_all()
	{
		m_gc.reclaim_all();
	}

	template <typename Dummy = void, typename = std::enable_if_t<Traits::STAT, Dummy>>
	inline std::size_t
	size() const
	{
		return m_stats->num_elements;
	}

	template <typename Dummy = void, typename = std::enable_if_t<Traits::STAT, Dummy>>
	inline bool
	empty() const
	{
		return size() == 0;
	}

	template <typename Dummy = void, typename = typename std::enable_if_t<Traits::STAT, Dummy>>
	inline const Stats &
	stats() const
	{
		return m_stats;
	}

	~concurrent_map()
	{
		std::deque<node_t *> nodes;

		if (m_root)
			nodes.emplace_back(m_root);

		while (!nodes.empty())
		{
			node_t *node = nodes.front();

			if (node->isInner())
				ASINNER(node)->get_children(nodes);

			node_t::free(node);
			nodes.pop_front();
		}
	}

	BTREE_DUMP_METHODS
};

} // namespace btree
