#pragma once

#include <atomic>
#include <limits>
#include <type_traits>

namespace indexes::utils
{
template <typename IntType, typename Tag, typename Enable = void>
class TypeSafeInt;

template <typename IntType, typename Tag>
class TypeSafeInt<
    IntType,
    Tag,
    typename std::enable_if_t<
        std::is_integral_v<
            IntType> && !std::is_same_v<IntType, bool> && !std::is_same_v<IntType, char>>>
{
public:
	constexpr TypeSafeInt(IntType val) noexcept : m_val(val)
	{}

	constexpr TypeSafeInt() noexcept = default;

	constexpr IntType
	get() const noexcept
	{
		return m_val;
	}

	// Conversion operators
	explicit constexpr operator IntType() const noexcept
	{
		return m_val;
	}

	explicit constexpr operator bool() const noexcept
	{
		return m_val != 0;
	}

	// Deleted assignment operators
	constexpr TypeSafeInt &operator=(IntType) = delete;

	template <typename OtherType, typename OtherTag>
	constexpr TypeSafeInt &operator=(const TypeSafeInt<OtherType, OtherTag> other) = delete;

	template <typename OtherTag>
	constexpr TypeSafeInt &operator=(const TypeSafeInt<IntType, OtherTag> other) = delete;

	template <typename OtherType>
	constexpr TypeSafeInt &operator=(const TypeSafeInt<OtherType, Tag> other) = delete;

	// Overloaded unary operators
	constexpr TypeSafeInt
	operator+() const noexcept
	{
		return *this;
	}

	constexpr TypeSafeInt
	operator-() const noexcept
	{
		return TypeSafeInt(-m_val);
	}

	// Overloaded increment/decremet operators
	constexpr TypeSafeInt &
	operator++() noexcept
	{
		++m_val;
		return *this;
	}

	constexpr TypeSafeInt &
	operator--() noexcept
	{
		--m_val;
		return *this;
	}

	constexpr TypeSafeInt
	operator++(int) noexcept
	{
		auto t = *this;

		++m_val;
		return t;
	}

	constexpr TypeSafeInt
	operator--(int) noexcept
	{
		auto t = *this;

		--m_val;
		return t;
	}

	// Overloaded arithmetic operators
	constexpr TypeSafeInt
	operator+(const TypeSafeInt<IntType, Tag> other) const noexcept
	{
		return TypeSafeInt(m_val + other.m_val);
	}

	constexpr TypeSafeInt
	operator-(const TypeSafeInt<IntType, Tag> other) const noexcept
	{
		return TypeSafeInt(m_val - other.m_val);
	}

	constexpr TypeSafeInt operator*(const TypeSafeInt<IntType, Tag> other) const noexcept
	{
		return TypeSafeInt(m_val * other.m_val);
	}

	constexpr TypeSafeInt
	operator/(const TypeSafeInt<IntType, Tag> other) const noexcept
	{
		return TypeSafeInt(m_val / other.m_val);
	}

	constexpr TypeSafeInt
	operator%(const TypeSafeInt<IntType, Tag> other) const noexcept
	{
		return TypeSafeInt(m_val % other.m_val);
	}

	// Overloaded comparison operators
	constexpr bool
	operator==(const TypeSafeInt<IntType, Tag> other) const noexcept
	{
		return m_val == other.m_val;
	}

	constexpr bool
	operator!=(const TypeSafeInt<IntType, Tag> other) const noexcept
	{
		return m_val != other.m_val;
	}

	constexpr bool
	operator<(const TypeSafeInt<IntType, Tag> other) const noexcept
	{
		return m_val < other.m_val;
	}

	constexpr bool
	operator<=(const TypeSafeInt<IntType, Tag> other) const noexcept
	{
		return m_val <= other.m_val;
	}

	constexpr bool
	operator>(const TypeSafeInt<IntType, Tag> other) const noexcept
	{
		return m_val > other.m_val;
	}

	constexpr bool
	operator>=(const TypeSafeInt<IntType, Tag> other) const noexcept
	{
		return m_val >= other.m_val;
	}

	// Overloaded bit operators
	constexpr TypeSafeInt
	operator~() const noexcept
	{
		return ~m_val;
	}

	constexpr TypeSafeInt operator!() const noexcept
	{
		return !m_val;
	}

	constexpr TypeSafeInt
	operator|(const TypeSafeInt<IntType, Tag> other) const noexcept
	{
		return m_val | other.m_val;
	}

	constexpr TypeSafeInt operator&(const TypeSafeInt<IntType, Tag> other) const noexcept
	{
		return m_val & other.m_val;
	}

	constexpr TypeSafeInt
	operator^(const TypeSafeInt<IntType, Tag> other) const noexcept
	{
		return m_val ^ other.m_val;
	}

	template <typename AnyInt,
	          typename = std::enable_if_t<
	              std::is_integral_v<
	                  AnyInt> && !std::is_same_v<AnyInt, bool> && !std::is_same_v<AnyInt, char>>>
	constexpr TypeSafeInt
	operator<<(const AnyInt shift) const noexcept
	{
		return m_val << shift;
	}

	template <typename AnyInt,
	          typename = std::enable_if_t<
	              std::is_integral_v<
	                  AnyInt> && !std::is_same_v<AnyInt, bool> && !std::is_same_v<AnyInt, char>>>
	constexpr TypeSafeInt
	operator>>(const AnyInt shift) const noexcept
	{
		return m_val >> shift;
	}

	// Overloaded compound asignment operators
	constexpr TypeSafeInt &
	operator+=(const TypeSafeInt<IntType, Tag> other) noexcept
	{
		m_val += other.m_val;
		return *this;
	}

	constexpr TypeSafeInt &
	operator-=(const TypeSafeInt<IntType, Tag> other) noexcept
	{
		m_val -= other.m_val;
		return *this;
	}

	constexpr TypeSafeInt &
	operator*=(const TypeSafeInt<IntType, Tag> other) noexcept
	{
		m_val *= other.m_val;
		return *this;
	}

	constexpr TypeSafeInt &
	operator/=(const TypeSafeInt<IntType, Tag> other) noexcept
	{
		m_val /= other.m_val;
		return *this;
	}

	constexpr TypeSafeInt &
	operator%=(const TypeSafeInt<IntType, Tag> other) noexcept
	{
		m_val %= other.m_val;
		return *this;
	}

	constexpr TypeSafeInt &
	operator|=(const TypeSafeInt<IntType, Tag> other) noexcept
	{
		m_val |= other.m_val;
		return *this;
	}

	constexpr TypeSafeInt &
	operator&=(const TypeSafeInt<IntType, Tag> other) noexcept
	{
		m_val &= other.m_val;
		return *this;
	}

	template <typename AnyInt,
	          typename = std::enable_if_t<
	              std::is_integral_v<
	                  AnyInt> && !std::is_same_v<AnyInt, bool> && !std::is_same_v<AnyInt, char>>>
	constexpr TypeSafeInt &
	operator<<=(const AnyInt shift) noexcept
	{
		m_val <<= shift;
		return *this;
	}

	template <typename AnyInt,
	          typename = std::enable_if_t<
	              std::is_integral_v<
	                  AnyInt> && !std::is_same_v<AnyInt, bool> && !std::is_same_v<AnyInt, char>>>
	constexpr TypeSafeInt &
	operator>>=(TypeSafeInt<IntType, Tag> shift) noexcept
	{
		m_val >>= shift;
		return *this;
	}

private:
	IntType m_val;
};

} // namespace indexes::utils

template <typename IntType, typename Tag>
struct std::is_integral<indexes::utils::TypeSafeInt<IntType, Tag>> : public true_type
{};

template <typename IntType, typename Tag>
struct std::numeric_limits<indexes::utils::TypeSafeInt<IntType, Tag>>
{
	using type = indexes::utils::TypeSafeInt<IntType, Tag>;

	static constexpr bool is_specialized = true;

	static constexpr type
	min() noexcept
	{
		return type(std::numeric_limits<IntType>::min());
	}

	static constexpr type
	max() noexcept
	{
		return type(std::numeric_limits<IntType>::max());
	}

	static constexpr type
	lowest() noexcept
	{
		return type(std::numeric_limits<IntType>::lowest());
	}

	static constexpr int digits      = std::numeric_limits<IntType>::digits;
	static constexpr int digits10    = std::numeric_limits<IntType>::digits10;
	static constexpr bool is_signed  = std::numeric_limits<IntType>::is_signed;
	static constexpr bool is_integer = std::numeric_limits<IntType>::is_integer;
	static constexpr bool is_exact   = std::numeric_limits<IntType>::is_exact;
	static constexpr int radix       = std::numeric_limits<IntType>::radix;
	static constexpr bool is_iec559  = false;
	static constexpr bool is_bounded = false;
	static constexpr bool is_modulo  = false;
	static constexpr bool traps      = std::numeric_limits<IntType>::traps;
};

template <typename IntType, typename Tag>
struct std::atomic<indexes::utils::TypeSafeInt<IntType, Tag>>
{
	using Type = indexes::utils::TypeSafeInt<IntType, Tag>;

	static constexpr bool is_always_lock_free = true;

	bool
	is_lock_free() const volatile noexcept
	{
		return true;
	}

	bool
	is_lock_free() const noexcept
	{
		return true;
	}

	void
	store(Type desr, memory_order m = memory_order_seq_cst) volatile noexcept
	{
		m_val.store(desr.get(), m);
	}

	void
	store(Type desr, memory_order m = memory_order_seq_cst) noexcept
	{
		m_val.store(desr.get(), m);
	}

	Type
	load(memory_order m = memory_order_seq_cst) const volatile noexcept
	{
		return Type{ m_val.load(m) };
	}

	Type
	load(memory_order m = memory_order_seq_cst) const noexcept
	{
		return Type{ m_val.load(m) };
	}

	operator Type() const volatile noexcept
	{
		return Type{ m_val.load() };
	}

	operator Type() const noexcept
	{
		return Type{ m_val.load() };
	}

	Type
	exchange(Type desr, memory_order m = memory_order_seq_cst) volatile noexcept
	{
		return Type{ m_val.exchange(desr.get(), m) };
	}

	Type
	exchange(Type desr, memory_order m = memory_order_seq_cst) noexcept
	{
		return Type{ m_val.exchange(desr.get(), m) };
	}

	bool
	compare_exchange_weak(Type &expc, Type desr, memory_order s, memory_order f) volatile noexcept
	{
		IntType exp = expc.get();
		bool ret    = m_val.compare_exchange_weak(exp, s, f);

		expc = Type{ exp };

		return ret;
	}

	bool
	compare_exchange_weak(Type &expc, Type desr, memory_order s, memory_order f) noexcept
	{
		IntType exp = expc.get();
		bool ret    = m_val.compare_exchange_weak(exp, s, f);

		expc = Type{ exp };

		return ret;
	}

	bool
	compare_exchange_strong(Type &expc, Type desr, memory_order s, memory_order f) volatile noexcept
	{
		IntType exp = expc.get();
		bool ret    = m_val.compare_exchange_strong(exp, s, f);

		expc = Type{ exp };

		return ret;
	}

	bool
	compare_exchange_strong(Type &expc, Type desr, memory_order s, memory_order f) noexcept
	{
		IntType exp = expc.get();
		bool ret    = m_val.compare_exchange_strong(exp, s, f);

		expc = Type{ exp };

		return ret;
	}

	bool
	compare_exchange_weak(Type &expc,
	                      Type desr,
	                      memory_order m = memory_order_seq_cst) volatile noexcept
	{
		IntType exp = expc.get();
		bool ret    = m_val.compare_exchange_weak(exp, m);

		expc = Type{ exp };

		return ret;
	}

	bool
	compare_exchange_weak(Type &expc, Type desr, memory_order m = memory_order_seq_cst) noexcept
	{
		IntType exp = expc.get();
		bool ret    = m_val.compare_exchange_weak(exp, m);

		expc = Type{ exp };

		return ret;
	}

	bool
	compare_exchange_strong(Type &expc,
	                        Type desr,
	                        memory_order m = memory_order_seq_cst) volatile noexcept
	{
		IntType exp = expc.get();
		bool ret    = m_val.compare_exchange_strong(exp, m);

		expc = Type{ exp };

		return ret;
	}

	bool
	compare_exchange_strong(Type &expc, Type desr, memory_order m = memory_order_seq_cst) noexcept
	{
		IntType exp = expc.get();
		bool ret    = m_val.compare_exchange_strong(exp, m);

		expc = Type{ exp };

		return ret;
	}

	Type
	fetch_add(Type op, memory_order m = memory_order_seq_cst) volatile noexcept
	{
		return Type{ m_val.fetch_add(op.get(), m) };
	}

	Type
	fetch_add(Type op, memory_order m = memory_order_seq_cst) noexcept
	{
		return Type{ m_val.fetch_add(op.get(), m) };
	}

	Type
	fetch_sub(Type op, memory_order m = memory_order_seq_cst) volatile noexcept
	{
		return Type{ m_val.fetch_sub(op.get(), m) };
	}

	Type
	fetch_sub(Type op, memory_order m = memory_order_seq_cst) noexcept
	{
		return Type{ m_val.fetch_sub(op.get(), m) };
	}

	Type
	fetch_and(Type op, memory_order m = memory_order_seq_cst) volatile noexcept
	{
		return Type{ m_val.fetch_and(op.get(), m) };
	}

	Type
	fetch_and(Type op, memory_order m = memory_order_seq_cst) noexcept
	{
		return Type{ m_val.fetch_and(op.get(), m) };
	}

	Type
	fetch_or(Type op, memory_order m = memory_order_seq_cst) volatile noexcept
	{
		return Type{ m_val.fetch_or(op.get(), m) };
	}

	Type
	fetch_or(Type op, memory_order m = memory_order_seq_cst) noexcept
	{
		return Type{ m_val.fetch_or(op.get(), m) };
	}

	Type
	fetch_xor(Type op, memory_order m = memory_order_seq_cst) volatile noexcept
	{
		return Type{ m_val.fetch_xor(op.get(), m) };
	}

	Type
	fetch_xor(Type op, memory_order m = memory_order_seq_cst) noexcept
	{
		return Type{ m_val.fetch_xor(op.get(), m) };
	}

	atomic() noexcept = default;
	constexpr atomic(Type desr) noexcept : m_val{ desr.get() }
	{}

	atomic(const atomic &) = delete;
	atomic &operator=(const atomic &) = delete;
	atomic &operator=(const atomic &) volatile = delete;

	Type
	operator=(Type desr) volatile noexcept
	{
		store(desr);
	}

	Type
	operator=(Type desr) noexcept
	{
		store(desr);

		return desr;
	}

	Type
	operator++(int) volatile noexcept
	{
		return fetch_add(Type{ 1 });
	}

	Type
	operator++(int) noexcept
	{
		return fetch_add(Type{ 1 });
	}

	Type
	operator--(int) volatile noexcept
	{
		return fetch_sub(Type{ 1 });
	}

	Type
	operator--(int) noexcept
	{
		return fetch_sub(Type{ 1 });
	}

	Type
	operator++() volatile noexcept
	{
		return fetch_add(Type{ 1 }) + Type{ 1 };
	}

	Type
	operator++() noexcept
	{
		return fetch_add(Type{ 1 }) + Type{ 1 };
	}

	Type
	operator--() volatile noexcept
	{
		return fetch_sub(Type{ 1 }) - Type{ 1 };
	}

	Type
	operator--() noexcept
	{
		return fetch_sub(Type{ 1 }) - Type{ 1 };
	}

	Type
	operator+=(Type op) volatile noexcept
	{
		return fetch_add(op) + op;
	}

	Type
	operator+=(Type op) noexcept
	{
		return fetch_add(op) + op;
	}

	Type
	operator-=(Type op) volatile noexcept
	{
		return fetch_sub(op) - op;
	}

	Type
	operator-=(Type op) noexcept
	{
		return fetch_sub(op) - op;
	}

	Type
	operator&=(Type op) volatile noexcept
	{
		return fetch_and(op) & op;
	}

	Type
	operator&=(Type op) noexcept
	{
		return fetch_and(op) & op;
	}

	Type
	operator|=(Type op) volatile noexcept
	{
		return fetch_or(op) | op;
	}

	Type
	operator|=(Type op) noexcept
	{
		return fetch_or(op) | op;
	}

	Type
	operator^=(Type op) volatile noexcept
	{
		return fetch_xor(op) ^ op;
	}

	Type
	operator^=(Type op) noexcept
	{
		return fetch_xor(op) ^ op;
	}

private:
	std::atomic<IntType> m_val;
};
