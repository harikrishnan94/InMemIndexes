#pragma once

#include <algorithm>
#include <tuple>

namespace indexes::btree {
namespace detail {
enum { LESS, GREATER };
template <int Order, typename... Types>
struct compound_key : std::tuple<Types...> {
  using std::tuple<Types...>::tuple;
  using std::tuple<Types...>::operator=;

  static constexpr bool is_dynamic = false;
};

template <size_t Index, int Order, typename... Types1, typename... Types2>
bool less(const compound_key<Order, Types1...> &k1,
          const compound_key<Order, Types2...> &k2) {
  constexpr auto minlen = std::min(sizeof...(Types1), sizeof...(Types2));

  if constexpr (Index == minlen) {
    if constexpr (sizeof...(Types1) == sizeof...(Types2))
      return false;
    else if constexpr (sizeof...(Types1) == minlen)
      return true;
    else
      return false;
  } else {
    if (std::get<Index>(k1) < std::get<Index>(k2))
      return true;

    if (std::get<Index>(k2) < std::get<Index>(k1))
      return false;

    return less<Index + 1>(k1, k2);
  }
}

template <typename... Types1, int Order, typename... Types2>
inline bool operator==(const compound_key<Order, Types1...> &x,
                       const compound_key<Order, Types2...> &y) {
  if constexpr (sizeof...(Types1) != sizeof...(Types2))
    return false;
  else
    return std::tuple<Types1...>{x} == std::tuple<Types2...>{y};
}
template <typename... Types1, int Order, typename... Types2>
inline bool operator!=(const compound_key<Order, Types1...> &x,
                       const compound_key<Order, Types2...> &y) {
  return !(x == y);
}

template <typename... Types1, int Order, typename... Types2>
inline bool operator<(const compound_key<Order, Types1...> &x,
                      const compound_key<Order, Types2...> &y) {
  return less<0>(x, y);
}
template <typename... Types1, int Order, typename... Types2>
inline bool operator>(const compound_key<Order, Types1...> &x,
                      const compound_key<Order, Types2...> &y) {
  return less<0>(y, x);
}
template <typename... Types1, int Order, typename... Types2>
inline bool operator<=(const compound_key<Order, Types1...> &x,
                       const compound_key<Order, Types2...> &y) {
  return !less<0>(y, x);
}
template <typename... Types1, int Order, typename... Types2>
inline bool operator>=(const compound_key<Order, Types1...> &x,
                       const compound_key<Order, Types2...> &y) {
  return !less<0>(x, y);
}
} // namespace detail

template <typename... Types>
using compound_key = detail::compound_key<detail::LESS, Types...>;

template <typename... Types>
using compound_key_greater = detail::compound_key<detail::GREATER, Types...>;
} // namespace indexes::btree