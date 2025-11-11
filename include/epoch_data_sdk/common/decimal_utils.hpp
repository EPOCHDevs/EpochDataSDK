//
// Created by adesola on 1/18/25.
//

#pragma once

#include "decimal.hh"
#include <cmath>
#include <ranges>
#include <sstream>
#include <vector>

namespace decimal {
inline std::strong_ordering operator<=>(Decimal const &decimal1,
                                        Decimal const &decimal2) {
  return (decimal1 < decimal2)   ? std::strong_ordering::less
         : (decimal1 > decimal2) ? std::strong_ordering::greater
                                 : std::strong_ordering::equal;
}
} // namespace decimal

namespace data_sdk {
template <typename T> decimal::Decimal toDecimal(T x) {
  std::stringstream ss;
  ss.imbue(
      std::locale::classic()); // Set the stringstream locale to "C" (classic)
  ss << x;
  auto &&str = ss.str();
  if (str == "null" or str.empty()) {
    return {};
  }
  return decimal::Decimal(std::move(str));
}

template <template <typename> typename Container, typename T>
std::vector<decimal::Decimal> toDecimal(Container<T> const &x) {
  auto result = x | std::views::transform(toDecimal<T>);
  return {result.begin(), result.end()};
}

template <typename T = double>
T fromDecimal(decimal::Decimal const &x)
  requires(std::is_floating_point_v<T> or std::same_as<std::string, T> or
           std::is_integral_v<T>)
{
  if constexpr (std::is_floating_point_v<T> or std::same_as<std::string, T>) {
    auto str = x.to_sci();
    const bool isInvalid = ((str == "...") or (str == "null"));

    if constexpr (std::same_as<T, double>) {
      return isInvalid ? std::nan("") : std::stod(str);
    } else if constexpr (std::same_as<T, float>) {
      return isInvalid ? std::nan("") : std::stof(str);
    } else if constexpr (std::same_as<T, std::string>) {
      return isInvalid ? "NaN" : str;
    }
  } else if constexpr (std::is_integral_v<T>) {
    const auto casted = x.to_integral_exact();
    if constexpr (std::unsigned_integral<T>) {
      return casted.u64();
    }
    return casted.i64();
  } else {
    static_assert(true, "fromDecimal: Unsupported type");
  }
  return T{};
}

inline decimal::Decimal operator"" _dec(long double x) {
  return decimal::Decimal(std::to_string(x));
}

inline decimal::Decimal operator"" _dec(unsigned long long x) {
  return decimal::Decimal(std::to_string(x));
}

inline decimal::Decimal operator"" _dec(const char *x, size_t) {
  return decimal::Decimal(x);
}

inline double toDouble(decimal::Decimal const &x) {
  return fromDecimal<double>(x);
}
} // namespace data_sdk