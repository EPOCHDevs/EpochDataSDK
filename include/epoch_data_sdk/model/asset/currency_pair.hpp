//
// Created by adesola on 11/17/24.
//

#pragma once
#include <epoch_core/common_utils.h>

using namespace epoch_core;

namespace data_sdk::asset {
struct Currency {
  VARIANT_CLASS_MEMBERS(Currency, CryptoCurrency)

  VARIANT_CLASS_MEMBERS(Currency, CountryCurrency)

   static Currency FromString(const std::string &x) {
    return epoch_core::CryptoCurrencyWrapper::IsValid(x)
             ? Currency(epoch_core::CryptoCurrencyWrapper::FromString(x))
             : Currency(epoch_core::CountryCurrencyWrapper::FromString(x));
  }

   std::string ToString() const noexcept {
    return IsCryptoCurrency()
               ? epoch_core::CryptoCurrencyWrapper::ToString(GetCryptoCurrency())
               : epoch_core::CountryCurrencyWrapper::ToString(GetCountryCurrency());
  }

   bool operator==(Currency const &) const = default;

  friend std::ostream &operator<<(std::ostream &os, Currency const &currency) {
    os << currency.ToString();
    return os;
  }

  struct Hash {
    inline size_t operator()(Currency const &currency) const {
      return currency.IsCountryCurrency()
                 ? static_cast<size_t>(currency.GetCountryCurrency())
                 : (static_cast<size_t>(currency.GetCryptoCurrency()) +
                    static_cast<size_t>(CountryCurrency::Null));
    }
  };

private:
  std::variant<epoch_core::CryptoCurrency, epoch_core::CountryCurrency> impl;
};

using CurrencyPair = std::array<Currency, 2>;

template <class V>
using CurrencyHashMap = std::unordered_map<Currency, V, Currency::Hash>;

} // namespace data_sdk::asset