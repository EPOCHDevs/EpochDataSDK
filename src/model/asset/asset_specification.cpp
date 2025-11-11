//
// Created by dewe on 11/13/23.
//
#include <epoch_data_sdk/model/asset/asset_specification.hpp>

#include <epoch_core/ranges_to.h>

#include <epoch_data_sdk/model/builder/asset_builder.hpp>

namespace data_sdk::asset {
std::optional<CurrencyPair>
AssetSpecification::GetCryptoCurrencyPair(data_sdk::Symbol const &symbol) {
  auto symbolStr = symbol.get().substr(1);
  static auto countryCurrency =
      CountryCurrencyWrapper::GetAllAsStrings() | ranges::to_unordered_set_v;
  static auto cryptoCurrency =
      CryptoCurrencyWrapper::GetAllAsStrings() | ranges::to_unordered_set_v;

  for (size_t i = 3; i < symbolStr.size(); ++i) {
    auto base = symbolStr.substr(0, i);
    auto counter = symbolStr.substr(i);
    bool isCryptoBase = cryptoCurrency.contains(base);

    if (isCryptoBase) {
      if (cryptoCurrency.contains(counter)) {
        if (!CryptoCurrencyWrapper::IsValid(counter)) {
          return std::nullopt;
        }
        return std::optional(
            CurrencyPair{CryptoCurrencyWrapper::FromString(base),
                         CryptoCurrencyWrapper::FromString(counter)});
      } else {
        if (!CountryCurrencyWrapper::IsValid(counter)) {
          return std::nullopt;
        }
        auto CountryCurrency_ = CountryCurrencyWrapper::FromString(counter);
        if (CountryCurrency_ != epoch_core::CountryCurrency::USD) {
          return std::nullopt;
        }
        return std::optional(CurrencyPair{
            CryptoCurrencyWrapper::FromString(base), CountryCurrency_});
      }
    }
  }
  return std::nullopt;
}

std::optional<CurrencyPair>
AssetSpecification::GetFXPair(data_sdk::Symbol const &symbol) {
  auto symbolStr = symbol.get().substr(1);

  static auto countryCurrency =
      CountryCurrencyWrapper::GetAllAsStrings() | ranges::to_unordered_set_v;
  for (size_t i = 3; i < symbolStr.size(); ++i) {
    auto base = symbolStr.substr(0, i);
    if (countryCurrency.contains(base)) {
      auto counter = symbolStr.substr(i);
      if (CountryCurrencyWrapper::IsValid(counter)) {
        return std::make_optional<CurrencyPair>(
            {CountryCurrencyWrapper::FromString(base),
             CountryCurrencyWrapper::FromString(counter)});
      } else {
        break;
      }
    }
  }
  return std::nullopt;
}

AssetSpecification::AssetSpecification(AssetSpecificationData data)
    : m_data(std::move(data)) {
  try {
    if (m_data.asset_class == AssetClass::Crypto) {
      m_currencyPair = GetCryptoCurrencyPair(m_data.ticker);
    } else if (m_data.asset_class == AssetClass::FX) {
      m_currencyPair = GetFXPair(m_data.ticker);
    }
  } catch (...) {
  }
}

AssetSpecification AssetSpecification::MakeAssetSpecification(
    data_sdk::Symbol const &newSymbol) const {
  return MakeAssetSpec(
      AssetSpecificationQuery{std::pair{newSymbol.get(), m_data.asset_class},
                              epoch_core::Exchange::Null, m_data.currency});
}

AssetSpecification AssetSpecification::MakeContractSpecification(
    data_sdk::Symbol const &newSymbol) const {
  auto clone = *this;
  clone.m_data.ticker = newSymbol;
  clone.m_data.id = clone.m_data.ticker.get();
  return clone;
}

std::ostream &operator<<(std::ostream &os, const AssetSpecification &spec) {
  os << "AssetSpecification(ticker=" << spec.m_data.ticker.get() << ")";
  return os;
}
} // namespace data_sdk::asset