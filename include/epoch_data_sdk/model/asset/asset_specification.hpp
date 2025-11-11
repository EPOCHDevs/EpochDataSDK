#pragma once
//
// Created by dewe on 7/21/23.
//
#include <epoch_data_sdk/common/symbol.hpp>
#include <epoch_data_sdk/common/decimal_utils.hpp>
#include <epoch_data_sdk/common/glaze_custom_types.hpp>
#include <epoch_data_sdk/model/asset/currencies.hpp>
#include <epoch_data_sdk/model/asset/currency_pair.hpp>
#include <epoch_data_sdk/model/asset/exchanges.hpp>
#include <epoch_data_sdk/model/asset/constants.hpp>
#include <epoch_frame/datetime.h>
#include <optional>

// Preferred market data vendor for fetching historical bars
CREATE_ENUM(MarketDataVendor, Archive, Polygon);
namespace epoch_core {
enum class AssetClass : uint8_t { Stocks, FX, Crypto, Futures, Indices };

struct AssetClassWrapper {
  AssetClassWrapper() = default;

  static std::string ToString(AssetClass enumClass) noexcept {
    return ToShortFormString(enumClass);
  }

  static std::string ToShortFormString(AssetClass enumClass) noexcept {
    return g_shortFormStringEnum.at(enumClass);
  }

  static std::string ToLongFormString(AssetClass enumClass) {
    return g_longFormStringEnum.at(enumClass);
  }

  static AssetClass FromString(std::string const &enumClassAsString) {
    if (auto it = g_shortFormEnumAsString.find(enumClassAsString);
        it != g_shortFormEnumAsString.end()) {
      return it->second;
    }
    if (auto it = g_longFormEnumAsString.find(enumClassAsString);
        it != g_longFormEnumAsString.end()) {
      return it->second;
    }
    throw std::invalid_argument(("Invalid AssetClass: ") + (enumClassAsString));
  }

  static bool IsAssetClass(std::string const &enumClassAsString) {
    return g_shortFormEnumAsString.contains(enumClassAsString) ||
           g_longFormEnumAsString.contains(enumClassAsString);
  }

  static bool IsValid(std::string const &enumClassAsString) {
    return IsAssetClass(enumClassAsString);
  }

private:
  inline static std::unordered_map<AssetClass, std::string>
      g_shortFormStringEnum{
          {AssetClass::Stocks, "STK"},  {AssetClass::Crypto, "CRYPTO"},
          {AssetClass::FX, "FX"},       {AssetClass::Futures, "FUT"},
          {AssetClass::Indices, "IND"},
      };
  inline static std::unordered_map<AssetClass, std::string>
      g_longFormStringEnum{
          {AssetClass::Stocks, "Stocks"},   {AssetClass::Crypto, "Crypto"},
          {AssetClass::Futures, "Futures"}, {AssetClass::FX, "FX"},
          {AssetClass::Indices, "Indices"},
      };
  inline static std::unordered_map<std::string, AssetClass>
      g_shortFormEnumAsString{{"STK", AssetClass::Stocks},
                              {"CRYPTO", AssetClass::Crypto},
                              {"FUT", AssetClass::Futures},
                              {"FX", AssetClass::FX},
                              {"IND", AssetClass::Indices}};
  inline static std::unordered_map<std::string, AssetClass>
      g_longFormEnumAsString{{"Stocks", AssetClass::Stocks},
                             {"Crypto", AssetClass::Crypto},
                             {"Futures", AssetClass::Futures},
                             {"FX", AssetClass::FX},
                             {"Indices", AssetClass::Indices}};
};

static AssetClassWrapper AssetClassType;
} // namespace epoch_core

namespace data_sdk::asset {
struct InvalidCurrencyPairException : std::exception {
  std::string message;

  explicit InvalidCurrencyPairException(data_sdk::Symbol const &ticker)
      : message(std::format("Invalid Currency Pair: {}", ticker)) {}

  const char *what() const noexcept override { return message.c_str(); }
};

inline std::filesystem::path operator/(const std::filesystem::path &p,
                                       AssetClass const &type) {
  return p / AssetClassType.ToLongFormString(type).c_str();
}

inline std::ostream &operator<<(std::ostream &os, AssetClass const &type) {
  os << AssetClassType.ToLongFormString(type);
  return os;
}

struct PolygonMetadata {
  std::optional<std::string> original_ticker{}; // e.g., "C:EURUGX"
  std::optional<std::string> type{};
  std::optional<std::string> locale{};
  std::optional<std::string> cik{};
  std::optional<std::string> composite_figi{};
  std::optional<epoch_frame::Date> last_updated{};
};

struct AssetSpecificationData {
  std::string id{};
  std::string name{};
  data_sdk::Symbol ticker{"AAPL"};
  AssetClass asset_class{AssetClass::Stocks};
  Exchange exchange{Exchange::Null};
  CountryCurrency currency{CountryCurrency::USD};
  std::string industry{};
  std::string sector{};
  decimal::Decimal multiplier{1};
  decimal::Decimal min_tick{"0.01"};
  std::string category{};
  std::optional<epoch_frame::Date> eod_start{}, eod_end{}, minute_start{},
      minute_end{};
  std::optional<std::string> expiry_months{};
  // Preferred market data vendor for this asset's primary OHLCV
  epoch_core::MarketDataVendor vendor{epoch_core::MarketDataVendor::Archive};
  std::optional<PolygonMetadata> polygon_metadata{};
  std::optional<epoch_frame::Date>
      last_updated{};                       // asset-level metadata timestamp
  std::optional<std::string> data_source{}; // e.g., "merged_polygon_barchart"

  // Equality ignores external metadata; compares identity-defining fields only
  bool operator==(AssetSpecificationData const &other) const {
    return id == other.id && ticker == other.ticker &&
           asset_class == other.asset_class && exchange == other.exchange &&
           currency == other.currency && multiplier == other.multiplier &&
           min_tick == other.min_tick && name == other.name &&
           industry == other.industry && sector == other.sector &&
           category == other.category && vendor == other.vendor &&
           eod_start == other.eod_start && eod_end == other.eod_end &&
           minute_start == other.minute_start && minute_end == other.minute_end;
  }

  auto operator<=>(AssetSpecificationData const &other) const {
    return other.id <=> id;
  }
};

class AssetSpecification {
public:
  struct Hash {
    size_t operator()(AssetSpecification const &spec) const {
      return std::hash<std::string>()(spec.m_data.ticker.get()) ^
             std::hash<AssetClass>()(spec.m_data.asset_class) ^
             std::hash<epoch_core::Exchange>()(spec.m_data.exchange) ^
             std::hash<int>()(static_cast<int>(spec.m_data.currency));
    }
  };

  explicit AssetSpecification(AssetSpecificationData data);

  AssetSpecification
  MakeAssetSpecification(data_sdk::Symbol const &newSymbol) const;

  AssetSpecification MakeContractSpecification(
      data_sdk::Symbol const &newSymbol) const;

  bool HasCurrencyPair() const { return m_currencyPair.has_value(); }

  [[nodiscard]] std::optional<CurrencyPair> GetCurrencyPair() const {
    return m_currencyPair;
  }

  std::strong_ordering operator<=>(AssetSpecification const &other) const {
    return m_data <=> other.m_data;
  }

  bool operator==(AssetSpecification const &other) const {
    return m_data == other.m_data;
  }

  inline data_sdk::Symbol GetSymbol() const noexcept {
    return m_data.ticker;
  }

  inline std::string GetID() const noexcept { return m_data.id; }

  inline AssetClass GetAssetClass() const noexcept {
    return m_data.asset_class;
  }

  inline std::string GetFullName() const noexcept { return m_data.name; }

  inline decimal::Decimal GetMultiplier() const noexcept {
    return m_data.multiplier;
  }

  inline decimal::Decimal GetMinTick() const noexcept {
    return m_data.min_tick;
  }

  inline epoch_core::Exchange GetExchange() const noexcept {
    return m_data.exchange;
  }

  std::string GetIndustry() const noexcept {
    return m_data.industry.empty() ? "Others" : m_data.industry;
  }

  std::string GetSector() const noexcept {
    return m_data.sector.empty() ? "Others" : m_data.sector;
  }

  std::string GetCategory() const noexcept {
    return m_data.category.empty() ? "Others" : m_data.category;
  }

  inline epoch_core::CountryCurrency GetCountryCurrency() const noexcept {
    return m_data.currency;
  }

  inline epoch_core::MarketDataVendor GetMarketDataVendor() const noexcept {
    return m_data.vendor;
  }

  const std::optional<PolygonMetadata> &GetPolygonMetadata() const noexcept {
    return m_data.polygon_metadata;
  }

  std::optional<epoch_frame::DateTime> GetLastUpdated() const noexcept {
    return m_data.last_updated;
  }

  // Utility: vendor-specific symbol to use with Polygon
  std::optional<std::string> GetVendorSymbolForPolygon() const noexcept {
    if (m_data.polygon_metadata && m_data.polygon_metadata->original_ticker) {
      return m_data.polygon_metadata->original_ticker;
    }
    return std::nullopt;
  }
  std::optional<std::string> GetDataSource() const noexcept {
    return m_data.data_source;
  }

  std::optional<epoch_frame::Date> GetStartEODDate() const noexcept {
    return m_data.eod_start;
  }

  std::optional<epoch_frame::Date> GetEndEODDate() const noexcept {
    return m_data.eod_end;
  }

  std::optional<epoch_frame::Date> GetStartMinuteDate() const noexcept {
    return m_data.minute_start;
  }

  std::optional<epoch_frame::Date> GetEndMinuteDate() const noexcept {
    return m_data.minute_end;
  }

  bool hasMinuteData() const {
    return m_data.minute_start && m_data.minute_end;
  }

  friend std::ostream &operator<<(std::ostream &os,
                                  const AssetSpecification &spec);

  AssetSpecificationData GetData() const { return m_data; }

private:
  AssetSpecificationData m_data;
  std::optional<CurrencyPair> m_currencyPair;

  std::optional<CurrencyPair>
  GetCryptoCurrencyPair(data_sdk::Symbol const &symbolStr);

  std::optional<CurrencyPair> GetFXPair(data_sdk::Symbol const &symbolStr);

  friend class AssetSpecificationDatabase;
  friend class AssetBuilder;
};

using AssetSpecifications =
    std::unordered_set<AssetSpecification, AssetSpecification::Hash>;

} // namespace data_sdk::asset

ADD_GLAZE_ENUM(AssetClass, uint8_t, Stocks, FX, Crypto, Futures, Indices);