#pragma once
#include <epoch_data_sdk/common/enums.hpp>
#include <epoch_data_sdk/model/asset/asset_specification.hpp>
#include <string>
#include <variant>
#include <optional>
#include <functional>
#include <stdexcept>

namespace data_sdk::dataloader {

using epoch_core::AssetClass;
using data_sdk::DividendType;
using data_sdk::DividendTypeWrapper;
using data_sdk::BalanceSheetTimeframe;
using data_sdk::BalanceSheetTimeframeWrapper;
using data_sdk::FinancialsTimeframe;
using data_sdk::FinancialsTimeframeWrapper;

// ============================================================
// Kwargs Structs - One per category that needs additional params
// ============================================================

// Default: No kwargs needed
struct NoKwargs {
  bool operator==(const NoKwargs&) const = default;
};

// Dividends kwargs - filter by dividend type
struct DividendsKwargs {
  std::optional<DividendType> dividend_type;  // Optional filter: CD, SC, LT, ST (if not set, returns all types)

  bool operator==(const DividendsKwargs& other) const {
    return dividend_type == other.dividend_type;
  }

  // Get Polygon API dividend type string
  std::optional<std::string> getTypeString() const {
    if (!dividend_type) return std::nullopt;
    return std::string(DividendTypeWrapper::ToString(*dividend_type));
  }
};

// Balance sheet kwargs (quarterly, annual only - no TTM)
struct BalanceSheetsKwargs {
  BalanceSheetTimeframe timeframe = BalanceSheetTimeframe::quarterly;

  bool operator==(const BalanceSheetsKwargs&) const = default;

  // Get Polygon API timeframe string
  std::string getTimeframeString() const {
    return std::string(BalanceSheetTimeframeWrapper::ToString(timeframe));
  }
};

// Income statements and cash flow kwargs (quarterly, annual, trailing_twelve_months)
struct FinancialsKwargs {
  FinancialsTimeframe timeframe = FinancialsTimeframe::quarterly;

  bool operator==(const FinancialsKwargs&) const = default;

  // Get Polygon API timeframe string
  std::string getTimeframeString() const {
    return std::string(FinancialsTimeframeWrapper::ToString(timeframe));
  }
};

// Economic indicator kwargs (FRED data via enum)
struct EconomicIndicatorKwargs {
  CrossSectionalDataCategory indicator;  // Required: Economic indicator enum
  bool use_alfred = true;                // Use ALFRED for point-in-time data (recommended for backtesting)

  bool operator==(const EconomicIndicatorKwargs& other) const {
    return indicator == other.indicator && use_alfred == other.use_alfred;
  }

  // Get the FRED series ID for this indicator
  std::string getSeriesId() const {
    static const std::unordered_map<CrossSectionalDataCategory, std::string> seriesMap = {
      {CrossSectionalDataCategory::CPI, "CPIAUCSL"},
      {CrossSectionalDataCategory::CoreCPI, "CPILFESL"},
      {CrossSectionalDataCategory::PCE, "PCEPI"},
      {CrossSectionalDataCategory::CorePCE, "PCEPILFE"},
      {CrossSectionalDataCategory::FedFunds, "DFF"},
      {CrossSectionalDataCategory::Treasury3M, "DTB3"},
      {CrossSectionalDataCategory::Treasury2Y, "DGS2"},
      {CrossSectionalDataCategory::Treasury5Y, "DGS5"},
      {CrossSectionalDataCategory::Treasury10Y, "DGS10"},
      {CrossSectionalDataCategory::Treasury30Y, "DGS30"},
      {CrossSectionalDataCategory::Unemployment, "UNRATE"},
      {CrossSectionalDataCategory::NonfarmPayrolls, "PAYEMS"},
      {CrossSectionalDataCategory::InitialClaims, "ICSA"},
      {CrossSectionalDataCategory::GDP, "GDPC1"},
      {CrossSectionalDataCategory::IndustrialProduction, "INDPRO"},
      {CrossSectionalDataCategory::RetailSales, "RSXFS"},
      {CrossSectionalDataCategory::HousingStarts, "HOUST"},
      {CrossSectionalDataCategory::ConsumerSentiment, "UMCSENT"},
      {CrossSectionalDataCategory::M2, "M2SL"},
      {CrossSectionalDataCategory::SP500, "SP500"},
      {CrossSectionalDataCategory::VIX, "VIXCLS"}
    };
    auto it = seriesMap.find(indicator);
    if (it == seriesMap.end()) {
      throw std::runtime_error("Unknown CrossSectionalDataCategory");
    }
    return it->second;
  }

  // Get the human-readable name for this indicator
  std::string getName() const {
    return CrossSectionalDataCategoryWrapper::ToString(indicator);
  }
};

// Reference aggregates kwargs - load OHLCV for any Polygon ticker
// Supports: Stocks, FX, Crypto, Indices (NOT Futures)
// Timeframe is inherited from primary category (DailyBars/MinuteBars)
struct ReferenceAggKwargs {
  std::string ticker;         // Required: Ticker symbol (e.g., "SPX", "AAPL", "EURUSD", "BTCUSD")
  AssetClass asset_class;     // Required: Stocks, FX, Crypto, or Indices
  bool is_eod = true;         // true = daily bars, false = minute bars (set by dataloader from primary category)

  bool operator==(const ReferenceAggKwargs& other) const {
    return ticker == other.ticker && asset_class == other.asset_class && is_eod == other.is_eod;
  }

  // Validate asset class is supported (throws if not)
  void validate() const {
    if (asset_class != AssetClass::Stocks &&
        asset_class != AssetClass::FX &&
        asset_class != AssetClass::Crypto &&
        asset_class != AssetClass::Indices) {
      throw std::invalid_argument(
          "ReferenceAgg only supports Stocks, FX, Crypto, Indices. Got: " +
          std::string(epoch_core::AssetClassWrapper::ToString(asset_class)));
    }
  }

  // Get Polygon ticker prefix for this asset class
  std::string getPolygonPrefix() const {
    switch (asset_class) {
      case AssetClass::Indices: return "I:";
      case AssetClass::FX:      return "C:";
      case AssetClass::Crypto:  return "X:";
      default:                  return "";  // Stocks have no prefix
    }
  }

  // Get column prefix for this asset class
  std::string getColumnPrefix() const {
    switch (asset_class) {
      case AssetClass::Indices: return "IDX:";
      case AssetClass::Stocks:  return "STK:";
      case AssetClass::FX:      return "FX:";
      case AssetClass::Crypto:  return "CRYPTO:";
      default:
        throw std::invalid_argument("Unsupported asset class for column prefix");
    }
  }

  // Get full Polygon ticker (with prefix)
  std::string getPolygonTicker() const {
    return getPolygonPrefix() + ticker;
  }
};

// Backward compatibility alias
using IndicesKwargs = ReferenceAggKwargs;

// ============================================================
// Variant for Runtime Dispatch
// ============================================================

using FetchKwargs = std::variant<
  NoKwargs,
  DividendsKwargs,
  BalanceSheetsKwargs,
  FinancialsKwargs,
  EconomicIndicatorKwargs,
  ReferenceAggKwargs
>;

// ============================================================
// Category Traits - Compile-Time Mapping of DataCategory to Kwargs
// ============================================================

template <DataCategory Cat>
struct KwargsTraits {
  using type = NoKwargs;  // Default: no kwargs required
};

// Specializations for categories that require specific kwargs
template <>
struct KwargsTraits<DataCategory::Dividends> {
  using type = DividendsKwargs;
};

template <>
struct KwargsTraits<DataCategory::BalanceSheets> {
  using type = BalanceSheetsKwargs;
};

template <>
struct KwargsTraits<DataCategory::IncomeStatements> {
  using type = FinancialsKwargs;
};

template <>
struct KwargsTraits<DataCategory::CashFlowStatements> {
  using type = FinancialsKwargs;
};

template <>
struct KwargsTraits<DataCategory::EconomicIndicator> {
  using type = EconomicIndicatorKwargs;
};

template <>
struct KwargsTraits<DataCategory::ReferenceAgg> {
  using type = ReferenceAggKwargs;
};

// Convenience alias
template <DataCategory Cat>
using KwargsFor = typename KwargsTraits<Cat>::type;

// ============================================================
// Helper Functions
// ============================================================

// Type-safe kwargs extraction (throws std::bad_variant_access if wrong type)
template <DataCategory Cat>
const KwargsFor<Cat>& getKwargs(const FetchKwargs& kwargs) {
  return std::get<KwargsFor<Cat>>(kwargs);
}

// Safe kwargs extraction with default
template <DataCategory Cat>
KwargsFor<Cat> getKwargsOr(const FetchKwargs& kwargs, KwargsFor<Cat> defaultValue = {}) {
  if (auto* ptr = std::get_if<KwargsFor<Cat>>(&kwargs)) {
    return *ptr;
  }
  return defaultValue;
}

// Check if kwargs holds the correct type for a category
template <DataCategory Cat>
bool holdsKwargsFor(const FetchKwargs& kwargs) {
  return std::holds_alternative<KwargsFor<Cat>>(kwargs);
}

// ============================================================
// Hash Function for Cache Keys
// ============================================================

inline std::size_t hashKwargs(const FetchKwargs& kwargs) {
  return std::visit([](const auto& k) -> std::size_t {
    using T = std::decay_t<decltype(k)>;

    if constexpr (std::is_same_v<T, NoKwargs>) {
      return 0;
    }
    else if constexpr (std::is_same_v<T, DividendsKwargs>) {
      if (k.dividend_type) {
        return std::hash<int>{}(static_cast<int>(*k.dividend_type));
      }
      return 0;  // No filter = hash 0
    }
    else if constexpr (std::is_same_v<T, BalanceSheetsKwargs>) {
      return std::hash<int>{}(static_cast<int>(k.timeframe));
    }
    else if constexpr (std::is_same_v<T, FinancialsKwargs>) {
      return std::hash<int>{}(static_cast<int>(k.timeframe));
    }
    else if constexpr (std::is_same_v<T, EconomicIndicatorKwargs>) {
      std::size_t h = std::hash<int>{}(static_cast<int>(k.indicator));
      h ^= std::hash<bool>{}(k.use_alfred) << 1;
      return h;
    }
    else if constexpr (std::is_same_v<T, ReferenceAggKwargs>) {
      std::size_t h = std::hash<std::string>{}(k.ticker);
      h ^= std::hash<int>{}(static_cast<int>(k.asset_class)) << 1;
      return h;
    }
    else {
      return 0;
    }
  }, kwargs);
}

// ============================================================
// DataRequest - Unified request struct for DataLoaderOptions
// ============================================================

struct DataRequest {
  DataCategory category;
  FetchKwargs kwargs;

  bool operator==(const DataRequest& other) const {
    return category == other.category && kwargs == other.kwargs;
  }
};

} // namespace data_sdk::dataloader
