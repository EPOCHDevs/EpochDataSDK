#pragma once
#include <epoch_data_sdk/common/enums.hpp>
#include <string>
#include <variant>
#include <optional>
#include <functional>

namespace data_sdk::dataloader {

// Timeframe enum for financial statements
enum class FinancialsTimeframe {
  Quarterly,  // Q1, Q2, Q3, Q4 results
  Annual,     // Full year results (10-K)
  TTM         // Trailing Twelve Months
};

// Convert FinancialsTimeframe to Polygon API string
inline std::string toString(FinancialsTimeframe tf) {
  switch (tf) {
    case FinancialsTimeframe::Quarterly: return "quarterly";
    case FinancialsTimeframe::Annual: return "annual";
    case FinancialsTimeframe::TTM: return "ttm";
  }
  return "quarterly";  // Default fallback
}

// ============================================================
// Kwargs Structs - One per category that needs additional params
// ============================================================

// Default: No kwargs needed
struct NoKwargs {
  bool operator==(const NoKwargs&) const = default;
};

// Financial statements kwargs (BalanceSheets, IncomeStatements, CashFlowStatements)
struct FinancialsKwargs {
  FinancialsTimeframe timeframe = FinancialsTimeframe::Quarterly;

  bool operator==(const FinancialsKwargs&) const = default;
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

// Indices kwargs (market indices like SPX, VIX)
struct IndicesKwargs {
  std::string ticker;         // Required: Index ticker (e.g., "SPX", "VIX", "DJI")
  bool is_eod = true;         // Daily bars (true) or minute bars (false)

  bool operator==(const IndicesKwargs& other) const {
    return ticker == other.ticker && is_eod == other.is_eod;
  }
};

// ============================================================
// Variant for Runtime Dispatch
// ============================================================

using FetchKwargs = std::variant<
  NoKwargs,
  FinancialsKwargs,
  EconomicIndicatorKwargs,
  IndicesKwargs
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
struct KwargsTraits<DataCategory::BalanceSheets> {
  using type = FinancialsKwargs;
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
struct KwargsTraits<DataCategory::Indices> {
  using type = IndicesKwargs;
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
    else if constexpr (std::is_same_v<T, FinancialsKwargs>) {
      return std::hash<int>{}(static_cast<int>(k.timeframe));
    }
    else if constexpr (std::is_same_v<T, EconomicIndicatorKwargs>) {
      std::size_t h = std::hash<int>{}(static_cast<int>(k.indicator));
      h ^= std::hash<bool>{}(k.use_alfred) << 1;
      return h;
    }
    else if constexpr (std::is_same_v<T, IndicesKwargs>) {
      std::size_t h = std::hash<std::string>{}(k.ticker);
      h ^= std::hash<bool>{}(k.is_eod) << 1;
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
