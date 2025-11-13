#pragma once
#include <epoch_data_sdk/common/enums.hpp>
#include <epoch_frame/datetime.h>
#include <epoch_data_sdk/model/asset/asset.hpp>
#include <filesystem>
#include <optional>
#include <vector>
#include <unordered_map>
#include <chrono>
#include <variant>
#include <stdexcept>
#include <algorithm>
#include <cctype>

// CREATE_ENUM declarations for auxiliary category configuration
// These go in epoch_core namespace and are brought into data_sdk via using declarations
CREATE_ENUM(FinancialsStatementType, BalanceSheet, IncomeStatement, CashFlow, FinancialRatios);

CREATE_ENUM(MacroEconomicsIndicator,
            CPI, CoreCPI, PCE, CorePCE,
            FedFunds, Treasury3M, Treasury2Y, Treasury5Y, Treasury10Y, Treasury30Y,
            Unemployment, NonfarmPayrolls, InitialClaims,
            GDP, IndustrialProduction, RetailSales, HousingStarts,
            ConsumerSentiment, M2);

CREATE_ENUM(AlternativeDataSource, SEC_Form13F, SEC_InsiderTrading);

CREATE_ENUM(TickDataType, Quotes, Trades);

namespace data_sdk {

// Bring CREATE_ENUM types into data_sdk namespace
using epoch_core::FinancialsStatementType;
using epoch_core::FinancialsStatementTypeWrapper;
using epoch_core::MacroEconomicsIndicator;
using epoch_core::MacroEconomicsIndicatorWrapper;
using epoch_core::AlternativeDataSource;
using epoch_core::AlternativeDataSourceWrapper;
using epoch_core::TickDataType;
using epoch_core::TickDataTypeWrapper;

/**
 * Category-specific configuration structs
 * Each auxiliary data category can have typed configuration options
 */

// Financials configuration
struct FinancialsConfig {
  FinancialsStatementType type = FinancialsStatementType::BalanceSheet;

  FinancialsConfig() = default;
  explicit FinancialsConfig(FinancialsStatementType t) : type(t) {}
};

// Macroeconomic indicators configuration
struct MacroEconomicsConfig {
  MacroEconomicsIndicator indicator = MacroEconomicsIndicator::GDP;

  MacroEconomicsConfig() = default;
  explicit MacroEconomicsConfig(MacroEconomicsIndicator ind) : indicator(ind) {}
};

// Alternative data configuration (SEC filings, etc.)
struct AlternativeDataConfig {
  AlternativeDataSource source = AlternativeDataSource::SEC_Form13F;

  AlternativeDataConfig() = default;
  explicit AlternativeDataConfig(AlternativeDataSource src) : source(src) {}
};

// Tick data configuration (high-frequency data)
struct TickDataConfig {
  TickDataType type = TickDataType::Quotes;

  TickDataConfig() = default;
  explicit TickDataConfig(TickDataType t) : type(t) {}
};

/**
 * Universal configuration variant for all auxiliary categories
 * - std::monostate: For categories without specific config (News, Dividends, Splits, etc.)
 * - Typed configs: For categories requiring specific parameters
 */
using CategoryConfigVariant = std::variant<
  std::monostate,           // No specific configuration needed
  FinancialsConfig,
  MacroEconomicsConfig,
  AlternativeDataConfig,
  TickDataConfig
>;

/**
 * Configuration for an auxiliary data category with type-safe parameters
 *
 * Each category can have a specific typed configuration that determines
 * what data to fetch from the provider.
 *
 * Example usage:
 * - Financials: AuxiliaryCategoryConfig(DataCategory::Financials,
 *                                       FinancialsConfig{StatementType::BalanceSheet})
 * - MacroEconomics: AuxiliaryCategoryConfig(DataCategory::MacroEconomics,
 *                                           MacroEconomicsConfig{Indicator::CPI})
 * - Simple categories: AuxiliaryCategoryConfig(DataCategory::News)
 */
struct AuxiliaryCategoryConfig {
  DataCategory category = DataCategory::MinuteBars;
  CategoryConfigVariant config;  // Type-safe configuration

  // Default constructor
  AuxiliaryCategoryConfig() = default;

  // Constructor for categories without specific config (News, Dividends, etc.)
  explicit AuxiliaryCategoryConfig(DataCategory cat)
      : category(cat), config(std::monostate{}) {}

  // Template constructor for categories with typed config
  template<typename ConfigType>
  AuxiliaryCategoryConfig(DataCategory cat, ConfigType cfg)
      : category(cat), config(std::move(cfg)) {
    ValidateConfigMatch();
  }

  // Validation: ensure the config type matches the category
  void ValidateConfigMatch() const {
    bool valid = false;

    std::visit([this, &valid](auto&& config_val) {
      using T = std::decay_t<decltype(config_val)>;

      if constexpr (std::is_same_v<T, std::monostate>) {
        // Monostate is valid for categories without specific config
        valid = (category == DataCategory::News ||
                 category == DataCategory::Dividends ||
                 category == DataCategory::Splits ||
                 category == DataCategory::ShortInterest ||
                 category == DataCategory::ShortVolume);
      } else if constexpr (std::is_same_v<T, FinancialsConfig>) {
        valid = (category == DataCategory::Financials);
      } else if constexpr (std::is_same_v<T, MacroEconomicsConfig>) {
        valid = (category == DataCategory::MacroEconomics);
      } else if constexpr (std::is_same_v<T, AlternativeDataConfig>) {
        valid = (category == DataCategory::AlternativeData);
      } else if constexpr (std::is_same_v<T, TickDataConfig>) {
        valid = (category == DataCategory::TickData);
      }
    }, config);

    if (!valid) {
      throw std::invalid_argument(
        "Configuration type does not match category. "
        "Category: " + DataCategoryWrapper::ToString(category));
    }
  }

  // Check if this category has specific typed configuration
  bool HasTypedConfig() const {
    return !std::holds_alternative<std::monostate>(config);
  }

  // Convert typed config to string parameters for backward compatibility
  // This is used to pass configuration to fetchers that still use string maps
  // Note: The SDK doesn't actually need string parameters anymore since we have typed configs.
  // This method exists only for backward compatibility during the transition.
  // TODO: Refactor fetchers to use typed configs directly and remove this method.
  std::unordered_map<std::string, std::string> ToParameters() const {
    std::unordered_map<std::string, std::string> params;

    std::visit([&params](auto&& config_val) {
      using T = std::decay_t<decltype(config_val)>;

      if constexpr (std::is_same_v<T, std::monostate>) {
        // No parameters needed
      } else if constexpr (std::is_same_v<T, FinancialsConfig>) {
        // Convert FinancialsConfig to parameters for fetcher compatibility
        params["statement_type"] = ToSnakeCase(FinancialsStatementTypeWrapper::ToString(config_val.type));
      } else if constexpr (std::is_same_v<T, MacroEconomicsConfig>) {
        // MacroEconomics params - indicator remains PascalCase to match FRED convention
        params["indicator"] = MacroEconomicsIndicatorWrapper::ToString(config_val.indicator);
      } else if constexpr (std::is_same_v<T, AlternativeDataConfig>) {
        // Alternative data params - convert to snake_case
        params["source"] = ToSnakeCase(AlternativeDataSourceWrapper::ToString(config_val.source));
      } else if constexpr (std::is_same_v<T, TickDataConfig>) {
        // Tick data params - convert to lowercase
        auto type_str = TickDataTypeWrapper::ToString(config_val.type);
        std::transform(type_str.begin(), type_str.end(), type_str.begin(), ::tolower);
        params["tick_type"] = type_str;
      }
    }, config);

    return params;
  }

private:
  // Helper: Convert PascalCase to snake_case
  static std::string ToSnakeCase(const std::string& str) {
    std::string result;
    result.reserve(str.size() + 5); // Reserve extra space for underscores

    for (size_t i = 0; i < str.size(); ++i) {
      char c = str[i];

      // Insert underscore before uppercase letters (except at start)
      if (i > 0 && std::isupper(c)) {
        // Check if previous char is lowercase or if next char is lowercase (for acronyms like "SEC")
        if (std::islower(str[i-1]) ||
            (i + 1 < str.size() && std::islower(str[i+1]))) {
          result += '_';
        }
      }

      result += std::tolower(c);
    }

    return result;
  }
};

// DataLoader configuration options
struct DataLoaderOptions {
  // Time range
  epoch_frame::Date startDate;
  epoch_frame::Date endDate;

  // Data categories
  DataCategory primaryCategory = DataCategory::DailyBars;
  std::vector<AuxiliaryCategoryConfig> auxiliaryCategories;

  // Assets
  asset::AssetHashSet dataloaderAssets;  // All assets to load
  asset::AssetHashSet strategyAssets;     // Assets used in strategy (subset)
  asset::AssetHashSet continuationAssets; // For futures continuation

  // Data source / provider
  DataProvider provider = DataProvider::Polygon;

  // Provider-specific configuration (will be set based on provider)
  // For EpochArchive: base path to archive
  // For API providers: may not be needed (API key comes from provider options)
  std::optional<std::filesystem::path> sourcePath;

  // Cache configuration
  std::optional<std::filesystem::path> cacheDir;
  std::uint64_t cacheTTLSeconds = 86400;  // 1 day default (in seconds)
  bool enableCache = true;

  // Benchmark
  BenchmarkKind benchmarkKind = BenchmarkKind::None;

  // Batch fetching configuration
  // When enabled, processes assets in sequential batches instead of all concurrently
  // This prevents overwhelming the SDK with 100+ concurrent requests
  bool useBatchFetching = true;
  std::size_t batchSize = 10;

  // Additional configuration
  std::string timestampColumnName = "timestamp";  // For archive/parquet files
  bool intradayAlwaysFresh = false;  // Force refresh of today's data

  // Validation
  bool IsValid() const {
    // Primary must be MinuteBars or DailyBars
    if (!IsTimeSeriesCategory(primaryCategory)) {
      return false;
    }

    // Auxiliaries cannot be TimeBars (cannot mix MinuteBars + DailyBars)
    for (const auto& config : auxiliaryCategories) {
      if (IsTimeSeriesCategory(config.category)) {
        return false;
      }
    }
    return true;
  }

  // Get all categories (primary + auxiliaries)
  std::vector<DataCategory> GetAllCategories() const {
    std::vector<DataCategory> all = {primaryCategory};
    for (const auto& config : auxiliaryCategories) {
      all.push_back(config.category);
    }
    return all;
  }

  // Check if using multi-category mode
  bool IsMultiCategory() const {
    return !auxiliaryCategories.empty();
  }

  // For backward compatibility and getter methods
  DataCategory GetDataCategory() const { return primaryCategory; }
  asset::AssetHashSet GetStrategyAssets() const { return strategyAssets; }
  asset::AssetHashSet GetDataloaderAssets() const { return dataloaderAssets; }
  epoch_frame::Date GetStartDate() const { return startDate; }
  epoch_frame::Date GetEndDate() const { return endDate; }
  std::optional<std::filesystem::path> GetCacheDir() const { return cacheDir; }
  std::uint64_t GetCacheTTLSeconds() const { return cacheTTLSeconds; }
  bool GetEnableCache() const { return enableCache; }
  bool GetUseBatchFetching() const { return useBatchFetching; }
  std::size_t GetBatchSize() const { return batchSize; }

  // Setter methods
  void SetPrimaryCategory(DataCategory cat) { primaryCategory = cat; }
  std::vector<AuxiliaryCategoryConfig>& GetAuxiliaryCategories() { return auxiliaryCategories; }
};

// Alias for compatibility
using DataloaderOption = DataLoaderOptions;

} // namespace data_sdk
