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

namespace data_sdk {

/**
 * Category-specific configuration structs
 * Each auxiliary data category can have typed configuration options
 */

// Financials configuration
struct FinancialsConfig {
  enum class StatementType {
    BalanceSheet,
    IncomeStatement,
    CashFlow,
    FinancialRatios
  };
  StatementType type = StatementType::BalanceSheet;

  FinancialsConfig() = default;
  explicit FinancialsConfig(StatementType t) : type(t) {}
};

// Macroeconomic indicators configuration
struct MacroEconomicsConfig {
  enum class Indicator {
    // Inflation indicators
    CPI,          // Consumer Price Index
    CoreCPI,      // Core CPI (excludes food & energy)
    PCE,          // Personal Consumption Expenditures
    CorePCE,      // Core PCE

    // Interest rates
    FedFunds,     // Federal Funds Rate
    Treasury3M,   // 3-Month Treasury
    Treasury2Y,   // 2-Year Treasury
    Treasury5Y,   // 5-Year Treasury
    Treasury10Y,  // 10-Year Treasury
    Treasury30Y,  // 30-Year Treasury

    // Employment
    Unemployment,      // Unemployment Rate
    NonfarmPayrolls,   // Nonfarm Payrolls
    InitialClaims,     // Initial Jobless Claims

    // Growth indicators
    GDP,                  // Gross Domestic Product
    IndustrialProduction, // Industrial Production Index
    RetailSales,          // Retail Sales
    HousingStarts,        // Housing Starts

    // Sentiment & Money Supply
    ConsumerSentiment,    // Consumer Sentiment Index
    M2                    // M2 Money Supply
  };

  Indicator indicator = Indicator::GDP;

  MacroEconomicsConfig() = default;
  explicit MacroEconomicsConfig(Indicator ind) : indicator(ind) {}
};

// Alternative data configuration (SEC filings, etc.)
struct AlternativeDataConfig {
  enum class Source {
    SEC_Form13F,         // Institutional holdings (13F filings)
    SEC_InsiderTrading   // Insider trading transactions
  };

  Source source = Source::SEC_Form13F;

  AlternativeDataConfig() = default;
  explicit AlternativeDataConfig(Source src) : source(src) {}
};

// Tick data configuration (high-frequency data)
struct TickDataConfig {
  enum class TickType {
    Quotes,  // Bid/ask quotes
    Trades   // Trade ticks
  };

  TickType type = TickType::Quotes;

  TickDataConfig() = default;
  explicit TickDataConfig(TickType t) : type(t) {}
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
        // TODO: Refactor fetchers to accept AuxiliaryCategoryConfig directly
        switch (config_val.type) {
          case FinancialsConfig::StatementType::BalanceSheet:
            params["statement_type"] = "balance_sheet";
            break;
          case FinancialsConfig::StatementType::IncomeStatement:
            params["statement_type"] = "income_statement";
            break;
          case FinancialsConfig::StatementType::CashFlow:
            params["statement_type"] = "cash_flow";
            break;
          case FinancialsConfig::StatementType::FinancialRatios:
            params["statement_type"] = "financial_ratios";
            break;
        }
      } else if constexpr (std::is_same_v<T, MacroEconomicsConfig>) {
        // MacroEconomics params - indicator for future use
        params["indicator"] = MacroIndicatorToString(config_val.indicator);
      } else if constexpr (std::is_same_v<T, AlternativeDataConfig>) {
        // Alternative data params
        switch (config_val.source) {
          case AlternativeDataConfig::Source::SEC_Form13F:
            params["source"] = "sec_form13f";
            break;
          case AlternativeDataConfig::Source::SEC_InsiderTrading:
            params["source"] = "sec_insider_trading";
            break;
        }
      } else if constexpr (std::is_same_v<T, TickDataConfig>) {
        // Tick data params
        params["tick_type"] = (config_val.type == TickDataConfig::TickType::Quotes) ? "quotes" : "trades";
      }
    }, config);

    return params;
  }

private:
  // Helper: Convert MacroEconomics indicator to string
  static std::string MacroIndicatorToString(MacroEconomicsConfig::Indicator ind) {
    switch (ind) {
      case MacroEconomicsConfig::Indicator::CPI: return "CPI";
      case MacroEconomicsConfig::Indicator::CoreCPI: return "CoreCPI";
      case MacroEconomicsConfig::Indicator::PCE: return "PCE";
      case MacroEconomicsConfig::Indicator::CorePCE: return "CorePCE";
      case MacroEconomicsConfig::Indicator::FedFunds: return "FedFunds";
      case MacroEconomicsConfig::Indicator::Treasury3M: return "Treasury3M";
      case MacroEconomicsConfig::Indicator::Treasury2Y: return "Treasury2Y";
      case MacroEconomicsConfig::Indicator::Treasury5Y: return "Treasury5Y";
      case MacroEconomicsConfig::Indicator::Treasury10Y: return "Treasury10Y";
      case MacroEconomicsConfig::Indicator::Treasury30Y: return "Treasury30Y";
      case MacroEconomicsConfig::Indicator::Unemployment: return "Unemployment";
      case MacroEconomicsConfig::Indicator::NonfarmPayrolls: return "NonfarmPayrolls";
      case MacroEconomicsConfig::Indicator::InitialClaims: return "InitialClaims";
      case MacroEconomicsConfig::Indicator::GDP: return "GDP";
      case MacroEconomicsConfig::Indicator::IndustrialProduction: return "IndustrialProduction";
      case MacroEconomicsConfig::Indicator::RetailSales: return "RetailSales";
      case MacroEconomicsConfig::Indicator::HousingStarts: return "HousingStarts";
      case MacroEconomicsConfig::Indicator::ConsumerSentiment: return "ConsumerSentiment";
      case MacroEconomicsConfig::Indicator::M2: return "M2";
      default: return "Unknown";
    }
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
