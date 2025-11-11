#pragma once
#include <epoch_data_sdk/common/enums.hpp>
#include <epoch_frame/datetime.h>
#include <epoch_data_sdk/model/asset/asset.hpp>
#include <filesystem>
#include <optional>
#include <vector>
#include <unordered_map>
#include <chrono>

namespace data_sdk {

/**
 * Configuration for an auxiliary data category with optional parameters
 * Parameters allow passing filter/config options to data fetchers
 *
 * Example use cases:
 * - Financials: parameters["type"] = "balance_sheet" | "income_statement" | "cash_flow"
 * - SEC data: parameters["transaction_code"] = "P", parameters["min_value"] = "100000"
 */
struct AuxiliaryCategoryConfig {
  DataCategory category = DataCategory::MinuteBars;
  std::unordered_map<std::string, std::string> parameters;

  AuxiliaryCategoryConfig() = default;
  explicit AuxiliaryCategoryConfig(DataCategory cat) : category(cat) {}
  AuxiliaryCategoryConfig(DataCategory cat,
                         std::unordered_map<std::string, std::string> params)
      : category(cat), parameters(std::move(params)) {}
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
