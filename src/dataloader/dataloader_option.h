#pragma once
#include <epoch_data_sdk/common/enums.hpp>
#include <epoch_data_sdk/model/asset/asset.hpp>
#include <epoch_core/macros.h>
#include <epoch_frame/datetime.h>
#include <filesystem>
#include <unordered_map>
#include <optional>
#include <cstdlib>

namespace data_sdk::dataloader {

using data_sdk::DataCategory;
using data_sdk::BenchmarkKind;

inline bool GetDefaultCacheEnabled() {
  if (const char* env = std::getenv("ENABLE_CACHE")) {
    std::string val(env);
    return val == "1" || val == "true" || val == "TRUE";
  }
  return true;  // Default: cache enabled
}

inline std::optional<std::filesystem::path> GetDefaultCacheDir() {
  if (const char* env = std::getenv("CACHE_DIR")) {
    return std::filesystem::path(env);
  }
  return std::nullopt;
}

/**
 * Configuration for an auxiliary data category with optional parameters
 * Parameters allow passing filter/config options to data fetchers
 *
 * Example use cases:
 * - Financials: parameters["type"] = "balance_sheet" | "income_statement" | "cash_flow"
 * - SEC data: parameters["transaction_code"] = "P", parameters["min_value"] = "100000"
 */
struct AuxiliaryCategoryConfig {
  DataCategory category = DataCategory::MinuteBars;  // Default value
  std::unordered_map<std::string, std::string> parameters;

  // Default constructor (needed for std::unordered_map::operator[])
  AuxiliaryCategoryConfig() = default;

  // Constructor for convenience
  explicit AuxiliaryCategoryConfig(DataCategory cat) : category(cat) {}

  AuxiliaryCategoryConfig(DataCategory cat,
                         std::unordered_map<std::string, std::string> params)
      : category(cat), parameters(std::move(params)) {}
};

struct DataloaderOption {
  ADD_ACCESSORS_AND_MUTATORS(StartDate, startDate)
  ADD_ACCESSORS_AND_MUTATORS(EndDate, endDate)
  ADD_ACCESSORS_AND_MUTATORS(PrimaryCategory, primaryCategory)
  ADD_ACCESSORS_AND_MUTATORS(AuxiliaryCategories, auxiliaryCategories)
  ADD_ACCESSORS_AND_MUTATORS(DataloaderAssets, dataloaderAssets)
  ADD_ACCESSORS_AND_MUTATORS(StrategyAssets, strategyAssets)
  ADD_ACCESSORS_AND_MUTATORS(Source, source)
  ADD_ACCESSORS_AND_MUTATORS(CacheDir, cacheDir)
  ADD_ACCESSORS_AND_MUTATORS(CacheTTLSeconds, cacheTTLSeconds)
  ADD_ACCESSORS_AND_MUTATORS(EnableCache, enableCache)
  ADD_ACCESSORS_AND_MUTATORS(BenchmarkKind, benchmarkKind)
  ADD_ACCESSORS_AND_MUTATORS(UseBatchFetching, useBatchFetching)
  ADD_ACCESSORS_AND_MUTATORS(BatchSize, batchSize)

  epoch_frame::Date startDate, endDate;

  // Multi-category support
  DataCategory primaryCategory{DataCategory::MinuteBars};
  std::vector<AuxiliaryCategoryConfig> auxiliaryCategories{};

  asset::AssetHashSet dataloaderAssets{};
  asset::AssetHashSet strategyAssets{};
  asset::AssetHashSet continuationAssets{};
  std::filesystem::path source{};
  std::optional<std::filesystem::path> cacheDir{GetDefaultCacheDir()};

  // Cache configuration (seconds). 1 day default.
  std::uint64_t cacheTTLSeconds{static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::hours(24))
          .count())};
  bool enableCache{GetDefaultCacheEnabled()};
  BenchmarkKind benchmarkKind{BenchmarkKind::SPY};

  // Batch fetching configuration
  // When enabled, processes assets in sequential batches instead of all concurrently
  // This prevents overwhelming the SDK with 100+ concurrent requests
  bool useBatchFetching{true};  // Default: enabled for better efficiency
  std::size_t batchSize{10};    // Default: 10 assets per batch

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

  // For backward compatibility - can be removed if needed
  DataCategory GetDataCategory() const { return primaryCategory; }
};

} // namespace data_sdk::dataloader
