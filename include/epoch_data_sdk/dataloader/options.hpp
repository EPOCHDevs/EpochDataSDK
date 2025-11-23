#pragma once
#include <epoch_data_sdk/common/enums.hpp>
#include <epoch_frame/datetime.h>
#include <epoch_data_sdk/model/asset/asset.hpp>
#include <filesystem>
#include <optional>
#include <vector>
#include <unordered_map>
#include <set>
#include <chrono>
#include <variant>
#include <stdexcept>
#include <algorithm>
#include <cctype>

// CREATE_ENUM declarations for future auxiliary category configuration
// These go in epoch_core namespace and are brought into data_sdk via using declarations
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
using epoch_core::MacroEconomicsIndicator;
using epoch_core::MacroEconomicsIndicatorWrapper;
using epoch_core::AlternativeDataSource;
using epoch_core::AlternativeDataSourceWrapper;
using epoch_core::TickDataType;
using epoch_core::TickDataTypeWrapper;

// DataLoader configuration options
struct DataLoaderOptions {
  // Time range
  epoch_frame::Date startDate;
  epoch_frame::Date endDate;

  // Data categories - flat set (no primary/auxiliary distinction)
  std::set<DataCategory> categories = {DataCategory::DailyBars};

  // Cross-sectional economic indicators to load
  std::set<CrossSectionalDataCategory> crossSectionalCategories;

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
    if (categories.empty()) {
      return false;
    }

    // Cannot mix MinuteBars and DailyBars - they affect the same OHLCV columns
    bool hasMinuteBars = categories.count(DataCategory::MinuteBars) > 0;
    bool hasDailyBars = categories.count(DataCategory::DailyBars) > 0;
    if (hasMinuteBars && hasDailyBars) {
      return false;
    }

    return true;
  }

  // Get all categories
  std::vector<DataCategory> GetAllCategories() const {
    return std::vector<DataCategory>(categories.begin(), categories.end());
  }

  // Check if using multi-category mode
  bool IsMultiCategory() const {
    return categories.size() > 1;
  }

  // For backward compatibility - returns first category or DailyBars
  DataCategory GetDataCategory() const {
    return categories.empty() ? DataCategory::DailyBars : *categories.begin();
  }

  // Getter methods
  asset::AssetHashSet GetStrategyAssets() const { return strategyAssets; }
  asset::AssetHashSet GetDataloaderAssets() const { return dataloaderAssets; }
  epoch_frame::Date GetStartDate() const { return startDate; }
  epoch_frame::Date GetEndDate() const { return endDate; }
  std::optional<std::filesystem::path> GetCacheDir() const { return cacheDir; }
  std::uint64_t GetCacheTTLSeconds() const { return cacheTTLSeconds; }
  bool GetEnableCache() const { return enableCache; }
  bool GetUseBatchFetching() const { return useBatchFetching; }
  std::size_t GetBatchSize() const { return batchSize; }
  const std::set<DataCategory>& GetCategories() const { return categories; }
  const std::set<CrossSectionalDataCategory>& GetCrossSectionalCategories() const {
    return crossSectionalCategories;
  }

  // Setter methods
  void SetCategories(const std::set<DataCategory>& cats) { categories = cats; }
  void AddCategory(DataCategory cat) { categories.insert(cat); }
  void RemoveCategory(DataCategory cat) { categories.erase(cat); }
  void AddCrossSectionalCategory(CrossSectionalDataCategory cat) {
    crossSectionalCategories.insert(cat);
  }
  void RemoveCrossSectionalCategory(CrossSectionalDataCategory cat) {
    crossSectionalCategories.erase(cat);
  }
};

// Alias for compatibility
using DataloaderOption = DataLoaderOptions;

} // namespace data_sdk
