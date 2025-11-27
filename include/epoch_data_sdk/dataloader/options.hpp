#pragma once
#include <epoch_data_sdk/common/enums.hpp>
#include <epoch_data_sdk/dataloader/fetch_kwargs.hpp>
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

  // ============================================================
  // Unified data requests with kwargs
  // ============================================================
  // Each DataRequest specifies a category and its kwargs.
  // Use AddRequest() helpers below for type-safe construction.
  std::vector<dataloader::DataRequest> requests;

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

  // ============================================================
  // Request management helpers
  // ============================================================

  // Add a generic data request
  void AddRequest(const dataloader::DataRequest& request) {
    requests.push_back(request);
  }

  // Add a simple category request (no kwargs needed)
  void AddRequest(DataCategory category) {
    requests.push_back({category, dataloader::NoKwargs{}});
  }

  // Add a financials request with timeframe
  void AddFinancialsRequest(DataCategory category,
                            dataloader::FinancialsTimeframe timeframe = dataloader::FinancialsTimeframe::Quarterly) {
    requests.push_back({category, dataloader::FinancialsKwargs{timeframe}});
  }

  // Add an economic indicator request (uses enum, maps to FRED series internally)
  void AddEconomicIndicator(CrossSectionalDataCategory indicator, bool use_alfred = true) {
    requests.push_back({
      DataCategory::EconomicIndicator,
      dataloader::EconomicIndicatorKwargs{indicator, use_alfred}
    });
  }

  // Add a market index request
  void AddIndex(const std::string& ticker, bool is_eod = true) {
    requests.push_back({
      DataCategory::Indices,
      dataloader::IndicesKwargs{ticker, is_eod}
    });
  }

  // ============================================================
  // Backward compatibility helpers (deprecated, use AddEconomicIndicator/AddIndex)
  // ============================================================

  // Add cross-sectional category (deprecated, use AddEconomicIndicator directly)
  void AddCrossSectionalCategory(CrossSectionalDataCategory category) {
    AddEconomicIndicator(category);
  }

  // Add index ticker (deprecated, maps to AddIndex)
  void AddIndexTicker(const std::string& ticker) {
    AddIndex(ticker, true);  // Default to EOD
  }

  // Get count of cross-sectional categories (deprecated)
  std::set<CrossSectionalDataCategory> GetCrossSectionalCategories() const {
    // Return empty - this is for backward compat; use GetCrossSectionalRequests() instead
    return {};
  }

  // Get indices tickers (deprecated)
  std::set<std::string> GetIndicesTickers() const {
    std::set<std::string> tickers;
    for (const auto& req : requests) {
      if (req.category == DataCategory::Indices) {
        if (const auto* kw = std::get_if<dataloader::IndicesKwargs>(&req.kwargs)) {
          tickers.insert(kw->ticker);
        }
      }
    }
    return tickers;
  }

  // ============================================================
  // Query methods
  // ============================================================

  // Get all requests
  const std::vector<dataloader::DataRequest>& GetRequests() const {
    return requests;
  }

  // Get only per-asset requests (excludes EconomicIndicator and Indices)
  std::vector<dataloader::DataRequest> GetAssetRequests() const {
    std::vector<dataloader::DataRequest> asset_requests;
    for (const auto& req : requests) {
      if (req.category != DataCategory::EconomicIndicator &&
          req.category != DataCategory::Indices) {
        asset_requests.push_back(req);
      }
    }
    return asset_requests;
  }

  // Get only cross-sectional requests (EconomicIndicator and Indices)
  std::vector<dataloader::DataRequest> GetCrossSectionalRequests() const {
    std::vector<dataloader::DataRequest> cs_requests;
    for (const auto& req : requests) {
      if (req.category == DataCategory::EconomicIndicator ||
          req.category == DataCategory::Indices) {
        cs_requests.push_back(req);
      }
    }
    return cs_requests;
  }

  // Get unique categories from requests
  std::set<DataCategory> GetCategories() const {
    std::set<DataCategory> cats;
    for (const auto& req : requests) {
      cats.insert(req.category);
    }
    return cats;
  }

  // Check if any request has the given category
  bool HasCategory(DataCategory cat) const {
    return std::any_of(requests.begin(), requests.end(),
      [cat](const dataloader::DataRequest& r) { return r.category == cat; });
  }

  // Check if using multi-category mode
  bool IsMultiCategory() const {
    return GetCategories().size() > 1;
  }

  // Get primary data category (first non-cross-sectional, or DailyBars as default)
  DataCategory GetPrimaryCategory() const {
    for (const auto& req : requests) {
      if (req.category != DataCategory::EconomicIndicator &&
          req.category != DataCategory::Indices) {
        return req.category;
      }
    }
    return DataCategory::DailyBars;
  }

  // Validation
  bool IsValid() const {
    if (requests.empty()) {
      return false;
    }

    // Cannot mix MinuteBars and DailyBars - they affect the same OHLCV columns
    bool hasMinuteBars = HasCategory(DataCategory::MinuteBars);
    bool hasDailyBars = HasCategory(DataCategory::DailyBars);
    if (hasMinuteBars && hasDailyBars) {
      return false;
    }

    return true;
  }

  // ============================================================
  // Getter methods for other fields
  // ============================================================
  asset::AssetHashSet GetStrategyAssets() const { return strategyAssets; }
  asset::AssetHashSet GetDataloaderAssets() const { return dataloaderAssets; }
  epoch_frame::Date GetStartDate() const { return startDate; }
  epoch_frame::Date GetEndDate() const { return endDate; }
  std::optional<std::filesystem::path> GetCacheDir() const { return cacheDir; }
  std::uint64_t GetCacheTTLSeconds() const { return cacheTTLSeconds; }
  bool GetEnableCache() const { return enableCache; }
  bool GetUseBatchFetching() const { return useBatchFetching; }
  std::size_t GetBatchSize() const { return batchSize; }
};

// Alias for compatibility
using DataloaderOption = DataLoaderOptions;

} // namespace data_sdk
