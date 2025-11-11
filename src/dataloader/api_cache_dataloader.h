#pragma once
#include <epoch_data_sdk/dataloader/cache/types.hpp>
#include "cache/day_bucket_cache_provider.h"
#include <epoch_data_sdk/dataloader/options.hpp>
#include "fetcher_provider_default.h"
#include <epoch_data_sdk/dataloader/cache/provider.hpp>
#include <epoch_data_sdk/dataloader/dataloader.hpp>
#include <epoch_data_sdk/dataloader/fetcher.hpp>
#include <drogon/drogon.h>
#include <epoch_frame/series.h>

namespace data_sdk::dataloader {

using cache::ICacheProvider;
using cache::CacheLoadParams;

class ApiCacheDataloader : public IDataLoader {
public:
  explicit ApiCacheDataloader(DataloaderOption option,
                              std::shared_ptr<ICacheProvider> cache,
                              std::shared_ptr<IFetcherProvider> fetchers);

  void LoadData() final;

  asset::AssetHashMap<epoch_frame::DataFrame> GetStoredData() const final {
    return m_loadedData;
  }

  DataCategory GetDataCategory() const final {
    return m_option.GetDataCategory();
  }
  asset::AssetHashSet GetStrategyAssets() const final {
    return m_option.GetStrategyAssets();
  }
  asset::AssetHashSet GetAssets() const final {
    return m_option.GetDataloaderAssets();
  }
  std::optional<epoch_frame::Series> GetBenchmark() const final { return m_benchmark; }

  // Load bars for a specific asset and category (synchronous)
  std::expected<epoch_frame::DataFrame, std::string>
  LoadAssetBars(const asset::Asset &asset, DataCategory category,
                const IDataFetcher::Parameters& parameters = {}) const;

  // Async version - load bars for a specific asset and category
  drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
  LoadAssetBarsAsync(const asset::Asset &asset, DataCategory category,
                     IDataFetcher::Parameters parameters = {}) const;

  // Build cache load parameters from options
  cache::CacheLoadParams buildCacheParams(const asset::Asset& asset,
                                          DataCategory category,
                                          bool forceRefreshToday = false) const;

  // Log campaign viability report
  void LogCampaignViability() const;

private:
  DataloaderOption m_option;
  std::shared_ptr<ICacheProvider> m_cacheProvider;
  std::shared_ptr<IFetcherProvider> m_fetcherProvider;
  asset::AssetHashMap<epoch_frame::DataFrame> m_loadedData;
  std::optional<epoch_frame::Series> m_benchmark;

  // Validate loaded data is within requested range
  bool validateDataRange(const epoch_frame::DataFrame& df,
                        const epoch_frame::Date& fromDate,
                        const epoch_frame::Date& toDate,
                        const asset::Asset& asset) const;

  // Single-category loading (no merging) - sync
  std::expected<epoch_frame::DataFrame, std::string>
  LoadSingleCategory(const asset::Asset& asset) const;

  // Single-category loading (no merging) - async
  drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
  LoadSingleCategoryAsync(const asset::Asset& asset) const;

  // Multi-category loading with merging - sync
  std::expected<epoch_frame::DataFrame, std::string>
  LoadMultiCategory(const asset::Asset& asset) const;

  // Multi-category loading with merging - async
  drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
  LoadMultiCategoryAsync(const asset::Asset& asset) const;

  // Add timestamp preservation column to DataFrame
  epoch_frame::DataFrame AddTimestampColumn(
      const epoch_frame::DataFrame& df,
      const std::string& column_name) const;

  // Normalize DataFrame index to dates (midnight UTC)
  epoch_frame::DataFrame NormalizeToDates(
      const epoch_frame::DataFrame& df) const;

  // Merge auxiliary data into primary DataFrame
  epoch_frame::DataFrame MergeAuxiliaryData(
      epoch_frame::DataFrame primary,
      const asset::Asset& asset,
      bool is_intraday) const;
};

} // namespace data_sdk::dataloader
