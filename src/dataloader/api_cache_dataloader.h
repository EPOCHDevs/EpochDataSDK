#pragma once
#include <epoch_data_sdk/dataloader/cache/types.hpp>
#include "cache/day_bucket_cache_provider.h"
#include <epoch_data_sdk/dataloader/options.hpp>
#include "fetcher_provider_default.h"
#include <epoch_data_sdk/dataloader/cache/provider.hpp>
#include <epoch_data_sdk/dataloader/dataloader.hpp>
#include <epoch_data_sdk/dataloader/fetcher.hpp>
#include <epoch_data_sdk/dataloader/fetch_kwargs.hpp>
#include <epoch_data_sdk/dataloader/merger.hpp>
#include <drogon/drogon.h>
#include <epoch_frame/series.h>

namespace data_sdk::dataloader {

using cache::ICacheProvider;
using cache::CacheLoadParams;

class ApiCacheDataloader : public IDataLoader {
public:
  explicit ApiCacheDataloader(DataloaderOption option,
                              std::shared_ptr<ICacheProvider> cache,
                              std::shared_ptr<IFetcherProvider> fetchers,
                              std::unique_ptr<IDataMerger> merger = nullptr);

  void LoadData() final;

  asset::AssetHashMap<epoch_frame::DataFrame> GetStoredData() const final {
    return m_loadedData;
  }

  DataCategory GetDataCategory() const final {
    return m_option.GetPrimaryCategory();
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
  LoadAssetBars(const asset::Asset &asset,
                DataCategory category,
                const FetchKwargs& kwargs = NoKwargs{}) const final;

  // Async version - load bars for a specific asset and category
  drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
  LoadAssetBarsAsync(const asset::Asset &asset,
                     DataCategory category,
                     FetchKwargs kwargs = NoKwargs{}) const final;

  // Load economic indicator data (FRED series)
  std::expected<epoch_frame::DataFrame, std::string>
  LoadEconomicIndicator(CrossSectionalDataCategory indicator,
                        const epoch_frame::Date& fromDate,
                        const epoch_frame::Date& toDate,
                        bool use_alfred = true) const final;

  drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
  LoadEconomicIndicatorAsync(CrossSectionalDataCategory indicator,
                             const epoch_frame::Date& fromDate,
                             const epoch_frame::Date& toDate,
                             bool use_alfred = true) const final;

  // Load market index data (backward compat - delegates to LoadReferenceAggDataAsync)
  std::expected<epoch_frame::DataFrame, std::string>
  LoadIndexData(const std::string& ticker,
                const epoch_frame::Date& fromDate,
                const epoch_frame::Date& toDate,
                bool is_eod = true) const final;

  drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
  LoadIndexDataAsync(const std::string& ticker,
                     const epoch_frame::Date& fromDate,
                     const epoch_frame::Date& toDate,
                     bool is_eod = true) const final;

  // Load reference aggregate data (Stocks, FX, Crypto, Indices)
  // Note: kwargs passed by value to avoid dangling reference in coroutines
  drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
  LoadReferenceAggDataAsync(ReferenceAggKwargs kwargs) const;

  drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
  LoadReferenceAggDataAsync(ReferenceAggKwargs kwargs,
                            const epoch_frame::Date& fromDate,
                            const epoch_frame::Date& toDate) const;

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
  std::unique_ptr<IDataMerger> m_merger;
  asset::AssetHashMap<epoch_frame::DataFrame> m_loadedData;
  std::optional<epoch_frame::Series> m_benchmark;

  // Validate loaded data is within requested range
  bool validateDataRange(const epoch_frame::DataFrame& df,
                        const epoch_frame::Date& fromDate,
                        const epoch_frame::Date& toDate,
                        const asset::Asset& asset) const;

  // Load all categories for a single asset in parallel, then merge
  drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
  LoadAssetDataAsync(const asset::Asset& asset) const;
};

} // namespace data_sdk::dataloader
