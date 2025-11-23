#pragma once
#include <epoch_data_sdk/dataloader/cache/types.hpp>
#include "cache/day_bucket_cache_provider.h"
#include <epoch_data_sdk/dataloader/options.hpp>
#include "fetcher_provider_default.h"
#include <epoch_data_sdk/dataloader/cache/provider.hpp>
#include <epoch_data_sdk/dataloader/dataloader.hpp>
#include <epoch_data_sdk/dataloader/fetcher.hpp>
#include <epoch_data_sdk/dataloader/cross_sectional_fetcher.hpp>
#include <epoch_data_sdk/dataloader/indices_fetcher.hpp>
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
  LoadAssetBars(const asset::Asset &asset,
                DataCategory category,
                const std::unordered_map<std::string, std::string>& parameters = {}) const final;

  // Async version - load bars for a specific asset and category
  drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
  LoadAssetBarsAsync(const asset::Asset &asset,
                     DataCategory category,
                     std::unordered_map<std::string, std::string> parameters = {}) const final;

  // Cross-sectional economic data methods (FRED indicators)
  std::expected<epoch_frame::DataFrame, std::string>
  LoadCrossSectionalData(CrossSectionalDataCategory category,
                         const epoch_frame::Date& fromDate,
                         const epoch_frame::Date& toDate) const final;

  drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
  LoadCrossSectionalDataAsync(CrossSectionalDataCategory category,
                              const epoch_frame::Date& fromDate,
                              const epoch_frame::Date& toDate) const final;

  // Market indices data methods (Polygon indices like SPX, VIX, NDX)
  std::expected<epoch_frame::DataFrame, std::string>
  LoadIndicesData(const std::string& indexTicker,
                  const epoch_frame::Date& fromDate,
                  const epoch_frame::Date& toDate,
                  bool is_eod = true) const final;

  drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
  LoadIndicesDataAsync(const std::string& indexTicker,
                       const epoch_frame::Date& fromDate,
                       const epoch_frame::Date& toDate,
                       bool is_eod = true) const final;

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
  std::shared_ptr<ICrossSectionalFetcher> m_crossSectionalFetcher;
  std::shared_ptr<IIndicesFetcher> m_indicesFetcher;
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
