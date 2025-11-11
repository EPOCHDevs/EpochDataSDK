#pragma once
#include <epoch_data_sdk/dataloader/cache/types.hpp>
#include "cache_manifest.h"
#include <epoch_data_sdk/dataloader/fetcher.hpp>
#include <epoch_data_sdk/common/time_provider.hpp>
#include <drogon/drogon.h>
#include <memory>

namespace data_sdk::dataloader::cache {

// Orchestrates cache operations with clear separation of concerns
// This replaces the monolithic LoadWithCache method
class CacheOrchestrator {
public:
  explicit CacheOrchestrator(std::shared_ptr<CacheManifest> manifest,
                            TimeProviderPtr timeProvider = nullptr);

  // Main entry point - coordinates cache probe, fetch, and write (sync)
  std::expected<epoch_frame::DataFrame, std::string>
  load(const CacheLoadParams& params, const IDataFetcher& fetcher);

  // Async version - coordinates cache probe, fetch, and write
  drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
  loadAsync(const CacheLoadParams& params, const IDataFetcher& fetcher);

  // Individual responsibilities (public for testing)

  // 1. Probe cache to determine what we have
  CacheProbeResult probeCache(const CacheLoadParams& params) const;

  // 2. Determine optimal fetch strategy
  FetchStrategy determineFetchStrategy(const CacheLoadParams& params,
                                      const CacheProbeResult& probe) const;

  // 3. Execute fetch based on strategy (sync)
  std::expected<epoch_frame::DataFrame, std::string>
  executeFetch(const FetchStrategy& strategy, const CacheLoadParams& params,
               const IDataFetcher& fetcher) const;

  // 3. Execute fetch based on strategy (async)
  drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
  executeFetchAsync(const FetchStrategy& strategy, const CacheLoadParams& params,
                   const IDataFetcher& fetcher) const;

  // 4. Merge cached and fetched data
  std::expected<epoch_frame::DataFrame, std::string>
  mergeData(const CacheProbeResult& probe,
           const std::optional<epoch_frame::DataFrame>& fetched) const;

  // 5. Write fetched data to cache
  void writeToCache(const CacheWriteParams& params);

  // Set time provider (mainly for testing)
  void setTimeProvider(TimeProviderPtr provider) { m_timeProvider = provider; }

private:
  std::shared_ptr<CacheManifest> m_manifest;
  TimeProviderPtr m_timeProvider;

  // Check if date is today (for intraday freshness)
  bool isToday(const epoch_frame::Date& date) const;

  // Load day bucket files
  std::optional<epoch_frame::DataFrame>
  loadDayBuckets(const CacheLoadParams& params) const;

  // Write day bucket files
  void writeDayBuckets(const CacheWriteParams& params) const;
};

} // namespace data_sdk::dataloader::cache