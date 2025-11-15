#pragma once
#include <epoch_data_sdk/dataloader/cache/provider.hpp>
#include "cache_orchestrator.h"
#include "cache_manifest.h"
#include <memory>
#include <mutex>

namespace data_sdk::dataloader::cache {

/**
 * Refactored cache provider using the orchestrator design pattern
 * This version has clear separation of responsibilities
 *
 * Structure:
 *   /cache/MinuteBars/Stocks/AAPL/2024-01-15.arrow (minute data for one day)
 *   /cache/DailyBars/Stocks/AAPL.arrow (all daily data in one file)
 *   /cache/manifest.json (tracks what's cached)
 */
class DayBucketCacheProvider : public ICacheProvider {
public:
  explicit DayBucketCacheProvider();

  // Legacy interface methods (delegate to orchestrator)
  std::optional<epoch_frame::DataFrame>
  TryLoad(const std::optional<std::filesystem::path> &cacheDir,
          const asset::Asset &asset, DataCategory category,
          std::uint64_t ttlSeconds,
          std::filesystem::path &cachePathOut) const override;

  void Write(const std::optional<std::filesystem::path> &cacheDir,
             const std::filesystem::path &cachePath, bool enableCache,
             const epoch_frame::DataFrame &df) const override;

  epoch_frame::DataFrame
  AppendWrite(const std::optional<std::filesystem::path> &cacheDir,
              const asset::Asset &asset, DataCategory category,
              std::uint64_t ttlSeconds, bool enableCache,
              const epoch_frame::DataFrame &update,
              std::filesystem::path *outPath = nullptr) const override;

  // Main interface using IDataFetcher (sync)
  std::expected<epoch_frame::DataFrame, std::string> LoadWithCache(
      const std::optional<std::filesystem::path> &cacheDir,
      const asset::Asset &asset, DataCategory category,
      std::uint64_t ttlSeconds, bool enableCache,
      const epoch_frame::Date &fromDate, const epoch_frame::Date &toDate,
      const std::function<std::expected<epoch_frame::DataFrame, std::string>(
          const epoch_frame::Date &, const epoch_frame::Date &)> &fetchFn)
      const override;

  // Async version
  drogon::Task<std::expected<epoch_frame::DataFrame, std::string>> LoadWithCacheAsync(
      const std::optional<std::filesystem::path> &cacheDir,
      const asset::Asset &asset, DataCategory category,
      std::uint64_t ttlSeconds, bool enableCache,
      const epoch_frame::Date &fromDate, const epoch_frame::Date &toDate,
      const std::function<drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>(
          const epoch_frame::Date &, const epoch_frame::Date &)> &fetchAsyncFn)
      const override;

  // Direct access to orchestrator for testing
  std::shared_ptr<CacheOrchestrator> getOrchestrator() const {
    return m_orchestrator;
  }

private:
  mutable std::shared_ptr<CacheManifest> m_manifest;
  mutable std::shared_ptr<CacheOrchestrator> m_orchestrator;
  mutable std::mutex m_initMutex;  // Protect initialization in multi-threaded context

  // Initialize manifest and orchestrator for a cache directory
  void initializeForCacheDir(const std::filesystem::path& cacheDir) const;

  // Adapter to convert legacy fetch function to IDataFetcher
  class FetchFunctionAdapter : public IDataFetcher {
  public:
    using FetchFn = std::function<std::expected<epoch_frame::DataFrame, std::string>(
        const epoch_frame::Date &, const epoch_frame::Date &)>;

    FetchFunctionAdapter(const asset::Asset& asset, DataCategory category, FetchFn fn)
        : m_asset(asset), m_category(category), m_fetchFn(std::move(fn)) {}

    FetchResult Fetch([[maybe_unused]] const asset::Asset &asset,
                     [[maybe_unused]] DataCategory category,
                     const epoch_frame::Date &fromDate,
                     const epoch_frame::Date &toDate) const override {
      // Ignore passed asset/category and use the ones from constructor
      return m_fetchFn(fromDate, toDate);
    }

    // Async version - just calls sync fetch function
    drogon::Task<FetchResult> FetchAsync([[maybe_unused]] const asset::Asset &asset,
                                        [[maybe_unused]] DataCategory category,
                                        const epoch_frame::Date &fromDate,
                                        const epoch_frame::Date &toDate) const override {
      // Call the wrapped function directly
      co_return m_fetchFn(fromDate, toDate);
    }

  private:
    asset::Asset m_asset;
    DataCategory m_category;
    FetchFn m_fetchFn;
  };

  // Async adapter to convert async fetch function to IDataFetcher
  class AsyncFetchFunctionAdapter : public IDataFetcher {
  public:
    using AsyncFetchFn = std::function<drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>(
        const epoch_frame::Date &, const epoch_frame::Date &)>;

    AsyncFetchFunctionAdapter(const asset::Asset& asset, DataCategory category, AsyncFetchFn fn)
        : m_asset(asset), m_category(category), m_asyncFetchFn(std::move(fn)) {}

    // Sync version not supported - throws
    FetchResult Fetch([[maybe_unused]] const asset::Asset &asset,
                     [[maybe_unused]] DataCategory category,
                     [[maybe_unused]] const epoch_frame::Date &fromDate,
                     [[maybe_unused]] const epoch_frame::Date &toDate) const override {
      throw std::runtime_error("Sync Fetch() not supported on AsyncFetchFunctionAdapter");
    }

    // Async version - calls the wrapped async function
    drogon::Task<FetchResult> FetchAsync([[maybe_unused]] const asset::Asset &asset,
                                        [[maybe_unused]] DataCategory category,
                                        const epoch_frame::Date &fromDate,
                                        const epoch_frame::Date &toDate) const override {
      co_return co_await m_asyncFetchFn(fromDate, toDate);
    }

  private:
    asset::Asset m_asset;
    DataCategory m_category;
    AsyncFetchFn m_asyncFetchFn;
  };
};

} // namespace data_sdk::dataloader::cache