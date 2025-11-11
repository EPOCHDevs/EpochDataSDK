#include "day_bucket_cache_provider.h"

#include <epoch_data_sdk/common/bar_attribute.hpp>
#include <epoch_data_sdk/model/builder/asset_builder.hpp>
#include <epoch_data_sdk/common/env_loader.hpp>
#include <epoch_frame/serialization.h>
#include <spdlog/spdlog.h>

namespace data_sdk::dataloader::cache {

DayBucketCacheProvider::DayBucketCacheProvider() {}

void DayBucketCacheProvider::initializeForCacheDir(
    const std::filesystem::path& cacheDir) const {
  std::lock_guard<std::mutex> lock(m_initMutex);

  if (!m_manifest || m_manifest->getManifestPath().parent_path() != cacheDir) {
    auto manifestPath = cacheDir / "manifest.json";
    m_manifest = std::make_shared<CacheManifest>(manifestPath);
    m_orchestrator = std::make_shared<CacheOrchestrator>(m_manifest);
    SPDLOG_DEBUG("Initialized cache provider for directory: {}", cacheDir.string());
  }
}

std::optional<epoch_frame::DataFrame>
DayBucketCacheProvider::TryLoad(const std::optional<std::filesystem::path> &cacheDir,
                                 const asset::Asset &asset, DataCategory category,
                                 std::uint64_t ttlSeconds,
                                 std::filesystem::path &cachePathOut) const {
  if (!cacheDir) {
    return std::nullopt;
  }

  initializeForCacheDir(*cacheDir);

  // For daily+ data, this is straightforward
  if (category != DataCategory::MinuteBars) {
    auto catDir = DataCategoryWrapper::ToString(category);
    auto assetClassDir = AssetClassWrapper::ToLongFormString(asset.GetAssetClass());
    cachePathOut = *cacheDir / catDir / assetClassDir / (asset.GetID() + ".arrow");

    if (!std::filesystem::exists(cachePathOut)) {
      return std::nullopt;
    }

    // Check if expired
    if (ttlSeconds == 0) {
      // TTL of 0 means immediately expired
      SPDLOG_DEBUG("Cache expired: {} (TTL=0)", cachePathOut.string());
      return std::nullopt;
    } else if (ttlSeconds > 0) {
      const auto ftime = std::filesystem::last_write_time(cachePathOut);
      const auto now = decltype(ftime)::clock::now();
      const auto age = now - ftime;
      const auto ageSec = std::chrono::duration_cast<std::chrono::seconds>(age).count();

      if (static_cast<std::uint64_t>(ageSec) > ttlSeconds) {
        SPDLOG_DEBUG("Cache expired: {}", cachePathOut.string());
        return std::nullopt;
      }
    }

    auto res = epoch_frame::read_arrow(cachePathOut, {
      .index_column = data_sdk::EpochStratifyXConstants::instance().TIMESTAMP()
    });

    if (res.ok()) {
      auto dataframe = res.MoveValueUnsafe();
      SPDLOG_INFO("Cache HIT: {} ({} rows)",
                 cachePathOut.string(), dataframe.num_rows());
      return dataframe;
    }
  }

  // For minute data, we need a date range which this method doesn't provide
  SPDLOG_WARN("TryLoad called for minute data without date range");
  return std::nullopt;
}

void DayBucketCacheProvider::Write(const std::optional<std::filesystem::path> &cacheDir,
                                    const std::filesystem::path &cachePath, bool enableCache,
                                    const epoch_frame::DataFrame &df) const {
  if (!enableCache || !cacheDir || df.empty()) {
    return;
  }

  initializeForCacheDir(*cacheDir);

  // Determine asset and category from path
  std::string pathStr = cachePath.string();
  DataCategory category = DataCategory::DailyBars;

  for (auto cat : {DataCategory::MinuteBars, DataCategory::DailyBars}) {
    if (pathStr.find(DataCategoryWrapper::ToString(cat)) != std::string::npos) {
      category = cat;
      break;
    }
  }

  // Extract asset ID from the cache path filename
  // Note: This assumes the filename is the asset ID without extension
  auto assetId = cachePath.stem().string();
  auto asset = asset::MakeAsset({assetId});  // assetId should be full ID like "AAPL-Stocks"

  CacheWriteParams params{
    .asset = asset,
    .category = category,
    .cacheDir = cacheDir,
    .data = df,
    .enableCache = enableCache
  };

  m_orchestrator->writeToCache(params);
}

epoch_frame::DataFrame
DayBucketCacheProvider::AppendWrite(const std::optional<std::filesystem::path> &cacheDir,
                                     const asset::Asset &asset, DataCategory category,
                                     [[maybe_unused]] std::uint64_t ttlSeconds, bool enableCache,
                                     const epoch_frame::DataFrame &update,
                                     std::filesystem::path *outPath) const {
  if (!cacheDir || !enableCache || update.empty()) {
    return update;
  }

  initializeForCacheDir(*cacheDir);

  // With immutable day buckets, append is just a write
  CacheWriteParams params{
    .asset = asset,
    .category = category,
    .cacheDir = cacheDir,
    .data = update,
    .enableCache = enableCache
  };

  m_orchestrator->writeToCache(params);

  if (outPath && category != DataCategory::MinuteBars) {
    auto catDir = DataCategoryWrapper::ToString(category);
    auto assetClassDir = AssetClassWrapper::ToLongFormString(asset.GetAssetClass());
    *outPath = *cacheDir / catDir / assetClassDir / (asset.GetID() + ".arrow");
  }

  return update;
}

std::expected<epoch_frame::DataFrame, std::string>
DayBucketCacheProvider::LoadWithCache(
    const std::optional<std::filesystem::path> &cacheDir,
    const asset::Asset &asset, DataCategory category,
    std::uint64_t ttlSeconds, bool enableCache,
    const epoch_frame::Date &fromDate, const epoch_frame::Date &toDate,
    const std::function<std::expected<epoch_frame::DataFrame, std::string>(
        const epoch_frame::Date &, const epoch_frame::Date &)> &fetchFn) const {

  if (!cacheDir) {
    // No cache, just fetch
    return fetchFn(fromDate, toDate);
  }

  initializeForCacheDir(*cacheDir);

  // Check environment for intraday freshness requirement
  bool forceRefreshToday = false;
  if (category == DataCategory::MinuteBars) {
    auto intradayFresh = ENV("INTRADAY_ALWAYS_FRESH");
    forceRefreshToday = (intradayFresh == "true" || intradayFresh == "1");

    if (forceRefreshToday) {
      SPDLOG_INFO("Intraday freshness enabled - will fetch today's data from API");
    }
  }

  CacheLoadParams params{
    .asset = asset,
    .category = category,
    .fromDate = fromDate,
    .toDate = toDate,
    .cacheDir = cacheDir,
    .ttlSeconds = ttlSeconds,
    .enableCache = enableCache,
    .forceRefreshToday = forceRefreshToday
  };

  // Create adapter for legacy fetch function
  FetchFunctionAdapter fetcher(asset, category, fetchFn);

  return m_orchestrator->load(params, fetcher);
}

// Async version of LoadWithCache
drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
DayBucketCacheProvider::LoadWithCacheAsync(
    const std::optional<std::filesystem::path> &cacheDir,
    const asset::Asset &asset, DataCategory category,
    std::uint64_t ttlSeconds, bool enableCache,
    const epoch_frame::Date &fromDate, const epoch_frame::Date &toDate,
    const std::function<drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>(
        const epoch_frame::Date &, const epoch_frame::Date &)> &fetchAsyncFn) const {

  if (!cacheDir) {
    co_return std::unexpected("Cache directory not provided");
  }

  initializeForCacheDir(*cacheDir);

  // Check environment for intraday freshness requirement
  bool forceRefreshToday = false;
  if (category == DataCategory::MinuteBars) {
    auto intradayFresh = ENV("INTRADAY_ALWAYS_FRESH");
    forceRefreshToday = (intradayFresh == "true" || intradayFresh == "1");

    if (forceRefreshToday) {
      SPDLOG_INFO("Intraday freshness enabled - will fetch today's data from API");
    }
  }

  CacheLoadParams params{
    .asset = asset,
    .category = category,
    .fromDate = fromDate,
    .toDate = toDate,
    .cacheDir = cacheDir,
    .ttlSeconds = ttlSeconds,
    .enableCache = enableCache,
    .forceRefreshToday = forceRefreshToday
  };

  // Create adapter for async fetch function
  AsyncFetchFunctionAdapter fetcher(asset, category, fetchAsyncFn);

  co_return co_await m_orchestrator->loadAsync(params, fetcher);
}

} // namespace data_sdk::dataloader::cache