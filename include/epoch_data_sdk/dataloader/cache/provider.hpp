#pragma once
#include <epoch_data_sdk/common/enums.hpp>
#include <epoch_frame/dataframe.h>
#include <epoch_frame/datetime.h>
#include <epoch_data_sdk/model/asset/asset.hpp>
#include <drogon/drogon.h>
#include <filesystem>
#include <optional>
#include <expected>
#include <functional>

namespace data_sdk::dataloader::cache {

// Interface for cache providers (e.g., day-bucket cache, single-file cache)
struct ICacheProvider {
  virtual ~ICacheProvider() = default;

  // Try to load data from cache
  virtual std::optional<epoch_frame::DataFrame>
  TryLoad(const std::optional<std::filesystem::path> &cacheDir,
          const asset::Asset &asset, DataCategory category,
          std::uint64_t ttlSeconds,
          std::filesystem::path &cachePathOut) const = 0;

  // Write data to cache
  virtual void Write(const std::optional<std::filesystem::path> &cacheDir,
                     const std::filesystem::path &cachePath, bool enableCache,
                     const epoch_frame::DataFrame &df) const = 0;

  // Load existing (if any, TTL-valid), append-dedupe with 'update', write, return merged
  virtual epoch_frame::DataFrame
  AppendWrite(const std::optional<std::filesystem::path> &cacheDir,
              const asset::Asset &asset, DataCategory category,
              std::uint64_t ttlSeconds, bool enableCache,
              const epoch_frame::DataFrame &update,
              std::filesystem::path *outPath = nullptr) const = 0;

  // Orchestration: probe cache, fetch single contiguous range on miss, and write (sync)
  virtual std::expected<epoch_frame::DataFrame, std::string> LoadWithCache(
      const std::optional<std::filesystem::path> &cacheDir,
      const asset::Asset &asset, DataCategory category,
      std::uint64_t ttlSeconds, bool enableCache,
      const epoch_frame::Date &fromDate, const epoch_frame::Date &toDate,
      const std::function<std::expected<epoch_frame::DataFrame, std::string>(
          const epoch_frame::Date &, const epoch_frame::Date &)> &fetchFn)
      const = 0;

  // Async version: probe cache, fetch single contiguous range on miss, and write
  virtual drogon::Task<std::expected<epoch_frame::DataFrame, std::string>> LoadWithCacheAsync(
      const std::optional<std::filesystem::path> &cacheDir,
      const asset::Asset &asset, DataCategory category,
      std::uint64_t ttlSeconds, bool enableCache,
      const epoch_frame::Date &fromDate, const epoch_frame::Date &toDate,
      const std::function<drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>(
          const epoch_frame::Date &, const epoch_frame::Date &)> &fetchAsyncFn)
      const = 0;
};

} // namespace data_sdk::dataloader::cache
