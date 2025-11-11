#pragma once
#include <epoch_data_sdk/common/enums.hpp>
#include <epoch_data_sdk/model/asset/asset.hpp>
#include <cstdint>
#include <epoch_frame/dataframe.h>
#include <expected>
#include <filesystem>
#include <optional>

namespace data_sdk::dataloader::cache::cache {

struct TimeRange {
  std::int64_t start_ts{0};
  std::int64_t end_ts{0};
};

enum class CacheDecision { Hit, FullRefetch };

struct CacheMeta {
  TimeRange covered{};
  std::uint64_t row_count{0};
  std::filesystem::path cache_path;
  std::filesystem::path meta_path;
};

struct CacheRefetchPlan {
  CacheDecision decision{CacheDecision::FullRefetch};
  CacheMeta meta{};
  TimeRange required{0, 0};
};

// Compose a cache file path for an asset/category pair
std::filesystem::path MakeCachePath(const std::filesystem::path &dir,
                                    const asset::Asset &asset,
                                    DataCategory category);

// Attempt to load a cached frame if present and not older than ttlSeconds.
// Returns std::nullopt if cache is missing/expired/unreadable.
std::optional<epoch_frame::DataFrame>
TryLoad(const std::optional<std::filesystem::path> &cacheDir,
        const asset::Asset &asset, DataCategory category,
        std::uint64_t ttlSeconds, std::filesystem::path &cachePathOut);

// Probe cache and either return DataFrame (Hit) or a plan to full-refetch
std::expected<epoch_frame::DataFrame, CacheRefetchPlan>
TryLoadOrPlan(const std::optional<std::filesystem::path> &cacheDir,
              const asset::Asset &asset, DataCategory category,
              std::uint64_t ttlSeconds,
              const std::optional<TimeRange> &required);

// Write a dataframe to cache if enabled and cacheDir is set.
void Write(const std::optional<std::filesystem::path> &cacheDir,
           const std::filesystem::path &cachePath, bool enableCache,
           const epoch_frame::DataFrame &df);

} // namespace data_sdk::dataloader::cache::cache
