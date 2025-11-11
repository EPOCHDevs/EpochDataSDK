#pragma once
#include <epoch_data_sdk/common/enums.hpp>
#include <epoch_frame/dataframe.h>
#include <epoch_frame/datetime.h>
#include <epoch_data_sdk/model/asset/asset.hpp>
#include <filesystem>
#include <optional>
#include <expected>
#include <chrono>

namespace data_sdk::dataloader::cache {

// Parameter struct for cache operations
struct CacheLoadParams {
  asset::Asset asset;
  DataCategory category;
  epoch_frame::Date fromDate;
  epoch_frame::Date toDate;
  std::optional<std::filesystem::path> cacheDir;
  std::uint64_t ttlSeconds = 0;
  bool enableCache = true;
  bool forceRefreshToday = false;  // For intraday freshness requirement
};

// Parameter struct for write operations
struct CacheWriteParams {
  asset::Asset asset;
  DataCategory category;
  std::optional<std::filesystem::path> cacheDir;
  epoch_frame::DataFrame data;
  bool enableCache = true;
  bool isAtomic = true;  // Use atomic write with temp file
};

// Cache manifest entry for tracking what's cached
struct CacheManifestEntry {
  asset::Asset asset;
  DataCategory category;
  epoch_frame::Date startDate;
  epoch_frame::Date endDate;
  std::chrono::system_clock::time_point lastUpdated;
  std::size_t numRows = 0;

  bool coversRange(const epoch_frame::Date& from, const epoch_frame::Date& to) const {
    return startDate <= from && endDate >= to;
  }

  bool isExpired(std::uint64_t ttlSeconds, std::chrono::system_clock::time_point now) const {
    if (ttlSeconds == 0) return false;
    auto age = std::chrono::duration_cast<std::chrono::seconds>(now - lastUpdated);
    return static_cast<std::uint64_t>(age.count()) > ttlSeconds;
  }
};

// Result of cache probe operation
struct CacheProbeResult {
  bool hasData = false;
  bool isComplete = false;  // Fully covers requested range
  bool isExpired = false;
  std::optional<epoch_frame::DataFrame> data;
  std::optional<CacheManifestEntry> manifest;
};

// Strategy for determining what needs to be fetched
struct FetchStrategy {
  enum class Type {
    NONE,                // All data is cached
    FULL,                // Need to fetch entire range (no cache or expired)
    APPEND_ONLY,         // Only fetch new data at the end
    PREPEND_ONLY,        // Only fetch early data at the beginning
    PREPEND_AND_APPEND,  // Fetch both early and late data (cache in middle)
    TODAY_ONLY           // Only fetch today's data (intraday freshness)
    // Note: GAP_FILL is impossible with immutable day buckets
    // Once a day is cached, it's never modified
  };

  Type type = Type::NONE;
  std::optional<epoch_frame::Date> fetchFrom;
  std::optional<epoch_frame::Date> fetchTo;
  // For PREPEND_AND_APPEND: separate ranges for prepend and append
  std::optional<epoch_frame::Date> prependFrom;
  std::optional<epoch_frame::Date> prependTo;
};

} // namespace data_sdk::dataloader::cache
