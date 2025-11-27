#pragma once
#include <epoch_data_sdk/dataloader/cache/types.hpp>
#include <epoch_data_sdk/dataloader/fetch_kwargs.hpp>
#include <epoch_data_sdk/common/time_provider.hpp>
#include <mutex>
#include <unordered_map>
#include <string>

namespace data_sdk::dataloader::cache {

// Cache manifest for tracking what data is cached and its ranges
class CacheManifest {
public:
  explicit CacheManifest(const std::filesystem::path& manifestPath,
                         TimeProviderPtr timeProvider = nullptr);

  // Check if we have cached data for a specific request
  CacheProbeResult probe(const CacheLoadParams& params) const;

  // Update manifest after successful cache write
  void update(const asset::Asset& asset, DataCategory category,
              const epoch_frame::Date& startDate, const epoch_frame::Date& endDate,
              std::size_t numRows, const FetchKwargs& kwargs = NoKwargs{});

  // Load manifest from disk
  void load();

  // Save manifest to disk
  void save() const;

  // Clear expired entries
  void cleanExpired(std::uint64_t ttlSeconds);

  // Set time provider (mainly for testing)
  void setTimeProvider(TimeProviderPtr provider) { m_timeProvider = provider; }

  // Get manifest file path
  std::filesystem::path getManifestPath() const { return m_manifestPath; }

private:
  std::filesystem::path m_manifestPath;
  mutable std::mutex m_mutex;
  TimeProviderPtr m_timeProvider;

  // Key: "ASSET_SYMBOL:CATEGORY:KWARGS_HASH"
  std::unordered_map<std::string, CacheManifestEntry> m_entries;

  std::string makeKey(const asset::Asset& asset, DataCategory category,
                      const FetchKwargs& kwargs = NoKwargs{}) const;
  std::optional<CacheManifestEntry> getEntry(const asset::Asset& asset,
                                             DataCategory category,
                                             const FetchKwargs& kwargs = NoKwargs{}) const;
};

} // namespace data_sdk::dataloader::cache