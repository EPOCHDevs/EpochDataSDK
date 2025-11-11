#include "cache_manifest.h"
#include <epoch_data_sdk/common/time_provider.hpp>
#include <epoch_frame/serialization.h>
#include <spdlog/spdlog.h>
#include <fstream>
#include <glaze/glaze.hpp>
#include <map>

#include <epoch_data_sdk/model/builder/asset_builder.hpp>

namespace data_sdk::dataloader::cache {

// Serializable version of CacheManifestEntry for glaze
struct ManifestEntryJson {
  std::string startDate;
  std::string endDate;
  int64_t lastUpdated;
  std::size_t numRows;
};

CacheManifest::CacheManifest(const std::filesystem::path& manifestPath,
                             TimeProviderPtr timeProvider)
    : m_manifestPath(manifestPath),
      m_timeProvider(timeProvider ? timeProvider : std::make_shared<RealTimeProvider>()) {
  if (std::filesystem::exists(m_manifestPath)) {
    load();
  }
}

CacheProbeResult CacheManifest::probe(const CacheLoadParams& params) const {
  CacheProbeResult result{};

  auto entry = getEntry(params.asset, params.category);
  if (!entry) {
    SPDLOG_DEBUG("No manifest entry for {} {}",
                params.asset.GetID(),
                DataCategoryWrapper::ToString(params.category));
    return result;
  }

  result.manifest = entry;
  result.hasData = true;
  result.isExpired = entry->isExpired(params.ttlSeconds, m_timeProvider->now_timepoint());

  // Check if the cached range covers the requested range
  if (entry->coversRange(params.fromDate, params.toDate)) {
    result.isComplete = true;
    SPDLOG_DEBUG("Manifest: Complete coverage for {} [{} - {}]",
                params.asset.GetID(),
                params.fromDate.repr(), params.toDate.repr());
  } else {
    // With immutable day buckets, we only care about:
    // 1. Do we have data up to the requested start? (if not, need full fetch)
    // 2. Do we need to append data at the end?

    // With immutable day buckets, we just need to check coverage
    // No need to track missing days since we only append at the end
    result.isComplete = entry->coversRange(params.fromDate, params.toDate);

    if (!result.isComplete && params.category == DataCategory::MinuteBars) {
      if (entry->startDate > params.fromDate) {
        SPDLOG_WARN("Manifest: Missing early data for {} (cached from {} but need from {})",
                    params.asset.GetID(), entry->startDate.repr(), params.fromDate.repr());
      } else if (entry->endDate < params.toDate) {
        SPDLOG_DEBUG("Manifest: Need to append data for {} from {} to {}",
                    params.asset.GetID(),
                    (entry->endDate + chrono_days(1)).repr(),
                    params.toDate.repr());
      }
    }
  }

  return result;
}

void CacheManifest::update(const asset::Asset& asset, DataCategory category,
                          const epoch_frame::Date& startDate,
                          const epoch_frame::Date& endDate,
                          std::size_t numRows) {
  std::lock_guard<std::mutex> lock(m_mutex);

  auto key = makeKey(asset, category);

  CacheManifestEntry entry{
    .asset = asset,
    .category = category,
    .startDate = startDate,
    .endDate = endDate,
    .lastUpdated = m_timeProvider->now_timepoint(),
    .numRows = numRows
  };

  // For day-bucketed data, extend the range if we're appending
  if (category == DataCategory::MinuteBars) {
    auto existing = m_entries.find(key);
    if (existing != m_entries.end()) {
      // Since we only append and have no gaps, extend the range
      entry.startDate = std::min(existing->second.startDate, startDate);
      entry.endDate = std::max(existing->second.endDate, endDate);
      entry.numRows = existing->second.numRows + numRows;
    }
  }

  m_entries.insert_or_assign(key, entry);

  SPDLOG_INFO("Updated manifest for {} {}: [{} - {}], {} rows",
             asset.GetID(), DataCategoryWrapper::ToString(category),
             startDate.repr(), endDate.repr(), numRows);

  save();
}

void CacheManifest::load() {
  std::lock_guard lock(m_mutex);

  try {
    std::ifstream file(m_manifestPath, std::ios::binary);
    if (!file.is_open()) {
      SPDLOG_WARN("Cannot open manifest file: {}", m_manifestPath.string());
      return;
    }

    // Read file content
    std::string content((std::istreambuf_iterator<char>(file)),
                       std::istreambuf_iterator<char>());

    // Parse as map of string to ManifestEntryJson
    std::map<std::string, ManifestEntryJson> jsonEntries;
    auto parseResult = glz::read_json(jsonEntries, content);

    if (parseResult) {
      SPDLOG_ERROR("Failed to parse manifest: {}", glz::format_error(parseResult, content));
      return;
    }

    m_entries.clear();

    for (const auto& [key, jsonEntry] : jsonEntries) {
      // Parse asset from key
      auto pos = key.find(':');
      if (pos != std::string::npos) {
        auto assetId = key.substr(0, pos);
        auto categoryStr = key.substr(pos + 1);

        CacheManifestEntry entry{
          .asset=asset::MakeAsset({assetId}),  // assetId is the full ID from manifest
          .category=DataCategoryWrapper::FromString(categoryStr),
        .startDate = epoch_frame::DateTime::from_date_str(jsonEntry.startDate).date(),
        .endDate = epoch_frame::DateTime::from_date_str(jsonEntry.endDate).date(),
          .lastUpdated =  std::chrono::system_clock::time_point(
            std::chrono::milliseconds(jsonEntry.lastUpdated)),
        .numRows=jsonEntry.numRows};

        m_entries.insert_or_assign(key, entry);
      }
    }

    SPDLOG_INFO("Loaded {} entries from manifest", m_entries.size());

  } catch (const std::exception& e) {
    SPDLOG_ERROR("Failed to load manifest: {}", e.what());
    m_entries.clear();
  }
}

void CacheManifest::save() const {
  try {
    std::map<std::string, ManifestEntryJson> jsonEntries;

    for (const auto& [key, entry] : m_entries) {
      ManifestEntryJson jsonEntry;
      jsonEntry.startDate = entry.startDate.repr();
      jsonEntry.endDate = entry.endDate.repr();
      jsonEntry.numRows = entry.numRows;

      // Save timestamp as milliseconds since epoch
      auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
          entry.lastUpdated.time_since_epoch()).count();
      jsonEntry.lastUpdated = ms;

      // No need to save individual days - we use date range

      jsonEntries[key] = jsonEntry;
    }

    // Create directory if it doesn't exist
    std::filesystem::create_directories(m_manifestPath.parent_path());

    // Serialize to JSON
    std::string json = glz::write_json(jsonEntries).value_or("{}");

    // Atomic write
    auto tempPath = m_manifestPath.string() + ".tmp";
    std::ofstream file(tempPath, std::ios::binary);
    file << json;
    file.close();

    std::filesystem::rename(tempPath, m_manifestPath);

    SPDLOG_DEBUG("Saved {} entries to manifest", m_entries.size());

  } catch (const std::exception& e) {
    SPDLOG_ERROR("Failed to save manifest: {}", e.what());
  }
}

void CacheManifest::cleanExpired(std::uint64_t ttlSeconds) {
  if (ttlSeconds == 0) {
    return;
  }

  std::lock_guard<std::mutex> lock(m_mutex);

  std::vector<std::string> toRemove;
  for (const auto& [key, entry] : m_entries) {
    if (entry.isExpired(ttlSeconds, m_timeProvider->now_timepoint())) {
      toRemove.push_back(key);
    }
  }

  for (const auto& key : toRemove) {
    m_entries.erase(key);
    SPDLOG_DEBUG("Removed expired manifest entry: {}", key);
  }

  if (!toRemove.empty()) {
    save();
    SPDLOG_INFO("Cleaned {} expired entries from manifest", toRemove.size());
  }
}

std::string CacheManifest::makeKey(const asset::Asset& asset,
                                  DataCategory category) const {
  return asset.GetID() + ":" + DataCategoryWrapper::ToString(category);
}

std::optional<CacheManifestEntry> CacheManifest::getEntry(const asset::Asset& asset,
                                                         DataCategory category) const {
  std::lock_guard<std::mutex> lock(m_mutex);

  auto key = makeKey(asset, category);
  auto it = m_entries.find(key);

  if (it != m_entries.end()) {
    return it->second;
  }

  return std::nullopt;
}

} // namespace data_sdk::dataloader::cache