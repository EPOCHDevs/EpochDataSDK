#include "cache_utils.h"

#include <epoch_data_sdk/common/bar_attribute.hpp>
#include <epoch_frame/serialization.h>
#include <fstream>
#include <spdlog/spdlog.h>
#include <unistd.h>

namespace data_sdk::dataloader::cache::cache {

static bool Expired(const std::filesystem::path &p, std::uint64_t ttlSeconds) {
  // ttlSeconds == 0 means "do not expire"
  if (ttlSeconds == 0) {
    return false;
  }
  if (!std::filesystem::exists(p)) {
    return true;
  }
  const auto ftime = std::filesystem::last_write_time(p);
  const auto now = decltype(ftime)::clock::now();
  const auto age = now - ftime;
  const auto ageSec =
      std::chrono::duration_cast<std::chrono::seconds>(age).count();
  return static_cast<std::uint64_t>(ageSec) > ttlSeconds;
}

std::filesystem::path MakeCachePath(const std::filesystem::path &dir,
                                    const asset::Asset &asset,
                                    DataCategory category) {
  const auto catDir = DataCategoryWrapper::ToString(category);
  const auto assetClassDir =
      AssetClassWrapper::ToLongFormString(asset.GetAssetClass());
  const auto file = std::format("{}.arrow", asset.GetID());
  auto path = dir / catDir / assetClassDir / file;
  std::filesystem::create_directories(path.parent_path());
  return path;
}

static std::filesystem::path
MakeMetaPath(const std::filesystem::path &cachePath) {
  return std::filesystem::path{cachePath.string() + ".meta"};
}

static void WriteMeta(const std::filesystem::path &metaPath,
                      const epoch_frame::DataFrame &df) {
  if (df.empty()) {
    // Create or truncate empty meta to indicate unusable cache
    std::ofstream(metaPath).close();
    return;
  }
  // FIX: Use actual timestamps, not normalized to midnight
  const auto timestampArray = df.index()->array().to_timestamp_view();
  const auto minTs = timestampArray->Value(0);
  const auto maxTs = timestampArray->Value(static_cast<int64_t>(df.num_rows() - 1));
  const auto rows = static_cast<unsigned long long>(df.num_rows());

  std::ofstream out(metaPath, std::ios::trunc);
  out << "min_ts=" << minTs << "\n";
  out << "max_ts=" << maxTs << "\n";
  out << "rows=" << rows << "\n";
}

static std::optional<CacheMeta>
ReadMeta(const std::filesystem::path &cachePath) {
  CacheMeta meta{};
  meta.cache_path = cachePath;
  meta.meta_path = MakeMetaPath(cachePath);
  if (!std::filesystem::exists(meta.meta_path)) {
    return std::nullopt;
  }
  std::ifstream in(meta.meta_path);
  if (!in.is_open()) {
    return std::nullopt;
  }
  std::string line;
  bool haveMin = false, haveMax = false, haveRows = false;
  while (std::getline(in, line)) {
    auto pos = line.find('=');
    if (pos == std::string::npos)
      continue;
    auto key = line.substr(0, pos);
    auto val = line.substr(pos + 1);
    if (key == "min_ts") {
      meta.covered.start_ts = std::stoll(val);
      haveMin = true;
    } else if (key == "max_ts") {
      meta.covered.end_ts = std::stoll(val);
      haveMax = true;
    } else if (key == "rows") {
      meta.row_count = static_cast<std::uint64_t>(std::stoull(val));
      haveRows = true;
    }
  }
  if (!haveMin || !haveMax || !haveRows) {
    return std::nullopt;
  }
  return meta;
}

std::optional<epoch_frame::DataFrame>
TryLoad(const std::optional<std::filesystem::path> &cacheDir,
        const asset::Asset &asset, DataCategory category,
        std::uint64_t ttlSeconds, std::filesystem::path &cachePathOut) {
  if (!cacheDir) {
    return std::nullopt;
  }
  std::filesystem::create_directories(*cacheDir);
  cachePathOut = MakeCachePath(*cacheDir, asset, category);
  std::filesystem::create_directories(cachePathOut.parent_path());
  if (!std::filesystem::exists(cachePathOut) ||
      Expired(cachePathOut, ttlSeconds)) {
    return std::nullopt;
  }
  auto res = epoch_frame::read_arrow(
      cachePathOut,
      {.index_column =
           data_sdk::EpochStratifyXConstants::instance().TIMESTAMP()});
  if (!res.ok()) {
    return std::nullopt;
  }
  return res.MoveValueUnsafe();
}

std::expected<epoch_frame::DataFrame, CacheRefetchPlan>
TryLoadOrPlan(const std::optional<std::filesystem::path> &cacheDir,
              const asset::Asset &asset, DataCategory category,
              std::uint64_t ttlSeconds,
              const std::optional<TimeRange> &required) {
  using namespace epoch_frame;
  CacheRefetchPlan plan{};
  plan.decision = CacheDecision::FullRefetch;
  if (required) {
    plan.required = *required;
  } else {
    plan.required = {0, 0};
  }
  if (!cacheDir) {
    return std::unexpected(plan);
  }
  std::filesystem::create_directories(*cacheDir);
  const auto cachePath = MakeCachePath(*cacheDir, asset, category);
  const bool exists = std::filesystem::exists(cachePath);
  const bool isExpired = Expired(cachePath, ttlSeconds);
  if (!exists || isExpired) {
    if (auto m = ReadMeta(cachePath)) {
      plan.meta = *m;
    } else {
      plan.meta.cache_path = cachePath;
      plan.meta.meta_path = MakeMetaPath(cachePath);
    }
    // Emit detailed TTL diagnostics at INFO once
    if (exists) {
      try {
        const auto ftime = std::filesystem::last_write_time(cachePath);
        const auto now = decltype(ftime)::clock::now();
        const auto age = now - ftime;
        const auto ageSec =
            std::chrono::duration_cast<std::chrono::seconds>(age).count();
        SPDLOG_INFO("Cache TTL check path={} ttl_s={} age_s={} expired={}",
                    cachePath.string(), ttlSeconds,
                    static_cast<long long>(ageSec), isExpired);
      } catch (const std::exception &e) {
        SPDLOG_WARN("Cache TTL diagnostics failed for path={} error={}",
                    cachePath.string(), e.what());
      } catch (...) {
        SPDLOG_WARN(
            "Cache TTL diagnostics failed for path={} with unknown error",
            cachePath.string());
      }
    }
    if (required) {
      SPDLOG_INFO("Cache MISS cat={} asset={} path={} required=[{}, "
                  "{}] covered=[{}, {}]",
                  DataCategoryWrapper::ToString(category), asset.GetID(),
                  cachePath.string(), plan.required.start_ts,
                  plan.required.end_ts, plan.meta.covered.start_ts,
                  plan.meta.covered.end_ts);
    } else {
      SPDLOG_INFO("Cache MISS cat={} asset={} path={} required=[N/A] "
                  "covered=[{}, {}]",
                  DataCategoryWrapper::ToString(category), asset.GetID(),
                  cachePath.string(), plan.meta.covered.start_ts,
                  plan.meta.covered.end_ts);
    }
    return std::unexpected(plan);
  }

  auto meta = ReadMeta(cachePath);
  if (!meta) {
    // Metadata must be present; otherwise refetch
    plan.meta.cache_path = cachePath;
    plan.meta.meta_path = MakeMetaPath(cachePath);
    return std::unexpected(plan);
  }

  plan.meta = *meta;
  if (required) {
    // FIX: Check BOTH start and end coverage properly
    const bool start_covered = meta->covered.start_ts <= required->start_ts;
    const bool end_covered = meta->covered.end_ts >= required->end_ts;
    const bool coverage_acceptable = start_covered && end_covered;

    if (!coverage_acceptable) {
      SPDLOG_INFO("Cache PARTIAL cat={} asset={} path={} required=[{}, "
                  "{}] covered=[{}, {}]",
                  DataCategoryWrapper::ToString(category), asset.GetID(),
                  cachePath.string(), required->start_ts, required->end_ts,
                  meta->covered.start_ts, meta->covered.end_ts);
      return std::unexpected(plan);
    }
  }

  auto res = epoch_frame::read_arrow(
      cachePath,
      {.index_column =
           data_sdk::EpochStratifyXConstants::instance().TIMESTAMP()});
  if (!res.ok()) {
    SPDLOG_INFO("Cache READ_FAIL cat={} asset={} path={} status={}",
                DataCategoryWrapper::ToString(category), asset.GetID(),
                cachePath.string(), res.status().message());
    return std::unexpected(plan);
  }
  auto df = res.MoveValueUnsafe();
  SPDLOG_INFO("Cache HIT cat={} asset={} path={} rows={} covered=[{}, {}]",
              DataCategoryWrapper::ToString(category), asset.GetID(),
              cachePath.string(), plan.meta.row_count,
              plan.meta.covered.start_ts, plan.meta.covered.end_ts);
  return df;
}

void Write(const std::optional<std::filesystem::path> &cacheDir,
           const std::filesystem::path &cachePath, bool enableCache,
           const epoch_frame::DataFrame &df) {
  if (!enableCache || !cacheDir) {
    return;
  }
  std::filesystem::create_directories(cachePath.parent_path());

  // FIX: Atomic write with temp files
  auto tempDataPath = cachePath.string() + ".tmp." + std::to_string(getpid());
  auto tempMetaPath = MakeMetaPath(cachePath).string() + ".tmp." + std::to_string(getpid());

  try {
    // Write to temp files first
    epoch_frame::AssertStatusIsOk(epoch_frame::write_arrow(
        df, tempDataPath,
        {.include_index = true,
         .index_label =
             data_sdk::EpochStratifyXConstants::instance()
                 .TIMESTAMP()}));

    WriteMeta(tempMetaPath, df);

    // Atomic rename (works even on NFS/EFS)
    std::filesystem::rename(tempMetaPath, MakeMetaPath(cachePath));
    std::filesystem::rename(tempDataPath, cachePath);
  } catch (const std::exception& e) {
    // Cleanup temp files on error
    std::filesystem::remove(tempDataPath);
    std::filesystem::remove(tempMetaPath);
    throw;
  }
  if (!df.empty()) {
    // Use actual timestamps for logging
    const auto timestampArray = df.index()->array().to_timestamp_view();
    const auto startTs = timestampArray->Value(0);
    const auto endTs = timestampArray->Value(static_cast<int64_t>(df.num_rows() - 1));
    SPDLOG_INFO("Cache WRITE path={} rows={} range=[{}, {}]",
                cachePath.string(), df.num_rows(), startTs, endTs);
  } else {
    SPDLOG_INFO("Cache WRITE path={} rows=0", cachePath.string());
  }
}

} // namespace data_sdk::dataloader::cache::cache
