#include "cache_orchestrator.h"
#include <epoch_data_sdk/common/time_provider.hpp>

#include <epoch_frame/serialization.h>
#include <spdlog/spdlog.h>
#include <chrono>
#include <unistd.h>
#include <epoch_frame/common.h>

namespace data_sdk::dataloader::cache {

CacheOrchestrator::CacheOrchestrator(std::shared_ptr<CacheManifest> manifest,
                                   TimeProviderPtr timeProvider)
    : m_manifest(std::move(manifest)),
      m_timeProvider(timeProvider ? timeProvider : std::make_shared<RealTimeProvider>()) {}

std::expected<epoch_frame::DataFrame, std::string>
CacheOrchestrator::load(const CacheLoadParams& params, const IDataFetcher& fetcher) {
  // Step 1: Probe cache to see what we have
  auto probeResult = probeCache(params);
  SPDLOG_DEBUG("CacheOrchestrator: Probe result for {} - hasData={}, isComplete={}, isExpired={}",
              params.asset.GetID(), probeResult.hasData, probeResult.isComplete, probeResult.isExpired);

  // Step 2: Determine fetch strategy
  auto strategy = determineFetchStrategy(params, probeResult);
  SPDLOG_DEBUG("CacheOrchestrator: Strategy for {} = {}",
              params.asset.GetID(),
              strategy.type == FetchStrategy::Type::NONE ? "NONE" :
              strategy.type == FetchStrategy::Type::FULL ? "FULL" :
              strategy.type == FetchStrategy::Type::APPEND_ONLY ? "APPEND_ONLY" :
              strategy.type == FetchStrategy::Type::TODAY_ONLY ? "TODAY_ONLY" : "UNKNOWN");

  // Step 3: Execute fetch if needed
  std::optional<epoch_frame::DataFrame> fetchedData;
  if (strategy.type != FetchStrategy::Type::NONE) {
    auto fetchResult = executeFetch(strategy, params, fetcher);
    if (!fetchResult) {
      // If fetch fails but we have some cached data, use it
      if (probeResult.hasData && probeResult.data) {
        SPDLOG_WARN("Fetch failed, using partial cached data for {}: {}",
                   params.asset.GetID(), fetchResult.error());
        return *probeResult.data;
      }
      return fetchResult;
    }
    fetchedData = *fetchResult;

    // Step 4: Write fetched data to cache
    if (params.enableCache && fetchedData && !fetchedData->empty()) {
      CacheWriteParams writeParams{
        .asset = params.asset,
        .category = params.category,
        .cacheDir = params.cacheDir,
        .data = *fetchedData,
        .enableCache = params.enableCache
      };
      writeToCache(writeParams);
    }
  }

  // Step 5: Merge cached and fetched data
  return mergeData(probeResult, fetchedData);
}

CacheProbeResult CacheOrchestrator::probeCache(const CacheLoadParams& params) const {
  CacheProbeResult result{};

  if (!params.cacheDir || !params.enableCache) {
    return result;
  }

  // Get manifest entry
  result = m_manifest->probe(params);

  // Check for intraday freshness requirement
  if (params.category == DataCategory::MinuteBars && params.forceRefreshToday) {
    auto today = m_timeProvider->today();

    // If requested range includes today, mark as incomplete to force fresh fetch
    if (params.toDate >= today) {
      result.isComplete = false;
      SPDLOG_INFO("Intraday freshness: Will fetch today's data fresh for {}",
                 params.asset.GetID());
    }
  }

  // Load actual data from day buckets if manifest says we have it
  if (result.hasData && !result.isExpired) {
    auto data = loadDayBuckets(params);
    if (data) {
      result.data = data;
    } else {
      result.hasData = false;
      result.isComplete = false;
    }
  }

  return result;
}

FetchStrategy CacheOrchestrator::determineFetchStrategy(const CacheLoadParams& params,
                                                       const CacheProbeResult& probe) const {
  FetchStrategy strategy;

  // Case 1: All data is cached and not expired
  if (probe.isComplete && !probe.isExpired) {
    // Check for intraday freshness requirement
    if (params.category == DataCategory::MinuteBars && params.forceRefreshToday) {
      auto today = m_timeProvider->today();
      if (params.toDate >= today && params.fromDate <= today) {
        strategy.type = FetchStrategy::Type::TODAY_ONLY;
        strategy.fetchFrom = today;
        strategy.fetchTo = today;
        SPDLOG_INFO("Strategy: TODAY_ONLY for {} (intraday freshness)",
                   params.asset.GetID());
        return strategy;
      }
    }

    strategy.type = FetchStrategy::Type::NONE;
    SPDLOG_INFO("Strategy: NONE for {} (all cached)", params.asset.GetID());
    return strategy;
  }

  // Add buffer periods to handle weekend/holiday gaps and provide better data coverage
  auto getBufferedFetchRange = [this](const epoch_frame::Date& fromDate, const epoch_frame::Date& toDate,
                                       DataCategory category) -> std::pair<epoch_frame::Date, epoch_frame::Date> {
    // For minute bars, add 1 week buffer on each side to handle weekends
    // For daily bars, add 1 month buffer to handle holidays and weekends
    const int bufferDays = (category == DataCategory::MinuteBars) ? 7 : 30;

    auto bufferedFrom = fromDate - chrono_days(bufferDays);
    auto bufferedTo = toDate + chrono_days(bufferDays);

    // Don't fetch into the future beyond today
    auto todayDate = m_timeProvider->today();

    if (bufferedTo > todayDate) {
      bufferedTo = todayDate;
    }

    return {bufferedFrom, bufferedTo};
  };

  // Case 2: No cache or expired
  if (!probe.hasData || probe.isExpired) {
    strategy.type = FetchStrategy::Type::FULL;
    auto [bufferedFrom, bufferedTo] = getBufferedFetchRange(params.fromDate, params.toDate, params.category);
    strategy.fetchFrom = bufferedFrom;
    strategy.fetchTo = bufferedTo;
    SPDLOG_INFO("Strategy: FULL (buffered) for {} [{} - {}] (original range: [{} - {}])",
               params.asset.GetID(),
               strategy.fetchFrom->repr(), strategy.fetchTo->repr(),
               params.fromDate.repr(), params.toDate.repr());
    return strategy;
  }

  // Case 3: Partial cache - check if we can append
  if (probe.manifest && !probe.isComplete) {
    // With immutable day buckets, we only append at the end
    if (probe.manifest->startDate <= params.fromDate &&
        probe.manifest->endDate < params.toDate) {
      // We have the beginning but missing the end - can append
      strategy.type = FetchStrategy::Type::APPEND_ONLY;
      strategy.fetchFrom = probe.manifest->endDate + chrono_days(1);
      auto [_, bufferedTo] = getBufferedFetchRange(params.fromDate, params.toDate, params.category);
      strategy.fetchTo = bufferedTo;
      SPDLOG_INFO("Strategy: APPEND_ONLY (buffered) for {} [{} - {}] (original end: {})",
                 params.asset.GetID(),
                 strategy.fetchFrom->repr(), strategy.fetchTo->repr(),
                 params.toDate.repr());
      return strategy;
    }

    // Missing data at beginning - check if we also need to append at end
    // This handles the case where requested range extends beyond cache on both ends
    if (probe.manifest->startDate > params.fromDate) {
      // Check if we also need data at the end
      if (probe.manifest->endDate < params.toDate) {
        // Need data on both sides - prepend AND append
        strategy.type = FetchStrategy::Type::PREPEND_AND_APPEND;

        // Prepend range: from requested start to day before cache starts
        auto [bufferedFrom, _] = getBufferedFetchRange(params.fromDate, params.toDate, params.category);
        strategy.prependFrom = bufferedFrom;
        strategy.prependTo = probe.manifest->startDate - chrono_days(1);

        // Append range: from day after cache ends to requested end
        auto [__, bufferedTo] = getBufferedFetchRange(params.fromDate, params.toDate, params.category);
        strategy.fetchFrom = probe.manifest->endDate + chrono_days(1);
        strategy.fetchTo = bufferedTo;

        SPDLOG_INFO("Strategy: PREPEND_AND_APPEND for {} - prepend [{} - {}], append [{} - {}], cache [{} - {}]",
                   params.asset.GetID(),
                   strategy.prependFrom->repr(), strategy.prependTo->repr(),
                   strategy.fetchFrom->repr(), strategy.fetchTo->repr(),
                   probe.manifest->startDate.repr(), probe.manifest->endDate.repr());
        return strategy;
      } else {
        // Only missing early data - prepend only
        strategy.type = FetchStrategy::Type::PREPEND_ONLY;
        auto [bufferedFrom, _] = getBufferedFetchRange(params.fromDate, params.toDate, params.category);
        strategy.fetchFrom = bufferedFrom;
        strategy.fetchTo = probe.manifest->startDate - chrono_days(1);
        SPDLOG_INFO("Strategy: PREPEND_ONLY for {} [{} - {}] (cache starts at {})",
                   params.asset.GetID(),
                   strategy.fetchFrom->repr(), strategy.fetchTo->repr(),
                   probe.manifest->startDate.repr());
        return strategy;
      }
    }

    // Shouldn't reach here, but fall through to full fetch as safety
    strategy.type = FetchStrategy::Type::FULL;
    auto [bufferedFrom, bufferedTo] = getBufferedFetchRange(params.fromDate, params.toDate, params.category);
    strategy.fetchFrom = bufferedFrom;
    strategy.fetchTo = bufferedTo;
    SPDLOG_INFO("Strategy: FULL fetch (buffered) for {} (unexpected state) [{} - {}]",
               params.asset.GetID(), strategy.fetchFrom->repr(), strategy.fetchTo->repr());
    return strategy;
  }

  // This shouldn't happen - log error and fetch full as safety
  SPDLOG_ERROR("Unexpected cache state for {} - probe incomplete but no manifest. Doing full fetch.",
              params.asset.GetID());
  strategy.type = FetchStrategy::Type::FULL;
  auto [bufferedFrom, bufferedTo] = getBufferedFetchRange(params.fromDate, params.toDate, params.category);
  strategy.fetchFrom = bufferedFrom;
  strategy.fetchTo = bufferedTo;

  return strategy;
}

std::expected<epoch_frame::DataFrame, std::string>
CacheOrchestrator::executeFetch(const FetchStrategy& strategy,
                               const CacheLoadParams& params,
                               const IDataFetcher& fetcher) const {
  if (strategy.type == FetchStrategy::Type::NONE) {
    return std::unexpected("No fetch needed");
  }

  // Special case: PREPEND_AND_APPEND requires two separate fetches
  if (strategy.type == FetchStrategy::Type::PREPEND_AND_APPEND) {
    if (!strategy.prependFrom || !strategy.prependTo || !strategy.fetchFrom || !strategy.fetchTo) {
      return std::unexpected("Invalid PREPEND_AND_APPEND strategy: missing dates");
    }

    // Fetch prepend data
    auto prependResult = fetcher.Fetch(params.asset, params.category,
                                      *strategy.prependFrom, *strategy.prependTo);

    // Fetch append data
    auto appendResult = fetcher.Fetch(params.asset, params.category,
                                     *strategy.fetchFrom, *strategy.fetchTo);

    // Merge both results - prepend data comes first
    if (prependResult && appendResult) {
      std::vector<epoch_frame::FrameOrSeries> frames;
      frames.emplace_back(*prependResult);
      frames.emplace_back(*appendResult);
      auto merged = epoch_frame::concat({.frames = frames});
      SPDLOG_INFO("PREPEND_AND_APPEND: fetched {} prepend + {} append = {} total rows",
                 prependResult->num_rows(), appendResult->num_rows(), merged.num_rows());

      return merged;

    } else if (prependResult) {
      SPDLOG_WARN("Append fetch failed, using only prepend data: {}", appendResult.error());
      return *prependResult;

    } else if (appendResult) {
      SPDLOG_WARN("Prepend fetch failed (likely no early data), using only append data: {}", prependResult.error());
      return *appendResult;

    } else {
      return std::unexpected("Both prepend and append fetches failed");
    }
  }

  if (!strategy.fetchFrom || !strategy.fetchTo) {
    return std::unexpected("Invalid fetch strategy: missing dates");
  }

  const auto fetchStart = std::chrono::high_resolution_clock::now();

  auto result = fetcher.Fetch(params.asset, params.category,
                             *strategy.fetchFrom, *strategy.fetchTo);

  const auto fetchEnd = std::chrono::high_resolution_clock::now();
  const auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      fetchEnd - fetchStart);

  if (result) {
    SPDLOG_INFO("Fetched {} rows for {} [{} - {}] in {}ms",
               result->num_rows(), params.asset.GetID(),
               strategy.fetchFrom->repr(), strategy.fetchTo->repr(),
               duration.count());


  } else {
    SPDLOG_ERROR("Fetch failed for {} [{} - {}]: {}",
                params.asset.GetID(),
                strategy.fetchFrom->repr(), strategy.fetchTo->repr(),
                result.error());
  }

  return result;
}

std::expected<epoch_frame::DataFrame, std::string>
CacheOrchestrator::mergeData(const CacheProbeResult& probe,
                            const std::optional<epoch_frame::DataFrame>& fetched) const {
  // Case 1: Only fetched data
  if (!probe.hasData || !probe.data) {
    if (fetched) {
      return *fetched;
    }
    return std::unexpected("No data available");
  }

  // Case 2: Only cached data
  if (!fetched) {
    return *probe.data;
  }

  // Case 3: Merge cached and fetched
  std::vector<epoch_frame::FrameOrSeries> frames;
  frames.emplace_back(*probe.data);
  frames.emplace_back(*fetched);

  auto merged = epoch_frame::concat({.frames = frames});

  SPDLOG_INFO("Merged {} cached + {} fetched rows = {} total",
             probe.data->num_rows(), fetched->num_rows(), merged.num_rows());

  return merged;
}

void CacheOrchestrator::writeToCache(const CacheWriteParams& params) {
  if (!params.enableCache || !params.cacheDir || params.data.empty()) {
    return;
  }

  if (params.category == DataCategory::MinuteBars) {
    writeDayBuckets(params);
  } else {
    // Write single file for daily+ data
    auto catDir = DataCategoryWrapper::ToString(params.category);
    auto assetClassDir = AssetClassWrapper::ToLongFormString(
        params.asset.GetAssetClass());

    auto cachePath = *params.cacheDir / catDir / assetClassDir /
                    (params.asset.GetID() + ".arrow");

    std::filesystem::create_directories(cachePath.parent_path());

    // Atomic write
    auto tempPath = cachePath.string() + ".tmp." + std::to_string(getpid());

    auto status = epoch_frame::write_arrow(params.data, tempPath, {
      .include_index = true,
      .index_label = std::string(data_sdk::asset::DEFAULT_TIMESTAMP_COLUMN)
    });

    if (status.ok()) {
      std::filesystem::rename(tempPath, cachePath);
      SPDLOG_INFO("Wrote cache: {} ({} rows)",
                 cachePath.string(), params.data.num_rows());

      // Update manifest
      auto timestamps = params.data.index()->array().to_timestamp_view();
      auto startTs = timestamps->Value(0);
      auto endTs = timestamps->Value(params.data.num_rows() - 1);
      auto startDate = epoch_frame::DateTime::fromtimestamp(startTs, "UTC").date();
      auto endDate = epoch_frame::DateTime::fromtimestamp(endTs, "UTC").date();

      m_manifest->update(params.asset, params.category, startDate, endDate,
                        params.data.num_rows());
    } else {
      SPDLOG_ERROR("Failed to write cache {}: {}",
                  cachePath.string(), status.message());
      std::filesystem::remove(tempPath);
    }
  }
}

bool CacheOrchestrator::isToday(const epoch_frame::Date& date) const {
  return date == m_timeProvider->today();
}

std::optional<epoch_frame::DataFrame>
CacheOrchestrator::loadDayBuckets(const CacheLoadParams& params) const {
  if (!params.cacheDir) {
    return std::nullopt;
  }

  std::vector<epoch_frame::DataFrame> dayFrames;
  auto currentDate = params.fromDate;

  while (currentDate <= params.toDate) {
    // Skip today if intraday freshness is required
    if (params.forceRefreshToday && isToday(currentDate)) {
      SPDLOG_DEBUG("Skipping cache for today {} (intraday freshness)",
                  currentDate.repr());
      currentDate += chrono_days(1);
      continue;
    }

    auto catDir = DataCategoryWrapper::ToString(params.category);
    auto assetClassDir = AssetClassWrapper::ToLongFormString(
        params.asset.GetAssetClass());

    auto dayPath = *params.cacheDir / catDir / assetClassDir /
                  params.asset.GetID() /
                  (currentDate.repr() + ".arrow");

    if (std::filesystem::exists(dayPath)) {
      auto res = epoch_frame::read_arrow(dayPath, {
        .index_column = std::string(data_sdk::asset::DEFAULT_TIMESTAMP_COLUMN)
      });

      if (res.ok()) {
        dayFrames.push_back(res.MoveValueUnsafe());
        SPDLOG_DEBUG("Loaded day bucket: {} ({} rows)",
                    dayPath.string(), dayFrames.back().num_rows());
      }
    }

      currentDate += chrono_days(1);
  }

  if (dayFrames.empty()) {
    return std::nullopt;
  }

  if (dayFrames.size() == 1) {
    return dayFrames[0];
  }

  std::vector<epoch_frame::FrameOrSeries> frames;
  for (auto& df : dayFrames) {
    frames.emplace_back(std::move(df));
  }

  return epoch_frame::concat({.frames = frames});
}

void CacheOrchestrator::writeDayBuckets(const CacheWriteParams& params) const {
  if (!params.cacheDir || params.data.empty()) {
    return;
  }

  // Group data by date
  std::map<epoch_frame::Date, std::vector<int64_t>> dateIndices;

  const auto timestampArray = params.data.index()->array().to_timestamp_view();
  for (size_t i = 0; i < params.data.num_rows(); ++i) {
    auto ts = timestampArray->Value(i);
    auto date = epoch_frame::DateTime::fromtimestamp(ts, "UTC").date();

    // Don't cache today's data if it's intraday
    if (params.category == DataCategory::MinuteBars && isToday(date)) {
      SPDLOG_DEBUG("Skipping cache write for today {} (intraday data)",
                  date.repr());
      continue;
    }

    dateIndices[date].push_back(i);
  }

  epoch_frame::Date firstCachedDay;
  epoch_frame::Date lastCachedDay;
  size_t daysWritten = 0;

  // Write each day's data to its own file
  for (const auto& [date, indices] : dateIndices) {
    if (indices.empty()) continue;

    // Extract rows for this day
    auto dayDf = params.data.iloc({indices.front(), indices.back() + 1});

    auto catDir = DataCategoryWrapper::ToString(params.category);
    auto assetClassDir = AssetClassWrapper::ToLongFormString(
        params.asset.GetAssetClass());

    auto dayPath = *params.cacheDir / catDir / assetClassDir /
                  params.asset.GetID() /
                  (date.repr() + ".arrow");

    std::filesystem::create_directories(dayPath.parent_path());

    // Atomic write
    auto tempPath = dayPath.string() + ".tmp." + std::to_string(getpid());

    auto status = epoch_frame::write_arrow(dayDf, tempPath, {
      .include_index = true,
      .index_label = std::string(data_sdk::asset::DEFAULT_TIMESTAMP_COLUMN)
    });

    if (status.ok()) {
      std::filesystem::rename(tempPath, dayPath);
      SPDLOG_DEBUG("Wrote day bucket: {} ({} rows)",
                  dayPath.string(), dayDf.num_rows());
      if (daysWritten == 0) {
        firstCachedDay = date;
      }
      lastCachedDay = date;
      daysWritten++;
    } else {
      SPDLOG_ERROR("Failed to write day bucket {}: {}",
                  dayPath.string(), status.message());
      std::filesystem::remove(tempPath);
    }
  }

  // Update manifest with the continuous date range
  if (daysWritten > 0) {
    m_manifest->update(params.asset, params.category,
                      firstCachedDay, lastCachedDay,
                      params.data.num_rows());

    SPDLOG_INFO("Wrote {} day buckets for {} [{} to {}]",
               daysWritten, params.asset.GetID(),
               firstCachedDay.repr(), lastCachedDay.repr());
  }
}

// Async version of load
drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
CacheOrchestrator::loadAsync(const CacheLoadParams& params, const IDataFetcher& fetcher) {
  // Step 1: Probe cache
  auto probeResult = probeCache(params);
  SPDLOG_DEBUG("CacheOrchestrator: Probe result for {} - hasData={}, isComplete={}, isExpired={}",
              params.asset.GetID(), probeResult.hasData, probeResult.isComplete, probeResult.isExpired);

  // Step 2: Determine fetch strategy
  auto strategy = determineFetchStrategy(params, probeResult);
  SPDLOG_DEBUG("CacheOrchestrator: Strategy for {} = {}",
              params.asset.GetID(),
              strategy.type == FetchStrategy::Type::NONE ? "NONE" :
              strategy.type == FetchStrategy::Type::FULL ? "FULL" :
              strategy.type == FetchStrategy::Type::APPEND_ONLY ? "APPEND_ONLY" :
              strategy.type == FetchStrategy::Type::TODAY_ONLY ? "TODAY_ONLY" : "UNKNOWN");

  // Step 3: Execute fetch if needed
  std::optional<epoch_frame::DataFrame> fetchedData;
  if (strategy.type != FetchStrategy::Type::NONE) {
    auto fetchResult = co_await executeFetchAsync(strategy, params, fetcher);
    if (!fetchResult) {
      // If fetch fails but we have some cached data, use it
      if (probeResult.hasData && probeResult.data) {
        SPDLOG_WARN("Fetch failed, using partial cached data for {}: {}",
                   params.asset.GetID(), fetchResult.error());
        co_return *probeResult.data;
      }
      co_return fetchResult;
    }
    fetchedData = *fetchResult;

    // Step 4: Write fetched data to cache
    if (params.enableCache && fetchedData && !fetchedData->empty()) {
      CacheWriteParams writeParams{
        .asset = params.asset,
        .category = params.category,
        .cacheDir = params.cacheDir,
        .data = *fetchedData,
        .enableCache = params.enableCache
      };
      writeToCache(writeParams);
    }
  }

  // Step 5: Merge cached and fetched data
  co_return mergeData(probeResult, fetchedData);
}

// Async version of executeFetch
drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
CacheOrchestrator::executeFetchAsync(const FetchStrategy& strategy,
                                     const CacheLoadParams& params,
                                     const IDataFetcher& fetcher) const {
  if (strategy.type == FetchStrategy::Type::NONE) {
    co_return std::unexpected("No fetch needed");
  }

  // Special case: PREPEND_AND_APPEND requires two separate fetches
  if (strategy.type == FetchStrategy::Type::PREPEND_AND_APPEND) {
    if (!strategy.prependFrom || !strategy.prependTo || !strategy.fetchFrom || !strategy.fetchTo) {
      co_return std::unexpected("Invalid PREPEND_AND_APPEND strategy: missing dates");
    }

    // Fetch prepend and append data concurrently
    auto prependResult = co_await fetcher.FetchAsync(params.asset, params.category,
                                                     *strategy.prependFrom, *strategy.prependTo);

    auto appendResult = co_await fetcher.FetchAsync(params.asset, params.category,
                                                    *strategy.fetchFrom, *strategy.fetchTo);

    // Merge both results - prepend data comes first
    if (prependResult && appendResult) {
      std::vector<epoch_frame::FrameOrSeries> frames;
      frames.emplace_back(*prependResult);
      frames.emplace_back(*appendResult);
      auto merged = epoch_frame::concat({.frames = frames});
      SPDLOG_INFO("PREPEND_AND_APPEND: fetched {} prepend + {} append = {} total rows",
                 prependResult->num_rows(), appendResult->num_rows(), merged.num_rows());
      co_return merged;
    } else if (prependResult) {
      SPDLOG_WARN("Append fetch failed, using only prepend data: {}", appendResult.error());
      co_return *prependResult;
    } else if (appendResult) {
      SPDLOG_WARN("Prepend fetch failed (likely no early data), using only append data: {}", prependResult.error());
      co_return *appendResult;
    } else {
      co_return std::unexpected("Both prepend and append fetches failed");
    }
  }

  if (!strategy.fetchFrom || !strategy.fetchTo) {
    co_return std::unexpected("Invalid fetch strategy: missing dates");
  }

  const auto fetchStart = std::chrono::high_resolution_clock::now();

  auto result = co_await fetcher.FetchAsync(params.asset, params.category,
                                            *strategy.fetchFrom, *strategy.fetchTo);

  const auto fetchEnd = std::chrono::high_resolution_clock::now();
  const auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      fetchEnd - fetchStart);

  if (!result) {
    SPDLOG_ERROR("Fetch failed for {} [{} - {}]: {}",
                params.asset.GetID(),
                strategy.fetchFrom->repr(), strategy.fetchTo->repr(),
                result.error());
    co_return result;
  }

  SPDLOG_INFO("Fetched {} rows for {} [{} - {}] in {}ms",
             result->num_rows(), params.asset.GetID(),
             strategy.fetchFrom->repr(), strategy.fetchTo->repr(),
             duration.count());

  co_return result;
}

} // namespace data_sdk::dataloader::cache