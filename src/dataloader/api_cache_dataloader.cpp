#include "api_cache_dataloader.h"
#include <epoch_data_sdk/common/enums.hpp>
#include "epoch_frame/dataframe.h"

#include "cache/merge_strategy.h"
#include <epoch_data_sdk/model/asset/asset_constants.hpp>
#include <epoch_data_sdk/common/bar_attribute.hpp>
#include <chrono>
#include <common/epoch_thread_pool.h>
#include <epoch_data_sdk/common/env_loader.hpp>
#include <epoch_frame/datetime.h>
#include <epoch_data_sdk/common/async_batch.hpp>
#include <spdlog/spdlog.h>

namespace data_sdk::dataloader {

// Bring cache namespace functions into scope
using cache::normalizeForIntradayMerge;
using cache::normalizeForDailyMerge;

ApiCacheDataloader::ApiCacheDataloader(
    DataloaderOption option, std::shared_ptr<ICacheProvider> cache,
    std::shared_ptr<IFetcherProvider> fetchers)
    : m_option(std::move(option)), m_cacheProvider(std::move(cache)),
      m_fetcherProvider(std::move(fetchers)), m_benchmark(std::nullopt) {}

std::expected<epoch_frame::DataFrame, std::string>
ApiCacheDataloader::LoadAssetBars(const asset::Asset &asset, DataCategory cat,
                                  const IDataFetcher::Parameters& parameters) const {
  SPDLOG_DEBUG("LoadAssetBars: Starting for asset {} category {}",
               asset.GetSymbolStr(), DataCategoryWrapper::ToString(cat));
  using namespace epoch_frame;

  const auto start_time = std::chrono::high_resolution_clock::now();
  const auto fromDate = m_option.GetStartDate();
  const auto toDate = m_option.GetEndDate();
  SPDLOG_DEBUG("LoadAssetBars: Loading {} {} for dates [{} - {}]",
               asset.GetSymbolStr(), DataCategoryWrapper::ToString(cat),
               fromDate.repr(), toDate.repr());

  // Use cleaner parameter struct approach
  // Check environment for intraday freshness requirement
  bool forceRefreshToday = false;
  if (cat == DataCategory::MinuteBars) {
    auto intradayFresh = ENV("INTRADAY_ALWAYS_FRESH");
    forceRefreshToday = (intradayFresh == "true" || intradayFresh == "1");
  }
  auto params = buildCacheParams(asset, cat, forceRefreshToday);

  auto &fetcher = m_fetcherProvider->Get(asset, cat);
  auto res = m_cacheProvider->LoadWithCache(
      params.cacheDir, asset, cat, params.ttlSeconds,
      params.enableCache, fromDate, toDate,
      [&](const epoch_frame::Date &f, const epoch_frame::Date &t) {
        const auto fetch_start = std::chrono::high_resolution_clock::now();
        auto result = fetcher.Fetch(asset, cat, f, t, parameters);
        const auto fetch_end = std::chrono::high_resolution_clock::now();
        const auto fetch_duration =
            std::chrono::duration_cast<std::chrono::milliseconds>(fetch_end -
                                                                  fetch_start);
        SPDLOG_INFO("Fetch timing cat={} asset={} duration={}ms",
                    DataCategoryWrapper::ToString(cat), asset.GetID(),
                    fetch_duration.count());
        return result;
      });

  // Validate that returned data is within the requested date range
  if (res.has_value() && !res->empty()) {
    if (!validateDataRange(*res, fromDate, toDate, asset)) {
      return std::unexpected("Data is outside requested backtest date range");
    }
  }

  const auto end_time = std::chrono::high_resolution_clock::now();
  const auto total_duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end_time -
                                                            start_time);
  SPDLOG_INFO("LoadAssetBars timing cat={} asset={} total_duration={}ms",
              DataCategoryWrapper::ToString(cat), asset.GetID(),
              total_duration.count());
  return res;
}

// Async version of LoadAssetBars
drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
ApiCacheDataloader::LoadAssetBarsAsync(const asset::Asset &asset, DataCategory cat,
                                       IDataFetcher::Parameters parameters) const {
  SPDLOG_DEBUG("LoadAssetBarsAsync: Starting for asset {} category {}",
               asset.GetSymbolStr(), DataCategoryWrapper::ToString(cat));
  using namespace epoch_frame;

  const auto start_time = std::chrono::high_resolution_clock::now();
  const auto fromDate = m_option.GetStartDate();
  const auto toDate = m_option.GetEndDate();
  SPDLOG_DEBUG("LoadAssetBarsAsync: Loading {} {} for dates [{} - {}]",
               asset.GetSymbolStr(), DataCategoryWrapper::ToString(cat),
               fromDate.repr(), toDate.repr());

  // Check environment for intraday freshness requirement
  bool forceRefreshToday = false;
  if (cat == DataCategory::MinuteBars) {
    auto intradayFresh = ENV("INTRADAY_ALWAYS_FRESH");
    forceRefreshToday = (intradayFresh == "true" || intradayFresh == "1");
  }
  auto params = buildCacheParams(asset, cat, forceRefreshToday);

  auto &fetcher = m_fetcherProvider->Get(asset, cat);

  // Use async LoadWithCacheAsync with async lambda
  auto res = co_await m_cacheProvider->LoadWithCacheAsync(
      params.cacheDir, asset, cat, params.ttlSeconds,
      params.enableCache, fromDate, toDate,
      [&fetcher, asset, cat, parameters](const epoch_frame::Date &f, const epoch_frame::Date &t) -> drogon::Task<std::expected<epoch_frame::DataFrame, std::string>> {
        const auto fetch_start = std::chrono::high_resolution_clock::now();
        auto result = co_await fetcher.FetchAsync(asset, cat, f, t, parameters);
        const auto fetch_end = std::chrono::high_resolution_clock::now();
        const auto fetch_duration =
            std::chrono::duration_cast<std::chrono::milliseconds>(fetch_end -
                                                                  fetch_start);
        SPDLOG_INFO("Fetch timing cat={} asset={} duration={}ms",
                    DataCategoryWrapper::ToString(cat), asset.GetID(),
                    fetch_duration.count());
        co_return result;
      });

  // Validate that returned data is within the requested date range
  if (res.has_value() && !res->empty()) {
    if (!validateDataRange(*res, fromDate, toDate, asset)) {
      co_return std::unexpected("Data is outside requested backtest date range");
    }
  }

  const auto end_time = std::chrono::high_resolution_clock::now();
  const auto total_duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end_time -
                                                            start_time);
  SPDLOG_INFO("LoadAssetBarsAsync timing cat={} asset={} total_duration={}ms",
              DataCategoryWrapper::ToString(cat), asset.GetID(),
              total_duration.count());
  co_return res;
}

// Single-category loading: just load primary data, no merging (sync)
std::expected<epoch_frame::DataFrame, std::string>
ApiCacheDataloader::LoadSingleCategory(const asset::Asset& asset) const {
  return LoadAssetBars(asset, m_option.primaryCategory);
}

// Single-category loading: just load primary data, no merging (async)
drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
ApiCacheDataloader::LoadSingleCategoryAsync(const asset::Asset& asset) const {
  co_return co_await LoadAssetBarsAsync(asset, m_option.primaryCategory);
}

// Multi-category loading: load primary + auxiliaries, then merge (sync)
std::expected<epoch_frame::DataFrame, std::string>
ApiCacheDataloader::LoadMultiCategory(const asset::Asset& asset) const {
  // 1. Load primary data
  auto primary_res = LoadAssetBars(asset, m_option.primaryCategory);
  if (!primary_res || primary_res->empty()) {
    return primary_res;
  }

  // 2. Add timestamp column and normalize to dates
  bool is_intraday = IsIntraday(m_option.primaryCategory);
  std::string ts_col = is_intraday ? "minute_timestamp" : "daily_timestamp";

  auto with_ts = AddTimestampColumn(*primary_res, ts_col);
  auto normalized = NormalizeToDates(with_ts);

  // 3. Merge auxiliary data
  return std::expected<epoch_frame::DataFrame, std::string>(
      MergeAuxiliaryData(normalized, asset, is_intraday));
}

// Multi-category loading: load primary + auxiliaries, then merge (async)
drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
ApiCacheDataloader::LoadMultiCategoryAsync(const asset::Asset& asset) const {
  // 1. Load primary data
  auto primary_res = co_await LoadAssetBarsAsync(asset, m_option.primaryCategory);
  if (!primary_res || primary_res->empty()) {
    co_return primary_res;
  }

  // 2. Add timestamp column and normalize to dates
  bool is_intraday = IsIntraday(m_option.primaryCategory);
  std::string ts_col = is_intraday ? "minute_timestamp" : "daily_timestamp";

  auto with_ts = AddTimestampColumn(*primary_res, ts_col);
  auto normalized = NormalizeToDates(with_ts);

  // 3. Merge auxiliary data
  co_return std::expected<epoch_frame::DataFrame, std::string>(
      MergeAuxiliaryData(normalized, asset, is_intraday));
}

// Add timestamp preservation column
epoch_frame::DataFrame ApiCacheDataloader::AddTimestampColumn(
    const epoch_frame::DataFrame& df,
    const std::string& /* column_name */) const {

  // TODO: Implement df.with_column() in EpochFrame
  // Pseudocode for when EpochFrame API is ready:
  //   1. Extract timestamps from index using proper Index API
  //   2. Create new column with extracted timestamps
  //   3. Return df.with_column(column_name, timestamp_column)

  SPDLOG_WARN("AddTimestampColumn: EpochFrame API not available yet - skipping timestamp preservation");
  return df;
}

// Normalize DataFrame index to dates (midnight UTC)
epoch_frame::DataFrame ApiCacheDataloader::NormalizeToDates(
    const epoch_frame::DataFrame& df) const {

  // TODO: Implement df.set_index() in EpochFrame
  // Pseudocode for when EpochFrame API is ready:
  //   1. Extract index values using proper Index API
  //   2. Convert each timestamp to date using extractDate()
  //   3. Return df.set_index(date_index)

  SPDLOG_WARN("NormalizeToDates: EpochFrame API not available yet - returning original index");
  return df;
}

// Merge all auxiliary data into primary DataFrame
epoch_frame::DataFrame ApiCacheDataloader::MergeAuxiliaryData(
    epoch_frame::DataFrame primary,
    const asset::Asset& asset,
    bool is_intraday) const {

  for (const auto& aux_config : m_option.auxiliaryCategories) {
    // Load auxiliary data with parameters
    auto aux_res = LoadAssetBars(asset, aux_config.category, aux_config.parameters);
    if (!aux_res) {
      SPDLOG_WARN("Failed to load auxiliary data {} for {}: {}",
                 DataCategoryWrapper::ToString(aux_config.category),
                 asset.ToString(), aux_res.error());
      continue;
    }

    if (aux_res->empty()) {
      SPDLOG_DEBUG("No auxiliary data {} for {} in date range",
                  DataCategoryWrapper::ToString(aux_config.category), asset.GetSymbolStr());
      continue;
    }

    // Normalize auxiliary data based on strategy
    auto cat_name = DataCategoryWrapper::ToString(aux_config.category);
    epoch_frame::DataFrame normalized_aux;

    if (is_intraday) {
      // Intraday: groupby(date).first()
      normalized_aux = normalizeForIntradayMerge(*aux_res, cat_name);
    } else {
      // Daily: preserve all events
      normalized_aux = normalizeForDailyMerge(*aux_res, cat_name);
    }

    // TODO: Implement df.merge() in EpochFrame
    // For now, skip merging - needs EpochFrame API
    SPDLOG_WARN("MergeAuxiliaryData: Skipping merge for {} - EpochFrame API not available",
               cat_name);
  }

  return primary;
}

void ApiCacheDataloader::LoadData() {
  // Validation
  if (!m_option.IsValid()) {
    throw std::runtime_error("Invalid DataloaderOption: cannot mix MinuteBars and DailyBars");
  }

  SPDLOG_INFO("Starting data loading process");
  auto assets = GetAssets();
  auto all_categories = m_option.GetAllCategories();
  SPDLOG_INFO("Loading data for {} assets across {} categories",
              assets.size(), all_categories.size());

  if (assets.empty()) {
    SPDLOG_ERROR("No assets to load");
    return;
  }

  // Create vector of assets for indexed access
  std::vector<asset::Asset> asset_vec(assets.begin(), assets.end());

  try {
    // Choose processing mode based on configuration
    if (m_option.GetUseBatchFetching()) {
      // Sequential batch processing mode
      const std::size_t batch_size = m_option.GetBatchSize();
      const std::size_t total_assets = asset_vec.size();
      const std::size_t num_batches = (total_assets + batch_size - 1) / batch_size;

      SPDLOG_INFO("Using sequential batch fetching: {} assets in {} batches of size {}",
                  total_assets, num_batches, batch_size);

      // Process assets in sequential batches
      for (std::size_t batch_idx = 0; batch_idx < num_batches; ++batch_idx) {
        const std::size_t start_idx = batch_idx * batch_size;
        const std::size_t end_idx = std::min(start_idx + batch_size, total_assets);
        const std::size_t current_batch_size = end_idx - start_idx;

        SPDLOG_INFO("Processing batch {}/{}: assets {}-{} ({} assets)",
                    batch_idx + 1, num_batches, start_idx + 1, end_idx,
                    current_batch_size);

        // Create tasks for current batch
        std::vector<drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>> batch_tasks;
        batch_tasks.reserve(current_batch_size);

        for (std::size_t i = start_idx; i < end_idx; ++i) {
          const auto& asset = asset_vec[i];
          SPDLOG_DEBUG("Creating task for asset {} ({}/{})",
                      asset.GetSymbolStr(), i + 1, total_assets);

          if (m_option.IsMultiCategory()) {
            batch_tasks.push_back(LoadMultiCategoryAsync(asset));
          } else {
            batch_tasks.push_back(LoadSingleCategoryAsync(asset));
          }
        }

        // Execute current batch concurrently
        SPDLOG_DEBUG("Executing batch {} with {} tasks...", batch_idx + 1, batch_tasks.size());
        auto batch_results = data_sdk::common::syncWhenAll(std::move(batch_tasks));

        // Process batch results
        for (std::size_t i = 0; i < batch_results.size(); ++i) {
          const std::size_t asset_idx = start_idx + i;
          const auto& asset = asset_vec[asset_idx];
          const auto& result = batch_results[i];

          if (!result.has_value()) {
            SPDLOG_ERROR("Failed to load data for {}: {}",
                        asset.ToString(), result.error());
            continue;
          }

          if (result->empty()) {
            SPDLOG_ERROR("No data found for {} in date range [{} - {}]. "
                        "Excluding asset from campaign.",
                        asset.ToString(), m_option.GetStartDate().repr(),
                        m_option.GetEndDate().repr());
            continue;
          }

          // Store result
          SPDLOG_DEBUG("Storing {} rows for asset {}",
                      result->num_rows(), asset.GetSymbolStr());
          m_loadedData[asset] = *result;
        }

        SPDLOG_INFO("Batch {}/{} complete: successfully loaded {}/{} assets",
                    batch_idx + 1, num_batches,
                    batch_results.size(), current_batch_size);
      }

      SPDLOG_INFO("Sequential batch loading complete: {} total assets processed",
                  total_assets);

    } else {
      // Original concurrent processing mode (all at once)
      SPDLOG_INFO("Using full concurrent fetching for {} assets", asset_vec.size());

      std::vector<drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>> tasks;
      tasks.reserve(asset_vec.size());

      for (const auto& asset : asset_vec) {
        SPDLOG_DEBUG("Creating task for asset {} with {} categories",
                    asset.GetSymbolStr(), m_option.GetAllCategories().size());

        // Route to appropriate async loading strategy
        if (m_option.IsMultiCategory()) {
          tasks.push_back(LoadMultiCategoryAsync(asset));
        } else {
          tasks.push_back(LoadSingleCategoryAsync(asset));
        }
      }

      SPDLOG_INFO("Executing {} asset load tasks concurrently with syncWhenAll...", tasks.size());

      // Execute all tasks concurrently
      auto results = data_sdk::common::syncWhenAll(std::move(tasks));

      // Process results
      for (size_t i = 0; i < asset_vec.size(); ++i) {
        const auto& asset = asset_vec[i];
        const auto& result = results[i];

        if (!result.has_value()) {
          SPDLOG_ERROR("Failed to load data for {}: {}",
                      asset.ToString(), result.error());
          continue;
        }

        if (result->empty()) {
          SPDLOG_ERROR("No data found for {} in date range [{} - {}]. "
                      "Excluding asset from campaign.",
                      asset.ToString(), m_option.GetStartDate().repr(),
                      m_option.GetEndDate().repr());
          continue;
        }

        // Store result
        SPDLOG_DEBUG("Storing {} rows for asset {}",
                    result->num_rows(), asset.GetSymbolStr());
        m_loadedData[asset] = *result;
      }

      SPDLOG_INFO("Parallel loading complete");
    }

  } catch (const std::exception& e) {
    SPDLOG_ERROR("Exception in asset loading: {}", e.what());
    throw;
  } catch (...) {
    SPDLOG_ERROR("Unknown exception in asset loading");
    throw;
  }

  // Log summary of loaded assets and validate
  const auto loadedCount = m_loadedData.size();
  const auto totalCount = assets.size();

  SPDLOG_INFO("Successfully loaded data for {}/{} assets", loadedCount, totalCount);

  if (loadedCount == 0) {
    throw std::runtime_error("No assets have data for the specified date range");
  }

  // Log which assets were excluded (thread-safe approach)
  if (loadedCount < totalCount) {
    std::vector<asset::Asset> excludedAssets;
    excludedAssets.reserve(totalCount - loadedCount);

    for (const auto& asset : assets) {
      if (m_loadedData.count(asset) == 0) {  // Use count() instead of find() for safety
        excludedAssets.push_back(asset);
      }
    }

    SPDLOG_WARN("The following {} assets were excluded due to no data:",
                excludedAssets.size());
    for (const auto& asset : excludedAssets) {
      SPDLOG_WARN("  - {}", asset.GetSymbolStr());
    }
  }

  // Log campaign viability report (after parallel loading is complete)
#ifndef NDEBUG
  LogCampaignViability();
#endif

  // Benchmark SPY (daily) - Note: TearSheetDataOption requires benchmark to
  // have same index as equity
  auto benchmarkAsset = asset::AssetConstants::instance().SPY;
  auto dailyOption = m_option; // copy
  dailyOption.SetPrimaryCategory(DataCategory::DailyBars);
  dailyOption.GetAuxiliaryCategories().clear(); // No auxiliaries for benchmark
  ApiCacheDataloader helper(dailyOption, m_cacheProvider, m_fetcherProvider);
  if (auto df = helper.LoadAssetBars(benchmarkAsset, DataCategory::DailyBars)) {
    if (df->empty()) {
      SPDLOG_WARN("No benchmark data found for SPY in date range [{} - {}]. "
                  "Benchmark will be unavailable for performance comparison.",
                  m_option.GetStartDate().repr(), m_option.GetEndDate().repr());
      // Don't set m_benchmark - leave it as std::nullopt
    } else {
      m_benchmark =
          (*df)[data_sdk::EpochStratifyXConstants::instance().CLOSE()]
              .pct_change(1)
              .drop_null()
              .rename("benchmark");
    }
  } else {
    SPDLOG_ERROR("Failed to load benchmark data for SPY: {}. "
                 "Benchmark will be unavailable for performance comparison.",
                 df.error());
    // Don't set m_benchmark - leave it as std::nullopt
  }
}

CacheLoadParams ApiCacheDataloader::buildCacheParams(const asset::Asset& asset,
                                                     DataCategory category,
                                                     bool forceRefreshToday) const {
  return CacheLoadParams{
    .asset = asset,
    .category = category,
    .fromDate = m_option.GetStartDate(),
    .toDate = m_option.GetEndDate(),
    .cacheDir = m_option.GetCacheDir(),
    .ttlSeconds = m_option.GetCacheTTLSeconds(),
    .enableCache = m_option.GetEnableCache(),
    .forceRefreshToday = forceRefreshToday
  };
}


bool ApiCacheDataloader::validateDataRange(const epoch_frame::DataFrame& df,
                                          const epoch_frame::Date& fromDate,
                                          const epoch_frame::Date& toDate,
                                          const asset::Asset& asset) const {
  using namespace epoch_frame;

  const auto timestampArray = df.index()->array().to_timestamp_view();
  const auto dataStartTs = timestampArray->Value(0);
  const auto dataEndTs = timestampArray->Value(static_cast<int64_t>(df.num_rows() - 1));

  // Convert timestamps to dates for comparison
  const auto dataStartDate = DateTime::fromtimestamp(dataStartTs, "UTC").date();
  const auto dataEndDate = DateTime::fromtimestamp(dataEndTs, "UTC").date();

  // Check if data is completely outside requested range (compare by date, not timestamp)
  if (dataStartDate > toDate || dataEndDate < fromDate) {
    SPDLOG_ERROR(
        "Data for {} is completely outside requested range. "
        "Requested: [{} - {}], Got: [{} - {}]. "
        "This indicates forward loading of data outside backtest period.",
        asset.ToString(), fromDate.repr(), toDate.repr(),
        dataStartDate.repr(), dataEndDate.repr());
    return false;
  }

  // Warn if data doesn't fully cover requested range but overlaps
  if (dataStartDate > fromDate || dataEndDate < toDate) {
    SPDLOG_WARN("Data for {} partially covers requested range. "
                "Requested: [{} - {}], Got: [{} - {}]",
                asset.ToString(), fromDate.repr(), toDate.repr(),
                dataStartDate.repr(), dataEndDate.repr());
  }

  return true;
}

void ApiCacheDataloader::LogCampaignViability() const {
  const auto totalRequestedAssets = m_option.GetDataloaderAssets().size();

  SPDLOG_INFO("Campaign Viability Report:");
  SPDLOG_INFO("  - Assets with data: {}/{}", m_loadedData.size(), totalRequestedAssets);
  // Don't report benchmark status here - it hasn't been loaded yet
  // SPDLOG_INFO("  - Benchmark available: {}", m_benchmark.has_value() ? "Yes" : "No");

  if (m_loadedData.size() < 2) {
    SPDLOG_WARN("  - Warning: Only {} asset(s) available - limited diversification",
                m_loadedData.size());
  }

  // Benchmark warning removed - it hasn't been loaded yet at this point

  // Log successful assets
  if (!m_loadedData.empty()) {
    SPDLOG_INFO("  - Assets proceeding to strategy:");
    for (const auto& [asset, df] : m_loadedData) {
      SPDLOG_INFO("    * {} ({} rows)", asset.GetSymbolStr(), df.num_rows());
    }
  }
}

} // namespace data_sdk::dataloader