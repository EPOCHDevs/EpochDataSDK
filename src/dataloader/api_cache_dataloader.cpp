#include "api_cache_dataloader.h"
#include <epoch_data_sdk/common/enums.hpp>
#include "epoch_frame/dataframe.h"

#include "cache/merge_strategy.h"
#include "simple_merger.hpp"
#include "epoch_data_sdk/dataloader/metadata_registry.hpp"
#include <epoch_data_sdk/model/asset/asset_constants.hpp>
#include <epoch_data_sdk/common/bar_attribute.hpp>
#include <chrono>
#include <common/epoch_thread_pool.h>
#include <epoch_data_sdk/common/env_loader.hpp>
#include <epoch_frame/datetime.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_data_sdk/common/async_batch.hpp>
#include <spdlog/spdlog.h>

namespace data_sdk::dataloader {

// Bring cache namespace functions into scope
using cache::normalizeForIntradayMerge;
using cache::normalizeForDailyMerge;

// Helper function to convert ArrowType enum to actual Arrow type
static std::shared_ptr<arrow::DataType> arrowTypeToArrowDataType(ArrowType type) {
  switch (type) {
    case ArrowType::STRING:
      return arrow::utf8();
    case ArrowType::INT32:
      return arrow::int32();
    case ArrowType::INT64:
      return arrow::int64();
    case ArrowType::FLOAT32:
      return arrow::float32();
    case ArrowType::FLOAT64:
      return arrow::float64();
    case ArrowType::TIMESTAMP_NS_UTC:
      return arrow::timestamp(arrow::TimeUnit::NANO, "UTC");
    case ArrowType::BOOLEAN:
      return arrow::boolean();
    default:
      throw std::runtime_error("Unknown ArrowType");
  }
}

// Helper function to create an empty DataFrame with proper schema from metadata
static epoch_frame::DataFrame createEmptyDataFrame(DataCategory category) {
  using namespace epoch_frame;
  using namespace epoch_frame::factory::index;

  // Get metadata for this category
  auto metadata = MetadataRegistry::GetMetadataForCategory(category);

  SPDLOG_DEBUG("Creating empty DataFrame for {} with {} columns and prefix '{}'",
               DataCategoryWrapper::ToString(category),
               metadata.columns.size(),
               metadata.category_prefix);

  // Create empty index (truly empty - 0 rows)
  std::vector<DateTime> empty_dates;
  auto empty_index = make_datetime_index(empty_dates);

  // Create empty arrays for each column with correct type
  std::vector<arrow::ChunkedArrayPtr> column_arrays;
  std::vector<std::string> column_names;

  for (const auto& col_meta : metadata.columns) {
    // Apply category prefix to column name
    std::string col_name = metadata.category_prefix + col_meta.id;
    column_names.push_back(col_name);

    // Create empty array with correct type
    auto arrow_type = arrowTypeToArrowDataType(col_meta.type);
    std::unique_ptr<arrow::ArrayBuilder> builder;
    auto status = arrow::MakeBuilder(arrow::default_memory_pool(), arrow_type, &builder);
    if (!status.ok()) {
      throw std::runtime_error("Failed to create Arrow builder: " + status.ToString());
    }

    // Build empty array
    arrow::ArrayPtr empty_array;
    status = builder->Finish(&empty_array);
    if (!status.ok()) {
      throw std::runtime_error("Failed to build empty array: " + status.ToString());
    }

    // Wrap in ChunkedArray
    auto chunked = std::make_shared<arrow::ChunkedArray>(empty_array);
    column_arrays.push_back(chunked);
  }

  // Create Arrow table from empty arrays
  auto schema = arrow::schema(
    [&]() {
      std::vector<std::shared_ptr<arrow::Field>> fields;
      for (size_t i = 0; i < column_names.size(); ++i) {
        auto arrow_type = arrowTypeToArrowDataType(metadata.columns[i].type);
        fields.push_back(arrow::field(column_names[i], arrow_type, metadata.columns[i].nullable));
      }
      return fields;
    }()
  );

  auto table = arrow::Table::Make(schema, column_arrays);

  // Return DataFrame with empty index and columns
  return DataFrame(empty_index, table);
}

ApiCacheDataloader::ApiCacheDataloader(
    DataloaderOption option, std::shared_ptr<ICacheProvider> cache,
    std::shared_ptr<IFetcherProvider> fetchers,
    std::unique_ptr<IDataMerger> merger)
    : m_option(std::move(option)), m_cacheProvider(std::move(cache)),
      m_fetcherProvider(std::move(fetchers)), m_merger(std::move(merger)),
      m_benchmark(std::nullopt) {

  // Validate options
  if (m_option.categories.empty()) {
    throw std::runtime_error("Invalid DataloaderOption: Empty categories");
  }

  // Cannot mix MinuteBars and DailyBars - they affect the same OHLCV columns
  bool hasMinuteBars = m_option.categories.count(DataCategory::MinuteBars) > 0;
  bool hasDailyBars = m_option.categories.count(DataCategory::DailyBars) > 0;
  if (hasMinuteBars && hasDailyBars) {
    throw std::runtime_error("Invalid DataloaderOption: Cannot mix MinuteBars and DailyBars");
  }

  // If no merger provided, create default SimpleMerger
  if (!m_merger) {
    m_merger = std::make_unique<SimpleMerger>();
  }
}

std::expected<epoch_frame::DataFrame, std::string>
ApiCacheDataloader::LoadAssetBars(const asset::Asset &asset,
                                   DataCategory cat,
                                   const std::unordered_map<std::string, std::string>& parameters) const {
  // Simply delegate to async version and wait for result
  return drogon::sync_wait(LoadAssetBarsAsync(asset, cat, parameters));
}

// Async version of LoadAssetBars
drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
ApiCacheDataloader::LoadAssetBarsAsync(const asset::Asset &asset,
                                        DataCategory cat,
                                        std::unordered_map<std::string, std::string> /* parameters */) const {
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
      [&fetcher, asset, cat](const epoch_frame::Date &f, const epoch_frame::Date &t) -> drogon::Task<std::expected<epoch_frame::DataFrame, std::string>> {
        const auto fetch_start = std::chrono::high_resolution_clock::now();
        auto result = co_await fetcher.FetchAsync(asset, cat, f, t);
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

// Load all categories for a single asset in parallel, then merge
drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
ApiCacheDataloader::LoadAssetDataAsync(const asset::Asset& asset) const {
  const auto& categories = m_option.GetCategories();

  // Single category - no merge needed
  if (categories.size() == 1) {
    co_return co_await LoadAssetBarsAsync(asset, *categories.begin());
  }

  // Multi-category: parallel gather then merge
  SPDLOG_DEBUG("LoadAssetDataAsync: Loading {} categories for asset {}",
               categories.size(), asset.GetSymbolStr());

  // Step 1: Parallel gather - load all categories concurrently
  std::vector<drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>> tasks;
  std::vector<DataCategory> category_list(categories.begin(), categories.end());
  tasks.reserve(category_list.size());

  for (const auto& cat : category_list) {
    tasks.push_back(LoadAssetBarsAsync(asset, cat));
  }

  // Wait for all category fetches to complete
  auto results = co_await data_sdk::common::when_all(std::move(tasks));

  // Step 2: Build category_data map from results with column prefixing
  std::unordered_map<DataCategory, epoch_frame::DataFrame> category_data;
  std::vector<std::pair<DataCategory, epoch_frame::DataFrame>> empty_categories;

  for (std::size_t i = 0; i < results.size(); ++i) {
    const auto& result = results[i];
    const auto& cat = category_list[i];

    if (!result.has_value()) {
      SPDLOG_WARN("Failed to load {} for {}: {} - creating empty DataFrame with schema",
                  DataCategoryWrapper::ToString(cat),
                  asset.GetSymbolStr(),
                  result.error());
      // Create empty DataFrame even for failed categories to ensure schema consistency
      auto empty_df = createEmptyDataFrame(cat);
      empty_categories.emplace_back(cat, empty_df);
      continue;
    }

    // Apply column prefixing if needed
    auto df = *result;

    // If empty, create empty DataFrame with proper schema and save for later
    if (df.empty()) {
      SPDLOG_INFO("No {} data for {} in date range - creating empty DataFrame with schema",
                   DataCategoryWrapper::ToString(cat),
                   asset.GetSymbolStr());
      df = createEmptyDataFrame(cat);
      SPDLOG_INFO("Created empty {} DataFrame with {} columns",
                   DataCategoryWrapper::ToString(cat),
                   df.num_cols());
      // Note: prefix already applied in createEmptyDataFrame
      // Store for later reindexing after we know the final index
      empty_categories.emplace_back(cat, df);
      continue;
    }

    const auto metadata = MetadataRegistry::GetMetadataForCategory(cat);
    if (!metadata.category_prefix.empty()) {
      SPDLOG_DEBUG("Applying prefix '{}' to {} columns for {}",
                   metadata.category_prefix,
                   DataCategoryWrapper::ToString(cat),
                   asset.GetSymbolStr());
      df = df.add_prefix(metadata.category_prefix);
    }

    category_data[cat] = df;
  }

  // First merge non-empty categories to get the final index
  epoch_frame::DataFrame merged_df;
  if (!category_data.empty()) {
    auto merge_result = m_merger->Merge(category_data);
    if (!merge_result) {
      co_return std::unexpected("Merge failed: " + merge_result.error());
    }
    merged_df = *merge_result;
  }

  // Now reindex empty categories to match the merged index and add them as columns
  if (!empty_categories.empty()) {
    if (category_data.empty()) {
      // All categories were empty - this is an error
      co_return std::unexpected("No data available for any category");
    }

    SPDLOG_INFO("Reindexing {} empty categories to match merged index of {} rows",
                empty_categories.size(), merged_df.num_rows());

    for (const auto& [cat, empty_df] : empty_categories) {
      SPDLOG_INFO("Adding {} empty columns to merged index of {} rows",
                  DataCategoryWrapper::ToString(cat),
                  merged_df.num_rows());

      // Add each column from empty DataFrame as null column with merged_df's index
      for (const auto& col_name : empty_df.column_names()) {
        // Create null array matching merged_df's row count
        auto null_builder = std::make_shared<arrow::NullBuilder>();
        for (size_t i = 0; i < merged_df.num_rows(); ++i) {
          auto status = null_builder->AppendNull();
          if (!status.ok()) {
            throw std::runtime_error("Failed to append null: " + status.ToString());
          }
        }

        arrow::ArrayPtr null_array;
        auto status = null_builder->Finish(&null_array);
        if (!status.ok()) {
          throw std::runtime_error("Failed to finish null array: " + status.ToString());
        }

        // Create Series with the same index as merged_df
        auto null_series = epoch_frame::Series(merged_df.index(), std::make_shared<arrow::ChunkedArray>(null_array), col_name);

        // Add to merged DataFrame
        merged_df = merged_df.assign(col_name, null_series);
      }

      SPDLOG_INFO("Added {} columns from {} (all null)",
                  empty_df.num_cols(),
                  DataCategoryWrapper::ToString(cat));
    }
  }

  // Check if we have any data
  if (category_data.empty() && empty_categories.empty()) {
    co_return std::unexpected("No data available for any category");
  }

  co_return merged_df;
}

void ApiCacheDataloader::LoadData() {
  // Validation is done in constructor

  SPDLOG_INFO("Starting data loading process");
  auto assets = GetAssets();
  auto all_categories = m_option.GetAllCategories();
  SPDLOG_INFO("Loading data for {} assets across {} categories",
              assets.size(), all_categories.size());

  if (assets.empty()) {
    SPDLOG_ERROR("No assets to load");
    throw std::runtime_error("No assets to load");
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

          batch_tasks.push_back(LoadAssetDataAsync(asset));
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

        tasks.push_back(LoadAssetDataAsync(asset));
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
  dailyOption.SetCategories({DataCategory::DailyBars}); // Only DailyBars for benchmark
  ApiCacheDataloader helper(dailyOption, m_cacheProvider, m_fetcherProvider, std::make_unique<SimpleMerger>());
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