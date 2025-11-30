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
  auto empty_index = make_datetime_index(empty_dates, "", "UTC");

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
      m_fetcherProvider(std::move(fetchers)),
      m_merger(std::move(merger)),
      m_benchmark(std::nullopt) {

  // Validate options using the new unified API
  if (!m_option.IsValid()) {
    if (m_option.GetRequests().empty()) {
      throw std::runtime_error("Invalid DataloaderOption: Empty requests");
    }
    // IsValid also checks for MinuteBars + DailyBars conflict
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
                                   const FetchKwargs& kwargs) const {
  // Simply delegate to async version and wait for result
  return drogon::sync_wait(LoadAssetBarsAsync(asset, cat, kwargs));
}

// Async version of LoadAssetBars
drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
ApiCacheDataloader::LoadAssetBarsAsync(const asset::Asset &asset,
                                        DataCategory cat,
                                        FetchKwargs kwargs) const {
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
      [&fetcher, asset, cat, kwargs](const epoch_frame::Date &f, const epoch_frame::Date &t) -> drogon::Task<std::expected<epoch_frame::DataFrame, std::string>> {
        const auto fetch_start = std::chrono::high_resolution_clock::now();
        auto result = co_await fetcher.FetchAsync(asset, cat, f, t, kwargs);
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

  // Get only per-asset requests (excludes EconomicIndicator and Indices)
  const auto assetRequests = m_option.GetAssetRequests();

  // Single category - no merge needed
  if (assetRequests.size() == 1) {
    const auto& req = assetRequests[0];
    co_return co_await LoadAssetBarsAsync(asset, req.category, req.kwargs);
  }

  // Multi-category: parallel gather then merge
  SPDLOG_DEBUG("LoadAssetDataAsync: Loading {} categories for asset {}",
               assetRequests.size(), asset.GetSymbolStr());

  // Step 1: Parallel gather - load all categories concurrently
  std::vector<drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>> tasks;
  tasks.reserve(assetRequests.size());

  for (const auto& req : assetRequests) {
    tasks.push_back(LoadAssetBarsAsync(asset, req.category, req.kwargs));
  }

  // Wait for all category fetches to complete
  auto results = co_await data_sdk::common::when_all(std::move(tasks));

  // Step 2: Build category_data map from results with column prefixing
  std::unordered_map<std::string, epoch_frame::DataFrame> category_data;
  std::vector<std::pair<std::string, epoch_frame::DataFrame>> empty_categories;

  for (std::size_t i = 0; i < results.size(); ++i) {
    const auto& result = results[i];
    const auto& cat = assetRequests[i].category;
    std::string cat_key = DataCategoryWrapper::ToString(cat);

    if (!result.has_value()) {
      SPDLOG_WARN("Failed to load {} for {}: {} - creating empty DataFrame with schema",
                  cat_key,
                  asset.GetSymbolStr(),
                  result.error());
      // Create empty DataFrame even for failed categories to ensure schema consistency
      auto empty_df = createEmptyDataFrame(cat);
      empty_categories.emplace_back(cat_key, empty_df);
      continue;
    }

    // Apply column prefixing if needed
    auto df = *result;

    // If empty, create empty DataFrame with proper schema and save for later
    if (df.empty()) {
      SPDLOG_INFO("No {} data for {} in date range - creating empty DataFrame with schema",
                   cat_key,
                   asset.GetSymbolStr());
      df = createEmptyDataFrame(cat);
      SPDLOG_INFO("Created empty {} DataFrame with {} columns",
                   cat_key,
                   df.num_cols());
      // Note: prefix already applied in createEmptyDataFrame
      // Store for later reindexing after we know the final index
      empty_categories.emplace_back(cat_key, df);
      continue;
    }

    const auto metadata = MetadataRegistry::GetMetadataForCategory(cat);
    if (!metadata.category_prefix.empty()) {
      SPDLOG_DEBUG("Applying prefix '{}' to {} columns for {}",
                   metadata.category_prefix,
                   cat_key,
                   asset.GetSymbolStr());
      df = df.add_prefix(metadata.category_prefix);
    }

    category_data[cat_key] = df;
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
                  cat,
                  merged_df.num_rows());

      // Add each column from empty DataFrame as typed null column with merged_df's index
      for (const auto& col_name : empty_df.column_names()) {
        // Get the proper type from empty_df schema instead of using NullType
        auto schema = empty_df.table()->schema();
        auto field_idx = schema->GetFieldIndex(col_name);
        if (field_idx == -1) {
          throw std::runtime_error("Column not found in empty DataFrame: " + col_name);
        }
        auto col_type = schema->field(field_idx)->type();

        // Create typed builder that preserves the schema
        std::unique_ptr<arrow::ArrayBuilder> builder;
        auto status = arrow::MakeBuilder(arrow::default_memory_pool(), col_type, &builder);
        if (!status.ok()) {
          throw std::runtime_error("Failed to create typed builder: " + status.ToString());
        }

        // Append nulls using the typed builder (maintains type, not NullType)
        for (size_t i = 0; i < merged_df.num_rows(); ++i) {
          status = builder->AppendNull();
          if (!status.ok()) {
            throw std::runtime_error("Failed to append null: " + status.ToString());
          }
        }

        arrow::ArrayPtr typed_null_array;
        status = builder->Finish(&typed_null_array);
        if (!status.ok()) {
          throw std::runtime_error("Failed to finish typed null array: " + status.ToString());
        }

        // Create Series with the same index as merged_df
        auto null_series = epoch_frame::Series(merged_df.index(), std::make_shared<arrow::ChunkedArray>(typed_null_array), col_name);

        // Add to merged DataFrame
        merged_df = merged_df.assign(col_name, null_series);
      }

      SPDLOG_INFO("Added {} columns from {} (all null)",
                  empty_df.num_cols(),
                  cat);
    }
  }

  // Check if we have any data
  if (category_data.empty() && empty_categories.empty()) {
    co_return std::unexpected("No data available for any category");
  }

  co_return merged_df;
}

void ApiCacheDataloader::LoadData(events::ScopedProgressEmitter& emitter) {
  // Validation is done in constructor

  SPDLOG_INFO("Starting data loading process");
  auto assets = GetAssets();
  auto categories = m_option.GetCategories();
  SPDLOG_INFO("Loading data for {} assets across {} categories",
              assets.size(), categories.size());

  if (assets.empty()) {
    SPDLOG_ERROR("No assets to load");
    throw std::runtime_error("No assets to load");
  }

  std::size_t nodesSucceeded = 0;
  std::size_t nodesFailed = 0;
  std::size_t nodesSkipped = 0;

  // Start lifecycle event

    emitter.SetContext("total_assets", static_cast<int64_t>(assets.size()));
    emitter.SetContext("categories", static_cast<int64_t>(categories.size()));
    emitter.EmitStarted("dataloader", "LoadData");


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
            nodesFailed++;
            continue;
          }

          if (result->empty()) {
            SPDLOG_ERROR("No data found for {} in date range [{} - {}]. "
                        "Excluding asset from campaign.",
                        asset.ToString(), m_option.GetStartDate().repr(),
                        m_option.GetEndDate().repr());
            nodesFailed++;
            continue;
          }

          // Store result
          SPDLOG_DEBUG("Storing {} rows for asset {}",
                      result->num_rows(), asset.GetSymbolStr());
          m_loadedData[asset] = *result;
          nodesSucceeded++;
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
                    asset.GetSymbolStr(), categories.size());

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
          nodesFailed++;
          continue;
        }

        if (result->empty()) {
          SPDLOG_ERROR("No data found for {} in date range [{} - {}]. "
                      "Excluding asset from campaign.",
                      asset.ToString(), m_option.GetStartDate().repr(),
                      m_option.GetEndDate().repr());
          nodesFailed++;
          continue;
        }

        // Store result
        SPDLOG_DEBUG("Storing {} rows for asset {}",
                    result->num_rows(), asset.GetSymbolStr());
        m_loadedData[asset] = *result;
        nodesSucceeded++;
      }

      SPDLOG_INFO("Parallel loading complete");
    }

  } catch (const std::exception& e) {
    SPDLOG_ERROR("Exception in asset loading: {}", e.what());
    emitter.EmitFailed("dataloader", "LoadData", e.what());
    throw;
  } catch (...) {
    SPDLOG_ERROR("Unknown exception in asset loading");
    emitter.EmitFailed("dataloader", "LoadData", "unknown exception");
    throw;
  }

  // Log summary of loaded assets and validate
  const auto loadedCount = m_loadedData.size();
  const auto totalCount = assets.size();

  SPDLOG_INFO("Successfully loaded data for {}/{} assets", loadedCount, totalCount);

  if (loadedCount == 0) {
    emitter.EmitFailed("dataloader", "LoadData",
        "No assets have data for the specified date range");
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

  emitter.SetContext("assets_succeeded", static_cast<int64_t>(nodesSucceeded));
  emitter.SetContext("assets_failed", static_cast<int64_t>(nodesFailed));
  emitter.SetContext("assets_skipped", static_cast<int64_t>(nodesSkipped));
  emitter.EmitCompleted("dataloader", "LoadData");

  // Log campaign viability report (after parallel loading is complete)
#ifndef NDEBUG
  LogCampaignViability();
#endif

  // Load cross-sectional data (EconomicIndicator and ReferenceAgg) if requested
  const auto crossSectionalRequests = m_option.GetCrossSectionalRequests();
  if (!crossSectionalRequests.empty() && !m_loadedData.empty()) {
    SPDLOG_INFO("Loading {} cross-sectional data requests to merge with {} assets",
                crossSectionalRequests.size(), m_loadedData.size());

    // Separate economic indicators and reference aggs
    std::vector<DataRequest> economicRequests;
    std::vector<DataRequest> refAggRequests;

    for (const auto& req : crossSectionalRequests) {
      if (req.category == DataCategory::EconomicIndicator) {
        economicRequests.push_back(req);
      } else if (req.category == DataCategory::ReferenceAgg) {
        refAggRequests.push_back(req);
      }
    }

    // Load economic indicators
    std::unordered_map<std::string, epoch_frame::DataFrame> indicators;
    if (!economicRequests.empty()) {
      std::vector<drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>> econ_tasks;
      econ_tasks.reserve(economicRequests.size());

      for (const auto& req : economicRequests) {
        const auto* kw = std::get_if<EconomicIndicatorKwargs>(&req.kwargs);
        if (kw) {
          econ_tasks.push_back(LoadEconomicIndicatorAsync(
              kw->indicator, m_option.GetStartDate(), m_option.GetEndDate(), kw->use_alfred));
        }
      }

      auto econ_results = data_sdk::common::syncWhenAll(std::move(econ_tasks));

      for (size_t i = 0; i < economicRequests.size(); ++i) {
        const auto* kw = std::get_if<EconomicIndicatorKwargs>(&economicRequests[i].kwargs);
        if (!kw) continue;

        std::string indicator_name = kw->getName();
        const auto& result = econ_results[i];
        if (!result.has_value()) {
          SPDLOG_ERROR("Failed to load economic indicator {}: {}", indicator_name, result.error());
          continue;
        }
        if (result->empty()) {
          SPDLOG_WARN("No data found for {} in date range", indicator_name);
          continue;
        }

        SPDLOG_INFO("Loaded {} rows for economic indicator: {}", result->num_rows(), indicator_name);
        indicators[indicator_name] = *result;
      }
    }

    // Load reference aggregates (Stocks, FX, Crypto, Indices)
    // Determine is_eod from primary category
    bool is_eod = (m_option.GetPrimaryCategory() != DataCategory::MinuteBars);
    std::unordered_map<std::string, std::pair<epoch_frame::DataFrame, ReferenceAggKwargs>> refAggs;
    if (!refAggRequests.empty()) {
      std::vector<drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>> ref_tasks;
      std::vector<ReferenceAggKwargs> ref_fetch_kwargs;
      ref_tasks.reserve(refAggRequests.size());
      ref_fetch_kwargs.reserve(refAggRequests.size());

      for (const auto& req : refAggRequests) {
        const auto* kw = std::get_if<ReferenceAggKwargs>(&req.kwargs);
        if (kw) {
          // Create kwargs with is_eod set from primary category
          ReferenceAggKwargs fetch_kwargs = *kw;
          fetch_kwargs.is_eod = is_eod;
          ref_fetch_kwargs.push_back(fetch_kwargs);
          ref_tasks.push_back(LoadReferenceAggDataAsync(fetch_kwargs));
        }
      }

      auto ref_results = data_sdk::common::syncWhenAll(std::move(ref_tasks));

      for (size_t i = 0; i < ref_fetch_kwargs.size(); ++i) {
        const auto& kw = ref_fetch_kwargs[i];
        const auto& result = ref_results[i];
        if (!result.has_value()) {
          SPDLOG_ERROR("Failed to load reference agg {}: {}", kw.ticker, result.error());
          continue;
        }
        if (result->empty()) {
          SPDLOG_WARN("No data found for reference agg {} in date range", kw.ticker);
          continue;
        }

        SPDLOG_INFO("Loaded {} rows for reference agg: {} ({})",
                    result->num_rows(), kw.ticker,
                    epoch_core::AssetClassWrapper::ToString(kw.asset_class));
        // Store both DataFrame and kwargs for column prefix generation
        std::string ref_key = kw.getColumnPrefix() + kw.ticker;
        if (!kw.is_eod) {
          ref_key += ":minute";  // mark intraday for correct metadata (non-normalized index)
        }
        refAggs[ref_key] = {*result, kw};
      }
    }

    // Merge indicators and reference aggs into each asset's DataFrame
    if (!indicators.empty() || !refAggs.empty()) {
      SPDLOG_INFO("Merging {} indicators and {} reference aggs into {} assets",
                  indicators.size(), refAggs.size(), m_loadedData.size());

      for (auto& [asset, asset_df] : m_loadedData) {
        std::unordered_map<std::string, epoch_frame::DataFrame> merge_map;

        // Add current asset data
        auto base_category = m_option.GetPrimaryCategory();
        std::string base_key = DataCategoryWrapper::ToString(base_category);
        merge_map[base_key] = asset_df;

        // Add economic indicators with prefix
        for (const auto& [series_id, indicator_df] : indicators) {
          std::string prefix = "ECON:" + series_id + ":";
          SPDLOG_DEBUG("Indicator {} before add_prefix: {} rows, {} cols, index size {}",
                      series_id, indicator_df.num_rows(), indicator_df.num_cols(),
                      indicator_df.index()->size());
          auto prefixed_df = indicator_df.add_prefix(prefix);
          SPDLOG_DEBUG("Indicator {} after add_prefix: {} rows, {} cols, index size {}",
                      series_id, prefixed_df.num_rows(), prefixed_df.num_cols(),
                      prefixed_df.index()->size());
          merge_map[series_id] = prefixed_df;
        }

        // Add reference aggs with asset class specific prefix
        for (const auto& [key, ref_pair] : refAggs) {
          const auto& [ref_df, ref_kwargs] = ref_pair;
          // Column prefix should be "IDX:SPX:" so columns become "IDX:SPX:c"
          std::string prefix = ref_kwargs.getColumnPrefix() + ref_kwargs.ticker + ":";
          auto prefixed_df = ref_df.add_prefix(prefix);
          merge_map[key] = prefixed_df;
        }

        // Merge all data
        auto merge_result = m_merger->Merge(merge_map);
        if (!merge_result.has_value()) {
          SPDLOG_ERROR("Failed to merge cross-sectional data into {}: {}",
                      asset.GetSymbolStr(), merge_result.error());
          continue;
        }

        m_loadedData[asset] = *merge_result;

        SPDLOG_DEBUG("Merged cross-sectional data into {} ({} total columns)",
                    asset.GetSymbolStr(), m_loadedData[asset].num_cols());
      }

      SPDLOG_INFO("Cross-sectional data merge complete");
    }
  }

  // Benchmark SPY (daily)
  auto benchmarkAsset = asset::AssetConstants::instance().SPY;
  auto dailyOption = m_option;
  dailyOption.requests.clear();
  dailyOption.AddRequest(DataCategory::DailyBars);

  ApiCacheDataloader helper(dailyOption, m_cacheProvider, m_fetcherProvider, std::make_unique<SimpleMerger>());
  if (auto df = helper.LoadAssetBars(benchmarkAsset, DataCategory::DailyBars)) {
    if (df->empty()) {
      SPDLOG_WARN("No benchmark data found for SPY in date range [{} - {}]. "
                  "Benchmark will be unavailable for performance comparison.",
                  m_option.GetStartDate().repr(), m_option.GetEndDate().repr());
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

  if (m_loadedData.size() < 2) {
    SPDLOG_WARN("  - Warning: Only {} asset(s) available - limited diversification",
                m_loadedData.size());
  }

  // Log successful assets
  if (!m_loadedData.empty()) {
    SPDLOG_INFO("  - Assets proceeding to strategy:");
    for (const auto& [asset, df] : m_loadedData) {
      SPDLOG_INFO("    * {} ({} rows)", asset.GetSymbolStr(), df.num_rows());
    }
  }
}

// Economic indicator methods (FRED series)
std::expected<epoch_frame::DataFrame, std::string>
ApiCacheDataloader::LoadEconomicIndicator(CrossSectionalDataCategory indicator,
                                          const epoch_frame::Date& fromDate,
                                          const epoch_frame::Date& toDate,
                                          bool use_alfred) const {
  return drogon::sync_wait(LoadEconomicIndicatorAsync(indicator, fromDate, toDate, use_alfred));
}

drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
ApiCacheDataloader::LoadEconomicIndicatorAsync(CrossSectionalDataCategory indicator,
                                               const epoch_frame::Date& fromDate,
                                               const epoch_frame::Date& toDate,
                                               bool use_alfred) const {
  EconomicIndicatorKwargs kwargs{indicator, use_alfred};
  std::string indicator_name = kwargs.getName();
  std::string series_id = kwargs.getSeriesId();

  SPDLOG_DEBUG("LoadEconomicIndicatorAsync: Starting for {} ({})", indicator_name, series_id);

  const auto start_time = std::chrono::high_resolution_clock::now();

  // Use the unified fetcher with EconomicIndicator category
  auto& fetcher = m_fetcherProvider->Get(DataCategory::EconomicIndicator);

  // Use asset-less fetch for cross-sectional data
  auto result = co_await fetcher.FetchAsync(
      DataCategory::EconomicIndicator, fromDate, toDate, kwargs);

  const auto end_time = std::chrono::high_resolution_clock::now();
  const auto total_duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

  if (result.has_value()) {
    SPDLOG_INFO("LoadEconomicIndicatorAsync: Successfully loaded {} ({} rows) in {}ms",
                indicator_name, result->num_rows(), total_duration.count());
  } else {
    SPDLOG_ERROR("LoadEconomicIndicatorAsync: Failed to load {}: {}", indicator_name, result.error());
  }

  co_return result;
}

// Market index methods (backward compat - delegates to LoadReferenceAggDataAsync)
std::expected<epoch_frame::DataFrame, std::string>
ApiCacheDataloader::LoadIndexData(const std::string& ticker,
                                  const epoch_frame::Date& fromDate,
                                  const epoch_frame::Date& toDate,
                                  bool is_eod) const {
  return drogon::sync_wait(LoadIndexDataAsync(ticker, fromDate, toDate, is_eod));
}

drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
ApiCacheDataloader::LoadIndexDataAsync(const std::string& ticker,
                                       const epoch_frame::Date& fromDate,
                                       const epoch_frame::Date& toDate,
                                       bool is_eod) const {
  // Delegate to ReferenceAgg with Indices asset class
  ReferenceAggKwargs kwargs{ticker, epoch_core::AssetClass::Indices, is_eod};
  co_return co_await LoadReferenceAggDataAsync(kwargs, fromDate, toDate);
}

// Generic reference aggregate loading (Stocks, FX, Crypto, Indices)
drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
ApiCacheDataloader::LoadReferenceAggDataAsync(ReferenceAggKwargs kwargs) const {
  co_return co_await LoadReferenceAggDataAsync(std::move(kwargs), m_option.GetStartDate(), m_option.GetEndDate());
}

drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
ApiCacheDataloader::LoadReferenceAggDataAsync(ReferenceAggKwargs kwargs,
                                               const epoch_frame::Date& fromDate,
                                               const epoch_frame::Date& toDate) const {
  SPDLOG_DEBUG("LoadReferenceAggDataAsync: Starting for {} {} ({})",
               epoch_core::AssetClassWrapper::ToString(kwargs.asset_class),
               kwargs.ticker, kwargs.is_eod ? "daily" : "minute");

  const auto start_time = std::chrono::high_resolution_clock::now();

  // Use the unified fetcher with ReferenceAgg category
  auto& fetcher = m_fetcherProvider->Get(DataCategory::ReferenceAgg);

  // Use asset-less fetch for cross-sectional data
  auto result = co_await fetcher.FetchAsync(
      DataCategory::ReferenceAgg, fromDate, toDate, kwargs);

  const auto end_time = std::chrono::high_resolution_clock::now();
  const auto total_duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

  if (result.has_value()) {
    SPDLOG_INFO("LoadReferenceAggDataAsync: Successfully loaded {} {} ({} rows) in {}ms",
                epoch_core::AssetClassWrapper::ToString(kwargs.asset_class),
                kwargs.ticker, result->num_rows(), total_duration.count());
  } else {
    SPDLOG_ERROR("LoadReferenceAggDataAsync: Failed to load {} {}: {}",
                 epoch_core::AssetClassWrapper::ToString(kwargs.asset_class),
                 kwargs.ticker, result.error());
  }

  co_return result;
}

} // namespace data_sdk::dataloader
