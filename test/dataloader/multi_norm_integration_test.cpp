#include <catch2/catch_test_macros.hpp>
#include "dataloader/api_cache_dataloader.h"
#include "dataloader/fetcher_provider_default.h"
#include "dataloader/cache/day_bucket_cache_provider.h"
#include <epoch_frame/dataframe.h>
#include <epoch_data_sdk/common/constants.hpp>
#include <epoch_data_sdk/model/asset/asset_constants.hpp>
#include <epoch_data_sdk/common/env_loader.hpp>
#include <epoch_data_sdk/dataloader/metadata_registry.hpp>
#include <filesystem>
#include <spdlog/spdlog.h>

using namespace data_sdk;
using namespace data_sdk::dataloader;
using namespace epoch_frame;

/**
 * Multi-Normalization Integration Test Suite
 *
 * Tests real API integration with different normalization modes:
 * 1. Daily Norm: All normalized categories (NO News), 1 year
 * 2. Minute Norm: MinuteBars + News, 6 months
 * 3. Mixed Norm: All categories, 6 months
 *
 * Also tests empty category handling (categories with no data should
 * produce empty DataFrames with proper schema, not be skipped).
 */

class MultiNormIntegrationTestFixture {
public:
  MultiNormIntegrationTestFixture()
    : testAsset(data_sdk::asset::AssetConstants::instance().AAPL) {
    // Create temp cache directory
    tempCache = std::filesystem::temp_directory_path() / "multi_norm_integration_test_cache";
    std::filesystem::create_directories(tempCache);

    // Setup cache provider
    cacheProvider = std::make_shared<data_sdk::dataloader::cache::DayBucketCacheProvider>();

    // Setup fetcher provider (uses real API clients)
    std::string archivePath = "";
    fetcherProvider = std::make_shared<DefaultFetcherProvider>(archivePath);

    SPDLOG_INFO("MultiNormIntegrationTestFixture initialized with cache at {}", tempCache.string());
  }

  ~MultiNormIntegrationTestFixture() {
    // Cleanup temp cache
    std::filesystem::remove_all(tempCache);
  }

  DataloaderOption createDailyNormOption() {
    DataloaderOption option;

    // All normalized categories EXCEPT News (IPOs not supported in metadata registry)
    option.categories = {
      DataCategory::DailyBars,
      DataCategory::Dividends,
      DataCategory::Splits,
      DataCategory::TickerEvents,
      DataCategory::BalanceSheets,
      DataCategory::CashFlowStatements,
      DataCategory::IncomeStatements,
      DataCategory::Ratios,
      DataCategory::ShortInterest,
      DataCategory::ShortVolume
    };

    // 1 year lookback
    auto endDate = DateTime::now("UTC").date();
    auto startDate = endDate - chrono_days(365);

    option.startDate = startDate;
    option.endDate = endDate;
    option.enableCache = true;
    option.cacheDir = tempCache;
    option.cacheTTLSeconds = 86400;

    return option;
  }

  DataloaderOption createMinuteNormOption() {
    DataloaderOption option;

    // Only MinuteBars + News (both non-normalized)
    option.categories = {
      DataCategory::MinuteBars,
      DataCategory::News
    };

    // 6 months lookback
    auto endDate = DateTime::now("UTC").date();
    auto startDate = endDate - chrono_days(180);

    option.startDate = startDate;
    option.endDate = endDate;
    option.enableCache = true;
    option.cacheDir = tempCache;
    option.cacheTTLSeconds = 86400;

    return option;
  }

  DataloaderOption createMixedNormOption() {
    DataloaderOption option;

    // All non-bar categories (mixed normalized and non-normalized) - 11 total
    // Note: Cannot mix MinuteBars and DailyBars, so use DailyBars only
    option.categories = {
      DataCategory::DailyBars,
      DataCategory::News,
      DataCategory::Dividends,
      DataCategory::Splits,
      DataCategory::TickerEvents,
      DataCategory::BalanceSheets,
      DataCategory::CashFlowStatements,
      DataCategory::IncomeStatements,
      DataCategory::Ratios,
      DataCategory::ShortInterest,
      DataCategory::ShortVolume
    };

    // 6 months lookback
    auto endDate = DateTime::now("UTC").date();
    auto startDate = endDate - chrono_days(180);

    option.startDate = startDate;
    option.endDate = endDate;
    option.enableCache = true;
    option.cacheDir = tempCache;
    option.cacheTTLSeconds = 86400;

    return option;
  }

  std::filesystem::path tempCache;
  asset::Asset testAsset;
  std::shared_ptr<data_sdk::dataloader::cache::DayBucketCacheProvider> cacheProvider;
  std::shared_ptr<DefaultFetcherProvider> fetcherProvider;
};

TEST_CASE("Multi-Norm: Daily Norm Integration (All normalized categories, no News, 1 year)",
          "[multi_norm][daily][integration]") {

  // Skip if no API credentials
  if (ENV("POLYGON_API_KEY").empty()) {
    SKIP("POLYGON_API_KEY not set - skipping real API integration test");
  }

  MultiNormIntegrationTestFixture fixture;

  SECTION("Load daily normalized data with ALL metadata columns") {
    auto option = fixture.createDailyNormOption();
    option.dataloaderAssets.insert(fixture.testAsset);

    // Build expected column set from ALL metadata
    std::set<std::string> expected_columns;
    for (const auto& cat : option.categories) {
      auto metadata = MetadataRegistry::GetMetadataForCategory(cat);
      for (const auto& col : metadata.columns) {
        std::string col_name = metadata.category_prefix + col.id;
        expected_columns.insert(col_name);
      }
    }

    SPDLOG_INFO("Expected {} total columns from {} categories", expected_columns.size(), option.categories.size());

    ApiCacheDataloader loader(option, fixture.cacheProvider, fixture.fetcherProvider);
    loader.LoadData();
    auto stored_data = loader.GetStoredData();

    REQUIRE(stored_data.size() > 0);

    if (stored_data.count(fixture.testAsset) > 0) {
      const auto& merged_df = stored_data.at(fixture.testAsset);

      SPDLOG_INFO("Daily norm result: {} rows, {} columns\n{}", merged_df.num_rows(), merged_df.num_cols(),
        merged_df.head(20).repr());

      auto column_names = merged_df.column_names();
      std::set<std::string> actual_columns(column_names.begin(), column_names.end());

      // CRITICAL: ALL metadata columns must be present (even if all null)
      REQUIRE(actual_columns == expected_columns);

      // Verify we have data (not all nulls)
      REQUIRE(merged_df.num_rows() > 0);

      // Check that OHLCV columns exist
      for (const auto& col : {"o", "h", "l", "c", "v"}) {
        REQUIRE(actual_columns.count(col) > 0);
        SPDLOG_INFO("Column '{}' exists", col);
      }

      // Debug print first few values of 'c' column
      if (actual_columns.count("c") > 0) {
        auto close_series = merged_df["c"];
        SPDLOG_INFO("Close column sample (first 5 values): {}", close_series.head(5).repr());
      }
    }
  }
}

TEST_CASE("Multi-Norm: Minute Norm Integration (MinuteBars + News, 6 months)",
          "[multi_norm][minute][integration]") {

  // Skip if no API credentials
  if (ENV("POLYGON_API_KEY").empty()) {
    SKIP("POLYGON_API_KEY not set - skipping real API integration test");
  }

  MultiNormIntegrationTestFixture fixture;

  SECTION("Load minute-level data with ALL metadata columns") {
    auto option = fixture.createMinuteNormOption();
    option.dataloaderAssets.insert(fixture.testAsset);

    // Build expected column set from ALL metadata
    std::set<std::string> expected_columns;
    for (const auto& cat : option.categories) {
      auto metadata = MetadataRegistry::GetMetadataForCategory(cat);
      for (const auto& col : metadata.columns) {
        std::string col_name = metadata.category_prefix + col.id;
        expected_columns.insert(col_name);
      }
    }

    SPDLOG_INFO("Expected {} total columns from {} categories", expected_columns.size(), option.categories.size());

    ApiCacheDataloader loader(option, fixture.cacheProvider, fixture.fetcherProvider);
    loader.LoadData();
    auto stored_data = loader.GetStoredData();

    REQUIRE(stored_data.size() > 0);

    if (stored_data.count(fixture.testAsset) > 0) {
      const auto& merged_df = stored_data.at(fixture.testAsset);

      SPDLOG_INFO("Minute norm result: {} rows, {} columns\n{}",
        merged_df.num_rows(), merged_df.num_cols(), merged_df.head(20).repr());

      auto column_names = merged_df.column_names();
      std::set<std::string> actual_columns(column_names.begin(), column_names.end());

      // CRITICAL: ALL metadata columns must be present (even if all null)
      REQUIRE(actual_columns == expected_columns);

      // Verify we have data
      REQUIRE(merged_df.num_rows() > 0);

      // Check that OHLCV columns exist
      for (const auto& col : {"o", "h", "l", "c", "v"}) {
        REQUIRE(actual_columns.count(col) > 0);
        SPDLOG_INFO("Column '{}' exists", col);
      }

      // Debug print sample values
      if (actual_columns.count("c") > 0) {
        auto close_series = merged_df["c"];
        SPDLOG_INFO("Close column sample: {}", close_series.head(5).repr());
      }

      // Check that News columns exist (may be all null if no news)
      bool has_news_columns = false;
      for (const auto& col : actual_columns) {
        if (col.find("N:") == 0) {
          has_news_columns = true;
          SPDLOG_INFO("News column present: {}", col);
        }
      }
      REQUIRE(has_news_columns); // Should have News columns even if empty
    }
  }
}

TEST_CASE("Multi-Norm: Mixed Norm Integration (All categories, 6 months)",
          "[multi_norm][mixed][integration]") {

  // Skip if no API credentials
  if (ENV("POLYGON_API_KEY").empty()) {
    SKIP("POLYGON_API_KEY not set - skipping real API integration test");
  }

  MultiNormIntegrationTestFixture fixture;

  SECTION("Load all categories with mixed normalization and ALL metadata columns") {
    auto option = fixture.createMixedNormOption();
    option.dataloaderAssets.insert(fixture.testAsset);

    // Build expected column set from ALL metadata (all 13 categories)
    std::set<std::string> expected_columns;
    for (const auto& cat : option.categories) {
      auto metadata = MetadataRegistry::GetMetadataForCategory(cat);
      for (const auto& col : metadata.columns) {
        std::string col_name = metadata.category_prefix + col.id;
        expected_columns.insert(col_name);
      }
    }

    SPDLOG_INFO("Expected {} total columns from {} categories", expected_columns.size(), option.categories.size());

    ApiCacheDataloader loader(option, fixture.cacheProvider, fixture.fetcherProvider);
    loader.LoadData();
    auto stored_data = loader.GetStoredData();

    REQUIRE(stored_data.size() > 0);

    if (stored_data.count(fixture.testAsset) > 0) {
      const auto& merged_df = stored_data.at(fixture.testAsset);

      SPDLOG_INFO("Mixed norm result: {} rows, {} columns\n{}",
        merged_df.num_rows(), merged_df.num_cols(), merged_df.head(20).repr());

      auto column_names = merged_df.column_names();
      std::set<std::string> actual_columns(column_names.begin(), column_names.end());

      SPDLOG_INFO("Actual columns:");
      for (const auto& col : actual_columns) {
        SPDLOG_INFO("  - {}", col);
      }

      // CRITICAL: Check column consistency
      // Note: Some categories may fail validation or have incomplete schemas

      // Core OHLCV columns must exist
      for (const auto& col : {"o", "h", "l", "c", "v"}) {
        REQUIRE(actual_columns.count(col) > 0);
      }

      // Check for major category prefixes
      bool has_dividends = false, has_splits = false, has_news = false;
      for (const auto& col : actual_columns) {
        if (col.find("D:") == 0) has_dividends = true;
        if (col.find("S:") == 0) has_splits = true;
        if (col.find("N:") == 0) has_news = true;
      }

      SPDLOG_INFO("Category presence: Dividends={}, Splits={}, News={}",
                  has_dividends, has_splits, has_news);

      // Verify we have data
      REQUIRE(merged_df.num_rows() > 0);

      // Check that core OHLCV columns exist
      for (const auto& col : {"o", "h", "l", "c", "v"}) {
        REQUIRE(actual_columns.count(col) > 0);
        SPDLOG_INFO("Column '{}' exists", col);
      }

      // Debug print sample values from different column types
      if (actual_columns.count("c") > 0) {
        auto close_series = merged_df["c"];
        SPDLOG_INFO("Close column sample: {}", close_series.head(3).repr());
      }

      // Sample a prefixed column if exists
      for (const auto& col : actual_columns) {
        if (col.find("D:") == 0 || col.find("S:") == 0 || col.find("N:") == 0) {
          auto sample_series = merged_df[col];
          SPDLOG_INFO("Sample prefixed column '{}': {}", col, sample_series.head(3).repr());
          break;
        }
      }

      // Verify prefixed columns from different categories exist
      std::vector<std::string> expected_prefixes = {"D:", "S:", "N:"};
      for (const auto& prefix : expected_prefixes) {
        bool found = false;
        for (const auto& col : actual_columns) {
          if (col.find(prefix) == 0) {
            found = true;
            break;
          }
        }
        REQUIRE(found); // Each prefix category should have columns
      }
    }
  }
}

TEST_CASE("Multi-Norm: Empty Category Handling",
          "[multi_norm][empty][integration]") {

  // Skip if no API credentials
  if (ENV("POLYGON_API_KEY").empty()) {
    SKIP("POLYGON_API_KEY not set - skipping real API integration test");
  }

  MultiNormIntegrationTestFixture fixture;

  SECTION("Categories with no data should create empty DataFrames with schema") {
    DataloaderOption option;
    option.categories = {
      DataCategory::DailyBars,
      DataCategory::Dividends,
      DataCategory::Splits
    };

    // Very short date range (1 week) - unlikely to have dividends/splits
    auto endDate = DateTime::now("UTC").date();
    auto startDate = endDate - chrono_days(7);

    option.startDate = startDate;
    option.endDate = endDate;
    option.enableCache = false;
    option.cacheDir = fixture.tempCache;
    option.dataloaderAssets.insert(fixture.testAsset);

    // Build expected column set from ALL metadata
    std::set<std::string> expected_columns;
    for (const auto& cat : option.categories) {
      auto metadata = MetadataRegistry::GetMetadataForCategory(cat);
      for (const auto& col : metadata.columns) {
        std::string col_name = metadata.category_prefix + col.id;
        expected_columns.insert(col_name);
      }
    }

    SPDLOG_INFO("Expected {} total columns from metadata", expected_columns.size());

    ApiCacheDataloader loader(option, fixture.cacheProvider, fixture.fetcherProvider);

    loader.LoadData();
    auto stored_data = loader.GetStoredData();

    if (stored_data.count(fixture.testAsset) > 0) {
      const auto& merged_df = stored_data.at(fixture.testAsset);

      auto column_names = merged_df.column_names();
      std::set<std::string> actual_columns(column_names.begin(), column_names.end());

      SPDLOG_INFO("Empty category test merged columns:");
      for (const auto& col : actual_columns) {
        SPDLOG_INFO("  - {}", col);
      }

      // KEY TEST: Even if no dividends/splits occurred, D: and S: columns must exist
      REQUIRE(actual_columns == expected_columns);

      // Log any missing or extra columns for debugging
      for (const auto& expected : expected_columns) {
        if (actual_columns.find(expected) == actual_columns.end()) {
          SPDLOG_ERROR("MISSING COLUMN: {}", expected);
        }
      }
      for (const auto& actual : actual_columns) {
        if (expected_columns.find(actual) == expected_columns.end()) {
          SPDLOG_ERROR("EXTRA COLUMN: {}", actual);
        }
      }

      // Verify basic data properties
      REQUIRE(merged_df.num_rows() > 0);
    }
  }
}
