// Note: These tests were originally written for DefaultCacheProvider which had a buggy implementation.
// DayBucketCacheProvider uses a completely different architecture with CacheOrchestrator and CacheManifest,
// which are tested separately in cache_orchestrator_test.cpp and cache_manifest_test.cpp
// This file tests the DayBucketCacheProvider interface methods.

#include <catch2/catch_test_macros.hpp>
#include "dataloader/cache/day_bucket_cache_provider.h"
#include <epoch_data_sdk/common/time_provider.hpp>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/serialization.h>
#include <filesystem>
#include <epoch_data_sdk/model/builder/asset_builder.hpp>
#include <epoch_data_sdk/common/constants.hpp>
#include <epoch_data_sdk/model/asset/asset_constants.hpp>

using namespace data_sdk::dataloader::cache;
using namespace data_sdk::dataloader;
using namespace data_sdk;
using namespace epoch_frame;

class DayBucketCacheProviderTestFixture {
public:
  DayBucketCacheProviderTestFixture() {
    tempDir = std::filesystem::temp_directory_path() / "day_bucket_cache_provider_test";
    std::filesystem::create_directories(tempDir);
    provider = std::make_unique<DayBucketCacheProvider>();
  }

  ~DayBucketCacheProviderTestFixture() {
    std::filesystem::remove_all(tempDir);
  }

  DataFrame createTestData(const std::string& startDate = "2024-01-01", size_t numDays = 3) {
    std::vector<DateTime> dates;
    std::vector<double> closes;

    for (size_t i = 0; i < numDays; ++i) {
      auto date = DateTime::from_date_str(startDate, "UTC") + chrono_days(i);
      dates.push_back(date);
      closes.push_back(100.0 + i);
    }

    return epoch_frame::make_dataframe<double>(
        epoch_frame::factory::index::make_datetime_index(dates),
        {closes},
        {"close"}
    );
  }

  std::filesystem::path tempDir;
  std::unique_ptr<DayBucketCacheProvider> provider;
};

TEST_CASE("DayBucketCacheProvider: TryLoad", "[cache_provider]") {
  DayBucketCacheProviderTestFixture fixture;

  SECTION("TryLoad returns nullopt when no cache dir") {
    std::filesystem::path outPath;
    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;
    auto result = fixture.provider->TryLoad(
        std::nullopt, asset, DataCategory::DailyBars, 3600, outPath
    );

    REQUIRE_FALSE(result.has_value());
  }

  SECTION("TryLoad returns nullopt for non-existent daily cache") {
    std::filesystem::path outPath;
    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;
    auto result = fixture.provider->TryLoad(
        fixture.tempDir, asset, DataCategory::DailyBars, 3600, outPath
    );

    REQUIRE_FALSE(result.has_value());
  }

  SECTION("TryLoad successfully loads existing daily cache") {
    // First write some data
    auto testData = fixture.createTestData();
    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;
    std::filesystem::path cachePath;

    // Create a proper cache path structure that matches expected format
    auto assetId = asset.GetID();
    auto catDir = DataCategoryWrapper::ToString(DataCategory::DailyBars);
    auto assetClassDir = AssetClassWrapper::ToLongFormString(asset.GetAssetClass());
    auto expectedPath = fixture.tempDir / catDir / assetClassDir / (assetId + ".arrow");

    fixture.provider->Write(fixture.tempDir, expectedPath, true, testData);

    // Write test data to the expected location
    std::filesystem::create_directories(expectedPath.parent_path());
    epoch_frame::ArrowWriteOptions options;
    options.include_index = true;
    options.index_label = data_sdk::ColumnConstants::instance().TIMESTAMP();
    auto status = epoch_frame::write_arrow(testData, expectedPath, options);
    REQUIRE(status.ok());

    // Now try to load it
    std::filesystem::path outPath;
    auto result = fixture.provider->TryLoad(
        fixture.tempDir, asset, DataCategory::DailyBars, 3600, outPath
    );

    REQUIRE(result.has_value());
    REQUIRE(result->num_rows() == 3);

    // Verify the loaded data values using DataFrame indexing
    REQUIRE(result->iloc(0, "close").value<double>() == 100.0);
    REQUIRE(result->iloc(1, "close").value<double>() == 101.0);
    REQUIRE(result->iloc(2, "close").value<double>() == 102.0);
    REQUIRE(outPath == expectedPath);
  }

  SECTION("TryLoad respects TTL expiry") {
    // Create test data and write it
    auto testData = fixture.createTestData();
    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;
    auto catDir = DataCategoryWrapper::ToString(DataCategory::DailyBars);
    auto assetClassDir = AssetClassWrapper::ToLongFormString(asset.GetAssetClass());
    auto cachePath = fixture.tempDir / catDir / assetClassDir / (asset.GetID() + ".arrow");

    std::filesystem::create_directories(cachePath.parent_path());
    epoch_frame::ArrowWriteOptions options;
    options.include_index = true;
    options.index_label = data_sdk::ColumnConstants::instance().TIMESTAMP();
    auto status = epoch_frame::write_arrow(testData, cachePath, options);
    REQUIRE(status.ok());

    // Set TTL to 0 to make it immediately expired
    std::filesystem::path outPath;
    auto result = fixture.provider->TryLoad(
        fixture.tempDir, asset, DataCategory::DailyBars, 0, outPath
    );

    REQUIRE_FALSE(result.has_value());
  }

  SECTION("TryLoad warns for minute data without date range") {
    std::filesystem::path outPath;
    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;
    auto result = fixture.provider->TryLoad(
        fixture.tempDir, asset, DataCategory::MinuteBars, 3600, outPath
    );

    REQUIRE_FALSE(result.has_value());
  }
}

TEST_CASE("DayBucketCacheProvider: Write", "[cache_provider]") {
  DayBucketCacheProviderTestFixture fixture;

  SECTION("Write skips when cache disabled") {
    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;
    auto testData = fixture.createTestData();
    std::filesystem::path cachePath = fixture.tempDir / "test.arrow";

    fixture.provider->Write(fixture.tempDir, cachePath, false, testData);

    // Should not create any cache structure
    REQUIRE_FALSE(std::filesystem::exists(fixture.tempDir / "DailyBars"));
  }

  SECTION("Write skips when no cache dir") {
    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;
    auto testData = fixture.createTestData();
    std::filesystem::path cachePath = fixture.tempDir / "test.arrow";

    fixture.provider->Write(std::nullopt, cachePath, true, testData);

    // Should not create any cache structure
    REQUIRE_FALSE(std::filesystem::exists(fixture.tempDir / "DailyBars"));
  }

  SECTION("Write skips when data is empty") {
    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;
    DataFrame emptyData{};
    std::filesystem::path cachePath = fixture.tempDir / "test.arrow";

    fixture.provider->Write(fixture.tempDir, cachePath, true, emptyData);

    // Should not create any cache structure
    REQUIRE_FALSE(std::filesystem::exists(fixture.tempDir / "DailyBars"));
  }

  SECTION("Write creates cache structure for daily data") {
    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;
    auto testData = fixture.createTestData();
    auto cachePath = fixture.tempDir / "DailyBars" / "stocks" / "AAPL-Stocks.arrow";

    fixture.provider->Write(fixture.tempDir, cachePath, true, testData);

    // Verify cache structure was created
    auto catDir = DataCategoryWrapper::ToString(DataCategory::DailyBars);
    auto assetClassDir = AssetClassWrapper::ToLongFormString(asset.GetAssetClass());
    auto expectedPath = fixture.tempDir / catDir / assetClassDir / (asset.GetID() + ".arrow");

    REQUIRE(std::filesystem::exists(expectedPath));
  }

  SECTION("Write detects minute bars from path") {
    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;
    auto testData = fixture.createTestData();
    auto cachePath = fixture.tempDir / "MinuteBars" / "stocks" / "AAPL-Stocks.arrow";

    fixture.provider->Write(fixture.tempDir, cachePath, true, testData);

    // Should create day bucket structure for minute data
    auto catDir = DataCategoryWrapper::ToString(DataCategory::MinuteBars);
    auto assetClassDir = AssetClassWrapper::ToLongFormString(asset.GetAssetClass());
    auto assetDir = fixture.tempDir / catDir / assetClassDir / asset.GetID();

    REQUIRE(std::filesystem::exists(assetDir));
  }
}

TEST_CASE("DayBucketCacheProvider: AppendWrite", "[cache_provider]") {
  DayBucketCacheProviderTestFixture fixture;

  SECTION("AppendWrite returns original data when cache disabled") {
    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;
    auto testData = fixture.createTestData();
    std::filesystem::path outPath;

    auto result = fixture.provider->AppendWrite(
        fixture.tempDir, asset, DataCategory::DailyBars, 3600, false, testData, &outPath
    );

    REQUIRE(result.num_rows() == testData.num_rows());
    // outPath should not be set for minute bars or when cache disabled
  }

  SECTION("AppendWrite returns original data when no cache dir") {
    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;
    auto testData = fixture.createTestData();
    std::filesystem::path outPath;

    auto result = fixture.provider->AppendWrite(
        std::nullopt, asset, DataCategory::DailyBars, 3600, true, testData, &outPath
    );

    REQUIRE(result.num_rows() == testData.num_rows());
  }

  SECTION("AppendWrite returns original data when data is empty") {
    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;
    DataFrame emptyData{};
    std::filesystem::path outPath;

    auto result = fixture.provider->AppendWrite(
        fixture.tempDir, asset, DataCategory::DailyBars, 3600, true, emptyData, &outPath
    );

    REQUIRE(result.num_rows() == 0);
  }

  SECTION("AppendWrite writes cache and sets outPath for daily data") {
    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;
    auto testData = fixture.createTestData();
    std::filesystem::path outPath;

    auto result = fixture.provider->AppendWrite(
        fixture.tempDir, asset, DataCategory::DailyBars, 3600, true, testData, &outPath
    );

    REQUIRE(result.num_rows() == testData.num_rows());

    // Check that outPath was set correctly
    auto catDir = DataCategoryWrapper::ToString(DataCategory::DailyBars);
    auto assetClassDir = AssetClassWrapper::ToLongFormString(asset.GetAssetClass());
    auto expectedPath = fixture.tempDir / catDir / assetClassDir / (asset.GetID() + ".arrow");
    REQUIRE(outPath == expectedPath);

    // Verify the cache was actually written
    REQUIRE(std::filesystem::exists(expectedPath));
  }

  SECTION("AppendWrite does not set outPath for minute data") {
    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;
    auto testData = fixture.createTestData();
    std::filesystem::path outPath;

    auto result = fixture.provider->AppendWrite(
        fixture.tempDir, asset, DataCategory::MinuteBars, 3600, true, testData, &outPath
    );

    REQUIRE(result.num_rows() == testData.num_rows());
    // outPath should not be set for minute bars
    REQUIRE(outPath.empty());
  }
}

TEST_CASE("DayBucketCacheProvider: LoadWithCache", "[cache_provider]") {
  DayBucketCacheProviderTestFixture fixture;

  SECTION("LoadWithCache calls fetch function when no cache dir") {
    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;
    bool fetchCalled = false;
    auto fetchFn = [&fetchCalled, &fixture](const epoch_frame::Date& from, const epoch_frame::Date& to)
        -> std::expected<epoch_frame::DataFrame, std::string> {
      fetchCalled = true;
      return fixture.createTestData("2024-01-01", 3);
    };

    auto result = fixture.provider->LoadWithCache(
        std::nullopt, asset, DataCategory::DailyBars, 3600, true,
        DateTime::from_date_str("2024-01-01").date(),
        DateTime::from_date_str("2024-01-03").date(),
        fetchFn
    );

    REQUIRE(result.has_value());
    REQUIRE(fetchCalled);
    REQUIRE(result->num_rows() == 3);
  }

  SECTION("LoadWithCache uses orchestrator when cache dir provided") {
    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;
    bool fetchCalled = false;
    auto fetchFn = [&fetchCalled, &fixture](const epoch_frame::Date& from, const epoch_frame::Date& to)
        -> std::expected<epoch_frame::DataFrame, std::string> {
      fetchCalled = true;
      return fixture.createTestData("2024-01-01", 3);
    };

    auto result = fixture.provider->LoadWithCache(
        fixture.tempDir, asset, DataCategory::DailyBars, 3600, true,
        DateTime::from_date_str("2024-01-01").date(),
        DateTime::from_date_str("2024-01-03").date(),
        fetchFn
    );

    REQUIRE(result.has_value());
    REQUIRE(fetchCalled);  // Should call fetch for empty cache
    REQUIRE(result->num_rows() == 3);
  }

  SECTION("LoadWithCache handles fetch function errors") {
    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;
    auto fetchFn = [](const epoch_frame::Date& from, const epoch_frame::Date& to)
        -> std::expected<epoch_frame::DataFrame, std::string> {
      return std::unexpected("Network error");
    };

    auto result = fixture.provider->LoadWithCache(
        fixture.tempDir, asset, DataCategory::DailyBars, 3600, true,
        DateTime::from_date_str("2024-01-01").date(),
        DateTime::from_date_str("2024-01-03").date(),
        fetchFn
    );

    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error() == "Network error");
  }

  SECTION("LoadWithCache enables intraday freshness for minute data") {
    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;
    // Set environment variable for intraday freshness
    setenv("INTRADAY_ALWAYS_FRESH", "true", 1);

    bool fetchCalled = false;
    auto fetchFn = [&fetchCalled, &fixture](const epoch_frame::Date& from, const epoch_frame::Date& to)
        -> std::expected<epoch_frame::DataFrame, std::string> {
      fetchCalled = true;
      return fixture.createTestData("2024-01-01", 3);
    };

    auto result = fixture.provider->LoadWithCache(
        fixture.tempDir, asset, DataCategory::MinuteBars, 3600, true,
        DateTime::from_date_str("2024-01-01").date(),
        DateTime::from_date_str("2024-01-03").date(),
        fetchFn
    );

    REQUIRE(result.has_value());
    REQUIRE(fetchCalled);
    REQUIRE(result->num_rows() == 3);

    // Clean up environment
    unsetenv("INTRADAY_ALWAYS_FRESH");
  }
}

TEST_CASE("DayBucketCacheProvider: initializeForCacheDir", "[cache_provider]") {
  DayBucketCacheProviderTestFixture fixture;

  SECTION("initializeForCacheDir creates manifest and orchestrator") {
    // This is tested indirectly through other methods, but we can verify
    // that multiple calls to the same cache dir don't recreate objects

    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;
    // Call LoadWithCache twice with the same cache dir
    auto fetchFn = [&fixture](const epoch_frame::Date& from, const epoch_frame::Date& to)
        -> std::expected<epoch_frame::DataFrame, std::string> {
      return fixture.createTestData("2024-01-01", 1);
    };

    auto result1 = fixture.provider->LoadWithCache(
        fixture.tempDir, asset, DataCategory::DailyBars, 3600, true,
        DateTime::from_date_str("2024-01-01").date(),
        DateTime::from_date_str("2024-01-01").date(),
        fetchFn
    );

    auto result2 = fixture.provider->LoadWithCache(
        fixture.tempDir, asset, DataCategory::DailyBars, 3600, true,
        DateTime::from_date_str("2024-01-02").date(),
        DateTime::from_date_str("2024-01-02").date(),
        fetchFn
    );

    REQUIRE(result1.has_value());
    REQUIRE(result2.has_value());

    // Verify manifest file was created
    auto manifestPath = fixture.tempDir / "manifest.json";
    REQUIRE(std::filesystem::exists(manifestPath));
  }
}

TEST_CASE("DayBucketCacheProvider is the default", "[cache_provider]") {
  // This test just confirms that DayBucketCacheProvider compiles and can be instantiated
  auto provider = std::make_unique<DayBucketCacheProvider>();
  REQUIRE(provider != nullptr);
}