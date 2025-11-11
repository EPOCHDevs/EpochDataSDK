#include <catch2/catch_test_macros.hpp>
#include "dataloader/cache/cache_manifest.h"
#include <epoch_data_sdk/dataloader/cache/types.hpp>
#include <epoch_data_sdk/dataloader/cache/types.hpp>
#include <epoch_data_sdk/common/time_provider.hpp>
#include <filesystem>
#include <fstream>
#include <thread>
#include <epoch_data_sdk/model/builder/asset_builder.hpp>
#include <epoch_data_sdk/common/constants.hpp>
#include <epoch_data_sdk/model/asset/asset_constants.hpp>

using namespace data_sdk::dataloader;
using namespace data_sdk;
using namespace epoch_frame;
using data_sdk::dataloader::cache::CacheLoadParams;
using data_sdk::dataloader::cache::CacheManifest;
using data_sdk::dataloader::cache::CacheProbeResult;
using data_sdk::dataloader::cache::CacheManifestEntry;

class CacheManifestTestFixture {
public:
  CacheManifestTestFixture() {
    tempDir = std::filesystem::temp_directory_path() / "cache_manifest_test";
    std::filesystem::create_directories(tempDir);
    manifestPath = tempDir / "manifest.json";

    // Create fixed time provider for deterministic testing
    timeProvider = std::make_shared<FixedTimeProvider>();

    manifest = std::make_unique<CacheManifest>(manifestPath, timeProvider);
  }

  ~CacheManifestTestFixture() {
    std::filesystem::remove_all(tempDir);
  }

  CacheLoadParams createLoadParams(
      const std::string& symbol = "AAPL",
      const std::string& startDate = "2024-01-01",
      const std::string& endDate = "2024-01-10") {
    CacheLoadParams params{
      asset::MakeAsset({symbol + "-Stocks"}),  // Use asset ID format
      DataCategory::MinuteBars,
      DateTime::from_date_str(startDate).date(),
      DateTime::from_date_str(endDate).date(),
      tempDir,
      3600,
      true,
      false
    };
    return params;
  }

  std::filesystem::path tempDir;
  std::filesystem::path manifestPath;
  std::unique_ptr<CacheManifest> manifest;
  std::shared_ptr<FixedTimeProvider> timeProvider;
};

TEST_CASE("CacheManifest: Basic operations", "[cache_manifest]") {
  CacheManifestTestFixture fixture;

  SECTION("Empty manifest returns empty probe result") {
    auto params = fixture.createLoadParams();
    auto result = fixture.manifest->probe(params);

    REQUIRE_FALSE(result.hasData);
    REQUIRE_FALSE(result.isComplete);
    REQUIRE_FALSE(result.isExpired);
    REQUIRE_FALSE(result.manifest.has_value());
  }

  SECTION("Update and probe manifest entry") {
    auto asset = asset::MakeAsset({"AAPL-Stocks"});
    auto startDate = DateTime::from_date_str("2024-01-01").date();
    auto endDate = DateTime::from_date_str("2024-01-10").date();

    // Update manifest with cached data
    fixture.manifest->update(asset, DataCategory::MinuteBars,
                            startDate, endDate, 1000);

    // Probe for data within cached range
    auto params = fixture.createLoadParams("AAPL", "2024-01-05", "2024-01-08");
    auto result = fixture.manifest->probe(params);

    REQUIRE(result.hasData);
    REQUIRE(result.isComplete);
    REQUIRE_FALSE(result.isExpired);
    REQUIRE(result.manifest.has_value());
    REQUIRE(result.manifest->numRows == 1000);
  }

  SECTION("Probe for partial coverage") {
    auto asset = asset::MakeAsset({"AAPL-Stocks"});
    auto startDate = DateTime::from_date_str("2024-01-01").date();
    auto endDate = DateTime::from_date_str("2024-01-10").date();

    fixture.manifest->update(asset, DataCategory::MinuteBars,
                            startDate, endDate, 1000);

    // Request data beyond cached range
    auto params = fixture.createLoadParams("AAPL", "2024-01-05", "2024-01-15");
    auto result = fixture.manifest->probe(params);

    REQUIRE(result.hasData);
    REQUIRE_FALSE(result.isComplete);  // Partial coverage
  }

  SECTION("Different assets tracked separately") {
    auto aapl = asset::MakeAsset({"AAPL-Stocks"});
    auto googl = asset::MakeAsset({"GOOGL-Stocks"});
    auto date = DateTime::from_date_str("2024-01-01").date();

    fixture.manifest->update(aapl, DataCategory::MinuteBars, date, date, 100);
    fixture.manifest->update(googl, DataCategory::MinuteBars, date, date, 200);

    // Probe for AAPL
    auto paramsAAPL = fixture.createLoadParams("AAPL", "2024-01-01", "2024-01-01");
    auto resultAAPL = fixture.manifest->probe(paramsAAPL);
    REQUIRE(resultAAPL.hasData);
    REQUIRE(resultAAPL.manifest->numRows == 100);

    // Probe for GOOGL
    auto paramsGOOGL = fixture.createLoadParams("GOOGL", "2024-01-01", "2024-01-01");
    auto resultGOOGL = fixture.manifest->probe(paramsGOOGL);
    REQUIRE(resultGOOGL.hasData);
    REQUIRE(resultGOOGL.manifest->numRows == 200);
  }
}

TEST_CASE("CacheManifest: Day-bucketed data", "[cache_manifest]") {
  CacheManifestTestFixture fixture;

  SECTION("Track continuous date ranges") {
    auto asset = asset::MakeAsset({"AAPL-Stocks"});
    auto startDate = DateTime::from_date_str("2024-01-01").date();
    auto endDate = DateTime::from_date_str("2024-01-06").date();

    // With day-bucketed cache, we only cache continuous ranges (no gaps)
    fixture.manifest->update(asset, DataCategory::MinuteBars,
                            startDate, endDate, 500);

    // Probe for the cached range
    auto params = fixture.createLoadParams("AAPL", "2024-01-01", "2024-01-06");
    auto result = fixture.manifest->probe(params);

    REQUIRE(result.hasData);
    REQUIRE(result.isComplete);  // Full coverage since no gaps
  }

  SECTION("Merge cached days from multiple updates") {
    auto asset = asset::MakeAsset({"AAPL-Stocks"});

    // First update: days 1-3
    std::vector<Date> batch1 = {
      DateTime::from_date_str("2024-01-01").date(),
      DateTime::from_date_str("2024-01-02").date(),
      DateTime::from_date_str("2024-01-03").date()
    };
    fixture.manifest->update(asset, DataCategory::MinuteBars,
                            batch1.front(), batch1.back(), 300);

    // Second update: days 4-6
    std::vector<Date> batch2 = {
      DateTime::from_date_str("2024-01-04").date(),
      DateTime::from_date_str("2024-01-05").date(),
      DateTime::from_date_str("2024-01-06").date()
    };
    fixture.manifest->update(asset, DataCategory::MinuteBars,
                            batch2.front(), batch2.back(), 300);

    // Check merged result
    auto params = fixture.createLoadParams("AAPL", "2024-01-01", "2024-01-06");
    auto result = fixture.manifest->probe(params);

    REQUIRE(result.hasData);
    REQUIRE(result.manifest->numRows == 600);  // 300 + 300
  }
}

TEST_CASE("CacheManifest: Persistence", "[cache_manifest]") {
  CacheManifestTestFixture fixture;

  SECTION("Save and load manifest") {
    auto asset = asset::MakeAsset({"TSLA-Stocks"});
    auto startDate = DateTime::from_date_str("2024-02-01").date();
    auto endDate = DateTime::from_date_str("2024-02-10").date();

    // Update and save
    fixture.manifest->update(asset, DataCategory::DailyBars,
                            startDate, endDate, 750);

    // Create new manifest instance and load
    auto newManifest = std::make_unique<CacheManifest>(fixture.manifestPath);

    // Verify loaded data
    CacheLoadParams params{
      asset,
      DataCategory::DailyBars,
      startDate,
      endDate,
      fixture.tempDir,
      0,
      true,
      false
    };

    auto result = newManifest->probe(params);

    REQUIRE(result.hasData);
    REQUIRE(result.isComplete);
    REQUIRE(result.manifest->numRows == 750);
  }

  SECTION("Manifest file format") {
    auto asset = asset::MakeAsset({"SPY-Stocks"});
    auto date = DateTime::from_date_str("2024-03-15").date();

    fixture.manifest->update(asset, DataCategory::MinuteBars,
                            date, date, 390);

    // Read the JSON file directly
    std::ifstream file(fixture.manifestPath);
    REQUIRE(file.is_open());

    std::string content((std::istreambuf_iterator<char>(file)),
                       std::istreambuf_iterator<char>());

    // Check that key format is correct
    REQUIRE(content.find("\"SPY-Stocks:MinuteBars\"") != std::string::npos);
    REQUIRE(content.find("\"startDate\":\"2024-03-15\"") != std::string::npos);
    REQUIRE(content.find("\"numRows\":390") != std::string::npos);
  }
}

TEST_CASE("CacheManifest: Expiry handling", "[cache_manifest]") {
  CacheManifestTestFixture fixture;

  SECTION("Check expiry based on TTL") {
    auto asset = asset::MakeAsset({"AAPL-Stocks"});
    auto date = DateTime::from_date_str("2024-01-01").date();

    fixture.manifest->update(asset, DataCategory::MinuteBars,
                            date, date, 100);

    // Check with no expiry (TTL = 0)
    auto params1 = fixture.createLoadParams();
    params1.ttlSeconds = 0;
    auto result1 = fixture.manifest->probe(params1);
    REQUIRE_FALSE(result1.isExpired);

    // Check with very short TTL (should be expired)
    auto params2 = fixture.createLoadParams();
    params2.ttlSeconds = 1;  // 1 second TTL

    // Advance time by 2 seconds to ensure expiry
    fixture.timeProvider->advance_by(std::chrono::seconds(2));

    auto result2 = fixture.manifest->probe(params2);
    REQUIRE(result2.isExpired);  // Should be expired after 2 seconds
  }

  SECTION("Clean expired entries") {
    auto asset1 = asset::MakeAsset({"MSFT-Stocks"});
    auto asset2 = asset::MakeAsset({"GOOGL-Stocks"});
    auto date = DateTime::from_date_str("2024-01-01").date();

    // Add first entry
    fixture.manifest->update(asset1, DataCategory::MinuteBars,
                            date, date, 100);

    // Advance time by 1 hour
    fixture.timeProvider->advance_by(std::chrono::hours(1));

    // Add second entry (will have later timestamp)
    fixture.manifest->update(asset2, DataCategory::MinuteBars,
                            date, date, 200);

    // Clean with TTL of 30 minutes - should remove first entry only
    fixture.manifest->cleanExpired(1800);  // 30 minutes

    CacheLoadParams params{
      asset2,
      DataCategory::MinuteBars,
      date,
      date,
      fixture.tempDir,
      0,
      true,
      false
    };
    auto result = fixture.manifest->probe(params);
    REQUIRE(result.hasData);
  }
}

TEST_CASE("CacheManifest: Immutable day buckets behavior", "[cache_manifest]") {
  CacheManifestTestFixture fixture;

  SECTION("Append-only pattern for minute data") {
    auto asset = asset::MakeAsset({"AAPL-Stocks"});

    // Initial cache: Jan 1-10
    auto startDate = DateTime::from_date_str("2024-01-01").date();
    auto endDate = DateTime::from_date_str("2024-01-10").date();
    fixture.manifest->update(asset, DataCategory::MinuteBars,
                            startDate, endDate, 1000);

    // Request extending beyond cache
    auto params = fixture.createLoadParams("AAPL", "2024-01-01", "2024-01-15");
    auto result = fixture.manifest->probe(params);

    REQUIRE(result.hasData);
    REQUIRE_FALSE(result.isComplete);
  }

  SECTION("No gaps in middle for day-bucketed data") {
    auto asset = asset::MakeAsset({"AAPL-Stocks"});

    // Simulate complete coverage from day 1 to 10
    std::vector<Date> allDays;
    auto date = DateTime::from_date_str("2024-01-01").date();
    for (int i = 0; i < 10; ++i) {
      allDays.push_back(date + chrono_days(i));
    }

    fixture.manifest->update(asset, DataCategory::MinuteBars,
                            allDays.front(), allDays.back(),
                            1000);

    // Request subset should show complete coverage
    auto params = fixture.createLoadParams("AAPL", "2024-01-03", "2024-01-07");
    auto result = fixture.manifest->probe(params);

    REQUIRE(result.hasData);
    REQUIRE(result.isComplete);
  }
}