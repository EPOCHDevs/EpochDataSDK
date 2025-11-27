#include <catch2/catch_test_macros.hpp>
#include <catch2/trompeloeil.hpp>
#include "dataloader/cache/cache_orchestrator.h"
#include "dataloader/cache/cache_manifest.h"
#include <epoch_data_sdk/dataloader/cache/types.hpp>
#include <epoch_data_sdk/dataloader/fetch_kwargs.hpp>
#include <epoch_data_sdk/common/time_provider.hpp>
#include <epoch_frame/dataframe.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/serialization.h>
#include <filesystem>
#include <thread>
#include <epoch_data_sdk/model/builder/asset_builder.hpp>
#include <epoch_data_sdk/common/constants.hpp>
#include <epoch_data_sdk/model/asset/asset_constants.hpp>

using namespace data_sdk::dataloader;
using namespace data_sdk;
using namespace epoch_frame;
using data_sdk::dataloader::cache::FetchStrategy;
using data_sdk::dataloader::cache::CacheWriteParams;
using data_sdk::dataloader::cache::CacheProbeResult;
using data_sdk::dataloader::cache::CacheLoadParams;
using data_sdk::dataloader::cache::CacheManifest;
using data_sdk::dataloader::cache::CacheManifestEntry;
using data_sdk::dataloader::cache::CacheOrchestrator;
using data_sdk::dataloader::FetchKwargs;
using data_sdk::dataloader::NoKwargs;

using FetchResult = std::expected<epoch_frame::DataFrame, std::string>;

// Provide comparison operators for DataFrame to satisfy std::expected requirements
// These are only used for type checking, not actual comparison
namespace epoch_frame {
  inline bool operator==(const DataFrame&, const DataFrame&) {
    return true;  // Dummy implementation for compilation
  }
  inline bool operator==(const DataFrame&, std::nullptr_t) {
    return false;  // Dummy implementation for compilation
  }
  inline bool operator==(std::nullptr_t, const DataFrame&) {
    return false;  // Dummy implementation for compilation
  }
}


// Mock implementation of IDataFetcher for testing
class MockDataFetcher : public IDataFetcher {
public:
  MAKE_MOCK5(Fetch,
             FetchResult(const asset::Asset &, DataCategory, const epoch_frame::Date &,
                         const epoch_frame::Date &, const FetchKwargs &),
             const);

  // Async version - wraps sync Fetch in coroutine
  drogon::Task<FetchResult> FetchAsync(const asset::Asset &asset, DataCategory category,
                                       const epoch_frame::Date &fromDate,
                                       const epoch_frame::Date &toDate,
                                       const FetchKwargs &kwargs = NoKwargs{}) const override {
    co_return Fetch(asset, category, fromDate, toDate, kwargs);
  }

  MockDataFetcher() : shouldFail(false),
                      failureMessage("Mock fetch failed"),
                      fetchCallCount(0) {}

  bool shouldFail;
  std::string failureMessage;
  mutable int fetchCallCount;
  mutable epoch_frame::Date lastFromDate;
  mutable epoch_frame::Date lastToDate;

  // Helper method to create test data
  static FetchResult createTestData(const epoch_frame::Date &fromDate, const epoch_frame::Date &toDate) {
    std::vector<DateTime> dates;
    std::vector<double> opens, highs, lows, closes, volumes;

    auto currentDate = fromDate;
    while (currentDate <= toDate) {
      dates.push_back(DateTime(currentDate));
      opens.push_back(100.0);
      highs.push_back(105.0);
      lows.push_back(95.0);
      closes.push_back(102.0);
      volumes.push_back(1000000.0);
      currentDate = currentDate + chrono_days(1);
    }

    if (dates.empty()) {
      return DataFrame{};
    }

    return epoch_frame::make_dataframe<double>(
        epoch_frame::factory::index::make_datetime_index(dates),
        {opens, highs, lows, closes, volumes},
        {"open", "high", "low", "close", "volume"});
  }
};

class CacheOrchestratorTestFixture {
public:
  CacheOrchestratorTestFixture() {
    tempDir = std::filesystem::temp_directory_path() / "cache_orchestrator_test";
    std::filesystem::create_directories(tempDir);

    // Create fixed time provider for deterministic testing
    timeProvider = std::make_shared<FixedTimeProvider>();

    auto manifestPath = tempDir / "manifest.json";
    manifest = std::make_shared<CacheManifest>(manifestPath, timeProvider);
    orchestrator = std::make_shared<CacheOrchestrator>(manifest, timeProvider);
    fetcher = std::make_unique<MockDataFetcher>();
  }

  ~CacheOrchestratorTestFixture() {
    std::filesystem::remove_all(tempDir);
  }

  CacheLoadParams createLoadParams(
      const std::string& startDate = "2024-01-01",
      const std::string& endDate = "2024-01-10",
      bool forceRefreshToday = false) {
    CacheLoadParams params{
      data_sdk::asset::AssetConstants::instance().AAPL,
      DataCategory::MinuteBars,
      DateTime::from_date_str(startDate).date(),
      DateTime::from_date_str(endDate).date(),
      tempDir,
      3600,
      true,
      forceRefreshToday
    };
    return params;
  }

  std::filesystem::path tempDir;
  std::shared_ptr<CacheManifest> manifest;
  std::shared_ptr<CacheOrchestrator> orchestrator;
  std::unique_ptr<MockDataFetcher> fetcher;
  std::shared_ptr<FixedTimeProvider> timeProvider;
};

TEST_CASE("CacheOrchestrator: determineFetchStrategy", "[cache_orchestrator]") {
  CacheOrchestratorTestFixture fixture;

  SECTION("Empty cache returns FULL strategy") {
    auto params = fixture.createLoadParams();
    CacheProbeResult probe{};  // Empty probe result

    auto strategy = fixture.orchestrator->determineFetchStrategy(params, probe);

    REQUIRE(strategy.type == FetchStrategy::Type::FULL);
    REQUIRE(strategy.fetchFrom.has_value());
    REQUIRE(strategy.fetchTo.has_value());
    // With buffering, fetchFrom/fetchTo extend beyond requested range
    // MinuteBars: ±7 days, DailyBars: ±30 days
    REQUIRE(*strategy.fetchFrom <= params.fromDate);
    REQUIRE(*strategy.fetchTo >= params.toDate);
  }

  SECTION("Complete cache returns NONE strategy") {
    auto params = fixture.createLoadParams();
    CacheProbeResult probe;
    probe.hasData = true;
    probe.isComplete = true;
    probe.isExpired = false;

    auto strategy = fixture.orchestrator->determineFetchStrategy(params, probe);

    REQUIRE(strategy.type == FetchStrategy::Type::NONE);
    REQUIRE_FALSE(strategy.fetchFrom.has_value());
    REQUIRE_FALSE(strategy.fetchTo.has_value());
  }

  SECTION("Expired cache returns FULL strategy") {
    auto params = fixture.createLoadParams();
    CacheProbeResult probe;
    probe.hasData = true;
    probe.isComplete = true;
    probe.isExpired = true;

    auto strategy = fixture.orchestrator->determineFetchStrategy(params, probe);

    REQUIRE(strategy.type == FetchStrategy::Type::FULL);
  }

  SECTION("Missing days at end returns APPEND_ONLY strategy") {
    auto params = fixture.createLoadParams("2024-01-01", "2024-01-20");

    // Create manifest entry showing we have data up to Jan 10
    CacheManifestEntry manifestEntry{
      params.asset,
      params.category,
      params.fromDate,
      DateTime::from_date_str("2024-01-10").date(),
      std::chrono::system_clock::now(),
      100
    };

    CacheProbeResult probe;
    probe.hasData = true;
    probe.isComplete = false;
    probe.isExpired = false;
    probe.manifest = manifestEntry;


    auto strategy = fixture.orchestrator->determineFetchStrategy(params, probe);

    REQUIRE(strategy.type == FetchStrategy::Type::APPEND_ONLY);
    REQUIRE(strategy.fetchFrom.has_value());
    REQUIRE(strategy.fetchTo.has_value());
  }

  SECTION("Intraday freshness returns TODAY_ONLY strategy") {
    // Use the fixed time provider's "today"
    auto today = fixture.timeProvider->today();
    auto params = fixture.createLoadParams(
        DateTime::from_date_str("2024-01-01").date().repr(),
        today.repr(),
        true  // forceRefreshToday
    );
    params.category = DataCategory::MinuteBars;

    CacheProbeResult probe;
    probe.hasData = true;
    probe.isComplete = true;  // We have all data
    probe.isExpired = false;

    auto strategy = fixture.orchestrator->determineFetchStrategy(params, probe);

    // Should fetch today only despite having complete cache
    REQUIRE(strategy.type == FetchStrategy::Type::TODAY_ONLY);
    REQUIRE(strategy.fetchFrom.has_value());
    REQUIRE(strategy.fetchTo.has_value());
    REQUIRE(*strategy.fetchFrom == today);
    REQUIRE(*strategy.fetchTo == today);
  }
}

TEST_CASE("CacheOrchestrator: executeFetch", "[cache_orchestrator]") {
  CacheOrchestratorTestFixture fixture;

  SECTION("Successful fetch") {
    FetchStrategy strategy;
    strategy.type = FetchStrategy::Type::FULL;
    strategy.fetchFrom = DateTime::from_date_str("2024-01-01").date();
    strategy.fetchTo = DateTime::from_date_str("2024-01-05").date();

    auto params = fixture.createLoadParams();

    // Set up mock expectation
    using trompeloeil::_;
    auto testData = MockDataFetcher::createTestData(*strategy.fetchFrom, *strategy.fetchTo);
    REQUIRE_CALL(*fixture.fetcher, Fetch(_, _, _, _, _))
        .WITH(_1 == params.asset && _2 == params.category && _3 == *strategy.fetchFrom && _4 == *strategy.fetchTo)
        .TIMES(1)
        .LR_RETURN(testData);

    auto result = fixture.orchestrator->executeFetch(strategy, params, *fixture.fetcher);

    REQUIRE(result.has_value());
    REQUIRE(result->num_rows() == 5);  // 5 days of data
    // The mock was called exactly once (enforced by TIMES(1))
  }

  SECTION("Failed fetch") {
    FetchStrategy strategy;
    strategy.type = FetchStrategy::Type::FULL;
    strategy.fetchFrom = DateTime::from_date_str("2024-01-01").date();
    strategy.fetchTo = DateTime::from_date_str("2024-01-05").date();

    auto params = fixture.createLoadParams();

    // Set up mock expectation to return error
    using trompeloeil::_;
    auto errorResult = FetchResult(std::unexpected<std::string>("API error"));
    REQUIRE_CALL(*fixture.fetcher, Fetch(_, _, _, _, _))
        .WITH(_1 == params.asset && _2 == params.category && _3 == *strategy.fetchFrom && _4 == *strategy.fetchTo)
        .TIMES(1)
        .LR_RETURN(errorResult);

    auto result = fixture.orchestrator->executeFetch(strategy, params, *fixture.fetcher);

    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error() == "API error");
    // The mock was called exactly once (enforced by TIMES(1))
  }

  SECTION("NONE strategy returns error") {
    FetchStrategy strategy;
    strategy.type = FetchStrategy::Type::NONE;

    auto params = fixture.createLoadParams();

    auto result = fixture.orchestrator->executeFetch(strategy, params, *fixture.fetcher);

    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error() == "No fetch needed");
    // No fetch should have been made (strategy is NONE)
  }
}

TEST_CASE("CacheOrchestrator: mergeData", "[cache_orchestrator]") {
  CacheOrchestratorTestFixture fixture;

  SECTION("Merge cached and fetched data") {
    // Create cached data
    std::vector<DateTime> cachedDates = {
      DateTime::from_date_str("2024-01-01"),
      DateTime::from_date_str("2024-01-02")
    };

    auto cachedDf = epoch_frame::make_dataframe<double>(
        epoch_frame::factory::index::make_datetime_index(cachedDates),
        {std::vector<double>{100.0, 101.0}},
        {"close"});

    // Create fetched data
    std::vector<DateTime> fetchedDates = {
      DateTime::from_date_str("2024-01-03"),
      DateTime::from_date_str("2024-01-04")
    };

    auto fetchedDf = epoch_frame::make_dataframe<double>(
        epoch_frame::factory::index::make_datetime_index(fetchedDates),
        {std::vector<double>{102.0, 103.0}},
        {"close"});

    CacheProbeResult probe;
    probe.hasData = true;
    probe.data = cachedDf;

    auto result = fixture.orchestrator->mergeData(probe, fetchedDf);

    REQUIRE(result.has_value());
    REQUIRE(result->num_rows() == 4);  // 2 cached + 2 fetched

    // Verify the data values are correct using DataFrame indexing
    REQUIRE(result->iloc(0, "close").template value<double>() == 100.0);  // First cached value
    REQUIRE(result->iloc(1, "close").template value<double>() == 101.0);  // Second cached value
    REQUIRE(result->iloc(2, "close").template value<double>() == 102.0);  // First fetched value
    REQUIRE(result->iloc(3, "close").template value<double>() == 103.0);  // Second fetched value
  }

  SECTION("Only fetched data when no cache") {
    std::vector<DateTime> fetchedDates = {
      DateTime::from_date_str("2024-01-01"),
      DateTime::from_date_str("2024-01-02")
    };

    auto fetchedDf = epoch_frame::make_dataframe<double>(
        epoch_frame::factory::index::make_datetime_index(fetchedDates),
        {std::vector<double>{100.0, 101.0}},
        {"close"});

    CacheProbeResult probe{};  // No cached data

    auto result = fixture.orchestrator->mergeData(probe, fetchedDf);

    REQUIRE(result.has_value());
    REQUIRE(result->num_rows() == 2);
  }

  SECTION("Only cached data when no fetch") {
    std::vector<DateTime> cachedDates = {
      DateTime::from_date_str("2024-01-01"),
      DateTime::from_date_str("2024-01-02")
    };

    auto cachedDf = epoch_frame::make_dataframe<double>(
        epoch_frame::factory::index::make_datetime_index(cachedDates),
        {std::vector<double>{100.0, 101.0}},
        {"close"});

    CacheProbeResult probe;
    probe.hasData = true;
    probe.data = cachedDf;

    std::optional<DataFrame> fetchedData;  // No fetched data

    auto result = fixture.orchestrator->mergeData(probe, fetchedData);

    REQUIRE(result.has_value());
    REQUIRE(result->num_rows() == 2);
  }

  SECTION("No data available") {
    CacheProbeResult probe{};  // No cached data
    std::optional<DataFrame> fetchedData;  // No fetched data

    auto result = fixture.orchestrator->mergeData(probe, fetchedData);

    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error() == "No data available");
  }
}

TEST_CASE("CacheOrchestrator: writeToCache", "[cache_orchestrator]") {
  CacheOrchestratorTestFixture fixture;

  SECTION("Write daily data to cache") {
    std::vector<DateTime> dates = {
      DateTime::from_date_str("2024-01-01"),
      DateTime::from_date_str("2024-01-02"),
      DateTime::from_date_str("2024-01-03")
    };

    auto testData = epoch_frame::make_dataframe<double>(
        epoch_frame::factory::index::make_datetime_index(dates),
        {std::vector<double>{100.0, 101.0, 102.0}},
        {"close"});

    CacheWriteParams params{
      .asset = data_sdk::asset::AssetConstants::instance().AAPL,
      .category = DataCategory::DailyBars,
      .cacheDir = fixture.tempDir,
      .data = testData,
      .enableCache = true
    };

    fixture.orchestrator->writeToCache(params);

    // Check that the file was created
    auto catDir = DataCategoryWrapper::ToString(DataCategory::DailyBars);
    auto assetClassDir = AssetClassWrapper::ToLongFormString(params.asset.GetAssetClass());
    auto expectedPath = fixture.tempDir / catDir / assetClassDir / (params.asset.GetID() + ".arrow");

    REQUIRE(std::filesystem::exists(expectedPath));

    // Verify we can read it back
    auto readResult = epoch_frame::read_arrow(expectedPath, {
      .index_column = data_sdk::ColumnConstants::instance().TIMESTAMP()
    });

    REQUIRE(readResult.ok());
    auto cachedData = readResult.MoveValueUnsafe();
    REQUIRE(cachedData.num_rows() == 3);

    // Verify the cached data values using DataFrame indexing
    REQUIRE((cachedData.iloc(0, "close").template value<double>()) == 100.0);
    REQUIRE((cachedData.iloc(1, "close").template value<double>()) == 101.0);
    REQUIRE((cachedData.iloc(2, "close").template value<double>()) == 102.0);
  }

  SECTION("Skip write when cache disabled") {
    auto testData = epoch_frame::make_dataframe<double>(
        epoch_frame::factory::index::make_datetime_index({DateTime::from_date_str("2024-01-01")}),
        {std::vector<double>{100.0}},
        {"close"});

    CacheWriteParams params{
      .asset = data_sdk::asset::AssetConstants::instance().AAPL,
      .category = DataCategory::DailyBars,
      .cacheDir = fixture.tempDir,
      .data = testData,
      .enableCache = false
    };

    fixture.orchestrator->writeToCache(params);

    // No file should be created
    auto catDir = DataCategoryWrapper::ToString(DataCategory::DailyBars);
    auto assetClassDir = AssetClassWrapper::ToLongFormString(params.asset.GetAssetClass());
    auto expectedPath = fixture.tempDir / catDir / assetClassDir / (params.asset.GetID() + ".arrow");

    REQUIRE_FALSE(std::filesystem::exists(expectedPath));
  }

  SECTION("Skip write when data is empty") {
    DataFrame emptyData{};

    CacheWriteParams params{
      .asset = data_sdk::asset::AssetConstants::instance().AAPL,
      .category = DataCategory::DailyBars,
      .cacheDir = fixture.tempDir,
      .data = emptyData,
      .enableCache = true
    };

    fixture.orchestrator->writeToCache(params);

    // No file should be created
    auto catDir = DataCategoryWrapper::ToString(DataCategory::DailyBars);
    auto assetClassDir = AssetClassWrapper::ToLongFormString(params.asset.GetAssetClass());
    auto expectedPath = fixture.tempDir / catDir / assetClassDir / (params.asset.GetID() + ".arrow");

    REQUIRE_FALSE(std::filesystem::exists(expectedPath));
  }
}

TEST_CASE("CacheOrchestrator: full load workflow integration", "[cache_orchestrator]") {
  CacheOrchestratorTestFixture fixture;

  SECTION("Complete load workflow with empty cache") {
    auto params = fixture.createLoadParams("2024-01-01", "2024-01-03");

    // Set up mock to return test data
    // Note: Fetch will be called with buffered dates (±7 days for MinuteBars)
    using trompeloeil::_;
    auto testData = MockDataFetcher::createTestData(params.fromDate, params.toDate);
    REQUIRE_CALL(*fixture.fetcher, Fetch(_, _, _, _, _))
        .WITH(_1 == params.asset && _2 == params.category)
        // Don't check exact dates - buffering changes them
        .TIMES(1)
        .LR_RETURN(testData);

    auto result = fixture.orchestrator->load(params, *fixture.fetcher);

    REQUIRE(result.has_value());
    REQUIRE(result->num_rows() == 3);  // 3 days of data

    // Verify cache was written (day buckets should exist)
    auto catDir = DataCategoryWrapper::ToString(DataCategory::MinuteBars);
    auto assetClassDir = AssetClassWrapper::ToLongFormString(params.asset.GetAssetClass());
    auto assetDir = fixture.tempDir / catDir / assetClassDir / params.asset.GetID();

    REQUIRE(std::filesystem::exists(assetDir / "2024-01-01.arrow"));
    REQUIRE(std::filesystem::exists(assetDir / "2024-01-02.arrow"));
    REQUIRE(std::filesystem::exists(assetDir / "2024-01-03.arrow"));
  }

  SECTION("Load workflow with partial cache requiring append") {
    // First, populate cache with partial data
    auto initialData = MockDataFetcher::createTestData(
        DateTime::from_date_str("2024-01-01").date(),
        DateTime::from_date_str("2024-01-02").date()
    );

    CacheWriteParams writeParams{
      .asset = data_sdk::asset::AssetConstants::instance().AAPL,
      .category = DataCategory::MinuteBars,
      .cacheDir = fixture.tempDir,
      .data = *initialData,
      .enableCache = true
    };

    fixture.orchestrator->writeToCache(writeParams);

    // Now request a larger range that requires append
    auto params = fixture.createLoadParams("2024-01-01", "2024-01-05");

    // Mock should only be called for the missing range
    using trompeloeil::_;
    auto appendData = MockDataFetcher::createTestData(
        DateTime::from_date_str("2024-01-03").date(),
        DateTime::from_date_str("2024-01-05").date()
    );
    // With APPEND_ONLY strategy, cache has 2024-01-01 to 2024-01-02
    // Fetch starts at 2024-01-03 (day after cache end, no backward buffer)
    // End: 2024-01-05 + 7 days buffer = 2024-01-12
    REQUIRE_CALL(*fixture.fetcher, Fetch(_, _, _, _, _))
        .WITH(_1 == params.asset && _2 == params.category &&
              _3 == DateTime::from_date_str("2024-01-03").date() &&
              _4 == DateTime::from_date_str("2024-01-12").date())
        .TIMES(1)
        .LR_RETURN(appendData);

    auto result = fixture.orchestrator->load(params, *fixture.fetcher);

    REQUIRE(result.has_value());
    REQUIRE(result->num_rows() == 5);  // 2 cached + 3 fetched = 5 total
  }

  SECTION("Load workflow with fetch failure falls back to cached data") {
    // First, populate cache with some data
    auto cachedData = MockDataFetcher::createTestData(
        DateTime::from_date_str("2024-01-01").date(),
        DateTime::from_date_str("2024-01-02").date()
    );

    CacheWriteParams writeParams{
      .asset = data_sdk::asset::AssetConstants::instance().AAPL,
      .category = DataCategory::MinuteBars,
      .cacheDir = fixture.tempDir,
      .data = *cachedData,
      .enableCache = true
    };

    fixture.orchestrator->writeToCache(writeParams);

    // Request larger range, but mock will fail
    auto params = fixture.createLoadParams("2024-01-01", "2024-01-05");

    using trompeloeil::_;
    auto errorResult = FetchResult(std::unexpected<std::string>("Network error"));
    REQUIRE_CALL(*fixture.fetcher, Fetch(_, _, _, _, _))
        .TIMES(1)
        .LR_RETURN(errorResult);

    auto result = fixture.orchestrator->load(params, *fixture.fetcher);

    REQUIRE(result.has_value());  // Should succeed with cached data
    REQUIRE(result->num_rows() == 2);  // Only cached data
  }

  SECTION("PREPEND_ONLY strategy: cache starts after requested start date") {
    // Populate cache with data starting from 2024-01-05
    auto cachedData = MockDataFetcher::createTestData(
        DateTime::from_date_str("2024-01-05").date(),
        DateTime::from_date_str("2024-01-10").date()
    );

    CacheWriteParams writeParams{
      .asset = data_sdk::asset::AssetConstants::instance().AAPL,
      .category = DataCategory::MinuteBars,
      .cacheDir = fixture.tempDir,
      .data = *cachedData,
      .enableCache = true
    };

    fixture.orchestrator->writeToCache(writeParams);

    // Request range from 2024-01-01 (before cache) to 2024-01-10 (cache end)
    auto params = fixture.createLoadParams("2024-01-01", "2024-01-10");

    // Should trigger PREPEND_ONLY strategy
    auto probe = fixture.orchestrator->probeCache(params);
    auto strategy = fixture.orchestrator->determineFetchStrategy(params, probe);

    REQUIRE(strategy.type == FetchStrategy::Type::PREPEND_ONLY);
    REQUIRE(strategy.fetchFrom.has_value());
    REQUIRE(strategy.fetchTo.has_value());
    // Should fetch from 2023-12-25 (buffered) to 2024-01-04 (day before cache)
    REQUIRE(*strategy.fetchFrom == DateTime::from_date_str("2023-12-25").date());
    REQUIRE(*strategy.fetchTo == DateTime::from_date_str("2024-01-04").date());
  }

  SECTION("PREPEND_AND_APPEND strategy: cache in the middle") {
    // Populate cache with data in the middle: 2024-01-05 to 2024-01-10
    auto cachedData = MockDataFetcher::createTestData(
        DateTime::from_date_str("2024-01-05").date(),
        DateTime::from_date_str("2024-01-10").date()
    );

    CacheWriteParams writeParams{
      .asset = data_sdk::asset::AssetConstants::instance().AAPL,
      .category = DataCategory::MinuteBars,
      .cacheDir = fixture.tempDir,
      .data = *cachedData,
      .enableCache = true
    };

    fixture.orchestrator->writeToCache(writeParams);

    // Request range: 2024-01-01 (before cache) to 2024-01-20 (after cache)
    auto params = fixture.createLoadParams("2024-01-01", "2024-01-20");

    // Should trigger PREPEND_AND_APPEND strategy
    auto probe = fixture.orchestrator->probeCache(params);
    auto strategy = fixture.orchestrator->determineFetchStrategy(params, probe);

    REQUIRE(strategy.type == FetchStrategy::Type::PREPEND_AND_APPEND);

    // Prepend: 2023-12-25 (buffered) to 2024-01-04
    REQUIRE(strategy.prependFrom.has_value());
    REQUIRE(strategy.prependTo.has_value());
    REQUIRE(*strategy.prependFrom == DateTime::from_date_str("2023-12-25").date());
    REQUIRE(*strategy.prependTo == DateTime::from_date_str("2024-01-04").date());

    // Append: 2024-01-11 to 2024-01-27 (buffered)
    REQUIRE(strategy.fetchFrom.has_value());
    REQUIRE(strategy.fetchTo.has_value());
    REQUIRE(*strategy.fetchFrom == DateTime::from_date_str("2024-01-11").date());
    REQUIRE(*strategy.fetchTo == DateTime::from_date_str("2024-01-27").date());
  }

  SECTION("PREPEND_AND_APPEND execution: fetch both ends, use cache in middle") {
    // Populate cache: 2024-01-05 to 2024-01-10 (6 rows)
    auto cachedData = MockDataFetcher::createTestData(
        DateTime::from_date_str("2024-01-05").date(),
        DateTime::from_date_str("2024-01-10").date()
    );

    CacheWriteParams writeParams{
      .asset = data_sdk::asset::AssetConstants::instance().AAPL,
      .category = DataCategory::MinuteBars,
      .cacheDir = fixture.tempDir,
      .data = *cachedData,
      .enableCache = true
    };

    fixture.orchestrator->writeToCache(writeParams);

    // Request: 2024-01-01 to 2024-01-20
    auto params = fixture.createLoadParams("2024-01-01", "2024-01-20");

    // Mock prepend data: 2023-12-25 to 2024-01-04 (would be 11 days but let's say 3 rows)
    auto prependData = MockDataFetcher::createTestData(
        DateTime::from_date_str("2024-01-01").date(),
        DateTime::from_date_str("2024-01-04").date()
    );

    // Mock append data: 2024-01-11 to 2024-01-20 (10 rows)
    auto appendData = MockDataFetcher::createTestData(
        DateTime::from_date_str("2024-01-11").date(),
        DateTime::from_date_str("2024-01-20").date()
    );

    using trompeloeil::_;

    // Expect two fetch calls: prepend and append
    REQUIRE_CALL(*fixture.fetcher, Fetch(_, _, _, _, _))
        .WITH(_1 == params.asset && _2 == params.category &&
              _3 == DateTime::from_date_str("2023-12-25").date() &&
              _4 == DateTime::from_date_str("2024-01-04").date())
        .TIMES(1)
        .LR_RETURN(prependData);

    REQUIRE_CALL(*fixture.fetcher, Fetch(_, _, _, _, _))
        .WITH(_1 == params.asset && _2 == params.category &&
              _3 == DateTime::from_date_str("2024-01-11").date() &&
              _4 == DateTime::from_date_str("2024-01-27").date())
        .TIMES(1)
        .LR_RETURN(appendData);

    auto result = fixture.orchestrator->load(params, *fixture.fetcher);

    REQUIRE(result.has_value());
    // Total: 4 prepend + 6 cached + 10 append = 20 rows
    REQUIRE(result->num_rows() == 20);
  }

  SECTION("PREPEND_ONLY execution: prepend fetch fails, use only cache") {
    // Populate cache: 2024-01-05 to 2024-01-10
    auto cachedData = MockDataFetcher::createTestData(
        DateTime::from_date_str("2024-01-05").date(),
        DateTime::from_date_str("2024-01-10").date()
    );

    CacheWriteParams writeParams{
      .asset = data_sdk::asset::AssetConstants::instance().AAPL,
      .category = DataCategory::MinuteBars,
      .cacheDir = fixture.tempDir,
      .data = *cachedData,
      .enableCache = true
    };

    fixture.orchestrator->writeToCache(writeParams);

    // Request: 2024-01-01 to 2024-01-10 (prepend needed)
    auto params = fixture.createLoadParams("2024-01-01", "2024-01-10");

    using trompeloeil::_;

    // Prepend fetch fails (e.g., data doesn't exist before 2024-01-05)
    auto errorResult = FetchResult(std::unexpected<std::string>("No data available for requested range"));
    REQUIRE_CALL(*fixture.fetcher, Fetch(_, _, _, _, _))
        .TIMES(1)
        .LR_RETURN(errorResult);

    auto result = fixture.orchestrator->load(params, *fixture.fetcher);

    // Should still succeed with cached data only
    REQUIRE(result.has_value());
    REQUIRE(result->num_rows() == 6);  // Only cached data
  }
}