#include <catch2/catch_test_macros.hpp>
#include <catch2/trompeloeil.hpp>
#include "dataloader/api_cache_dataloader.h"
#include <epoch_data_sdk/dataloader/cache/provider.hpp>
#include <epoch_data_sdk/dataloader/fetcher.hpp>
#include <epoch_data_sdk/dataloader/fetch_kwargs.hpp>
#include <epoch_data_sdk/model/asset/asset_constants.hpp>
#include <epoch_frame/dataframe.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/series.h>
#include <memory>

using namespace data_sdk;
using namespace data_sdk::dataloader;
using namespace epoch_frame;
using namespace trompeloeil;
using data_sdk::dataloader::FetchKwargs;
using data_sdk::dataloader::NoKwargs;

// Mock classes
class MockCacheProvider : public ICacheProvider {
public:
  MAKE_MOCK5(
      TryLoad,
      std::optional<DataFrame>(const std::optional<std::filesystem::path> &,
                               const asset::Asset &, DataCategory,
                               std::uint64_t, std::filesystem::path &),
      const);

  MAKE_MOCK4(Write,
             void(const std::optional<std::filesystem::path> &,
                  const std::filesystem::path &, bool, const DataFrame &),
             const);

  MAKE_MOCK7(AppendWrite,
             DataFrame(const std::optional<std::filesystem::path> &,
                       const asset::Asset &, DataCategory, std::uint64_t, bool,
                       const DataFrame &, std::filesystem::path *),
             const);

  // Implement orchestration to keep existing tests unchanged
  std::expected<DataFrame, std::string>
  LoadWithCache(const std::optional<std::filesystem::path> &cacheDir,
                const asset::Asset &asset, DataCategory category,
                std::uint64_t ttl, bool enableCache, const Date &fromDate,
                const Date &toDate,
                const std::function<std::expected<DataFrame, std::string>(
                    const Date &, const Date &)> &fetchFn) const override {
    // Do not touch cache if disabled
    if (!enableCache || !cacheDir) {
      auto fetched = fetchFn(fromDate, toDate);
      if (!fetched)
        return std::unexpected(fetched.error());
      return *fetched;
    }
    std::filesystem::path cachePath;
    (void)TryLoad(cacheDir, asset, category, ttl, cachePath);
    auto fetched = fetchFn(fromDate, toDate);
    if (!fetched)
      return std::unexpected(fetched.error());
    std::filesystem::path outPath;
    (void)AppendWrite(cacheDir, asset, category, ttl, enableCache, *fetched,
                      &outPath);
    return *fetched;
  }

  // Async version
  drogon::Task<std::expected<DataFrame, std::string>>
  LoadWithCacheAsync(const std::optional<std::filesystem::path> &cacheDir,
                     const asset::Asset &asset, DataCategory category,
                     std::uint64_t ttl, bool enableCache, const Date &fromDate,
                     const Date &toDate,
                     const std::function<drogon::Task<std::expected<DataFrame, std::string>>(
                         const Date &, const Date &)> &fetchAsyncFn) const override {
    if (!enableCache || !cacheDir) {
      auto fetched = co_await fetchAsyncFn(fromDate, toDate);
      if (!fetched)
        co_return std::unexpected(fetched.error());
      co_return *fetched;
    }
    std::filesystem::path cachePath;
    (void)TryLoad(cacheDir, asset, category, ttl, cachePath);
    auto fetched = co_await fetchAsyncFn(fromDate, toDate);
    if (!fetched)
      co_return std::unexpected(fetched.error());
    std::filesystem::path outPath;
    (void)AppendWrite(cacheDir, asset, category, ttl, enableCache, *fetched,
                      &outPath);
    co_return *fetched;
  }
};

class MockDataFetcher : public IDataFetcher {
public:
  MAKE_MOCK5(Fetch,
             FetchResult(const asset::Asset &, DataCategory, const Date &,
                         const Date &, const FetchKwargs &),
             const);

  // Async version - wraps sync Fetch in coroutine
  drogon::Task<FetchResult> FetchAsync(const asset::Asset &asset, DataCategory category,
                                       const Date &fromDate, const Date &toDate,
                                       const FetchKwargs &kwargs = NoKwargs{}) const override {
    co_return Fetch(asset, category, fromDate, toDate, kwargs);
  }
};

class MockFetcherProvider : public IFetcherProvider {
public:
  MockFetcherProvider() = default;

  MAKE_MOCK2(Get, IDataFetcher &(const asset::Asset &, DataCategory), const);

  MAKE_MOCK1(Get, IDataFetcher &(DataCategory), const);
};

class ApiCacheDataloaderTestFixture {
public:
  ApiCacheDataloaderTestFixture() {
    // Create mock providers
    mockCache = std::make_shared<MockCacheProvider>();
    mockFetcherProvider = std::make_shared<MockFetcherProvider>();
    mockFetcher = std::make_unique<MockDataFetcher>();

    // Setup default options
    option.startDate = DateTime::from_date_str("2024-01-01").date();
    option.endDate = DateTime::from_date_str("2024-01-31").date();
    option.AddRequest(DataCategory::DailyBars);
    option.cacheDir = "/tmp/test_cache";
    option.enableCache = true;
    option.cacheTTLSeconds = 3600;
  }

  DataFrame createTestDataFrame(int numRows = 10) {
    std::vector<DateTime> dates;
    std::vector<double> opens, highs, lows, closes, volumes;

    auto start = DateTime::from_date_str("2024-01-01");
    for (int i = 0; i < numRows; ++i) {
      dates.push_back(start + chrono_days(i));
      opens.push_back(100.0 + i);
      highs.push_back(105.0 + i);
      lows.push_back(95.0 + i);
      closes.push_back(102.0 + i);
      volumes.push_back(1000000.0 + i * 10000);
    }

    return epoch_frame::make_dataframe<double>(
        epoch_frame::factory::index::make_datetime_index(dates),
        {opens, highs, lows, closes, volumes}, {"O", "h", "l", "c", "v"});
  }

  DataloaderOption option;
  std::shared_ptr<MockCacheProvider> mockCache;
  std::shared_ptr<MockFetcherProvider> mockFetcherProvider;
  std::unique_ptr<MockDataFetcher> mockFetcher;
};

TEST_CASE("ApiCacheDataloader::LoadAssetBars with cache enabled",
          "[api_cache_dataloader]") {
  ApiCacheDataloaderTestFixture fixture;
  const auto &assets = data_sdk::asset::AssetConstants::instance();

  auto asset = assets.AAPL;
  auto testDf = fixture.createTestDataFrame();

  SECTION("fetches and caches when no cache exists") {
    // Setup expectations
    ALLOW_CALL(*fixture.mockCache, TryLoad(_, _, _, _, _)).RETURN(std::nullopt);
    ALLOW_CALL(*fixture.mockFetcherProvider, Get(_, _))
        .LR_RETURN(std::ref(*fixture.mockFetcher));

    REQUIRE_CALL(*fixture.mockFetcher,
                 Fetch(asset, DataCategory::DailyBars, _, _, _))
        .RETURN(testDf);

    std::filesystem::path dummyPath = "/tmp/test_cache/AAPL.arrow";
    REQUIRE_CALL(
        *fixture.mockCache,
        AppendWrite(_, asset, DataCategory::DailyBars, 3600, true, _, _))
        .LR_SIDE_EFFECT(*_7 = dummyPath)
        .RETURN(testDf);

    ApiCacheDataloader loader(fixture.option, fixture.mockCache,
                              fixture.mockFetcherProvider);

    auto result = loader.LoadAssetBars(asset, DataCategory::DailyBars);

    REQUIRE(result.has_value());
    CHECK(result->num_rows() == testDf.num_rows());
  }

  SECTION("uses AppendWrite to merge cached and new data") {
    auto cachedDf = fixture.createTestDataFrame(5);
    auto newDf = fixture.createTestDataFrame(10);

    ALLOW_CALL(*fixture.mockCache, TryLoad(_, _, _, _, _)).RETURN(std::nullopt);
    ALLOW_CALL(*fixture.mockFetcherProvider, Get(_, _))
        .LR_RETURN(std::ref(*fixture.mockFetcher));

    REQUIRE_CALL(*fixture.mockFetcher,
                 Fetch(asset, DataCategory::DailyBars, _, _, _))
        .RETURN(newDf);

    // AppendWrite should handle the merge internally
    REQUIRE_CALL(
        *fixture.mockCache,
        AppendWrite(_, asset, DataCategory::DailyBars, 3600, true, _, _))
        .RETURN(newDf);

    ApiCacheDataloader loader(fixture.option, fixture.mockCache,
                              fixture.mockFetcherProvider);

    auto result = loader.LoadAssetBars(asset, DataCategory::DailyBars);

    REQUIRE(result.has_value());
    CHECK(result->num_rows() == newDf.num_rows());
  }
}

TEST_CASE("ApiCacheDataloader::LoadAssetBars with cache disabled",
          "[api_cache_dataloader]") {
  ApiCacheDataloaderTestFixture fixture;
  fixture.option.enableCache = false;
  const auto &assets = data_sdk::asset::AssetConstants::instance();

  auto asset = assets.GOOG;
  auto testDf = fixture.createTestDataFrame();

  SECTION("fetches without caching when cache is disabled") {
    ALLOW_CALL(*fixture.mockFetcherProvider, Get(_, _))
        .LR_RETURN(std::ref(*fixture.mockFetcher));

    REQUIRE_CALL(*fixture.mockFetcher,
                 Fetch(asset, DataCategory::DailyBars, _, _, _))
        .RETURN(testDf);

    // Should not call any cache methods when disabled
    FORBID_CALL(*fixture.mockCache, TryLoad(_, _, _, _, _));
    FORBID_CALL(*fixture.mockCache, Write(_, _, _, _));
    FORBID_CALL(*fixture.mockCache, AppendWrite(_, _, _, _, _, _, _));

    ApiCacheDataloader loader(fixture.option, fixture.mockCache,
                              fixture.mockFetcherProvider);

    auto result = loader.LoadAssetBars(asset, DataCategory::DailyBars);

    REQUIRE(result.has_value());
    CHECK(result->num_rows() == testDf.num_rows());
  }
}

TEST_CASE("ApiCacheDataloader::LoadAssetBars error handling",
          "[api_cache_dataloader]") {
  ApiCacheDataloaderTestFixture fixture;
  const auto &assets = data_sdk::asset::AssetConstants::instance();

  auto asset = assets.TSLA;

  SECTION("returns error when fetch fails") {
    ALLOW_CALL(*fixture.mockCache, TryLoad(_, _, _, _, _)).RETURN(std::nullopt);
    ALLOW_CALL(*fixture.mockFetcherProvider, Get(_, _))
        .LR_RETURN(std::ref(*fixture.mockFetcher));

    REQUIRE_CALL(*fixture.mockFetcher,
                 Fetch(asset, DataCategory::DailyBars, _, _, _))
        .RETURN(std::unexpected("Network error: Connection timeout"));

    ApiCacheDataloader loader(fixture.option, fixture.mockCache,
                              fixture.mockFetcherProvider);

    auto result = loader.LoadAssetBars(asset, DataCategory::DailyBars);

    REQUIRE(!result.has_value());
    CHECK(result.error() == "Network error: Connection timeout");
  }
}

TEST_CASE("ApiCacheDataloader::LoadData parallel processing",
          "[api_cache_dataloader]") {
  ApiCacheDataloaderTestFixture fixture;
  const auto &assets = data_sdk::asset::AssetConstants::instance();

  SECTION("loads multiple assets in parallel") {
    auto asset1 = assets.AAPL;
    auto asset2 = assets.GOOG;
    auto asset3 = assets.MSFT;

    auto df1 = fixture.createTestDataFrame(5);
    auto df2 = fixture.createTestDataFrame(7);
    auto df3 = fixture.createTestDataFrame(10);

    ALLOW_CALL(*fixture.mockCache, TryLoad(_, _, _, _, _)).RETURN(std::nullopt);
    // Setup assets in option
    asset::AssetHashSet assetSet{asset1, asset2, asset3};
    fixture.option.dataloaderAssets = assetSet;

    // Setup expectations for parallel loads
    ALLOW_CALL(*fixture.mockFetcherProvider, Get(_, _))
        .LR_RETURN(std::ref(*fixture.mockFetcher));

    ALLOW_CALL(*fixture.mockFetcher, Fetch(asset1, _, _, _, _)).RETURN(df1);
    ALLOW_CALL(*fixture.mockFetcher, Fetch(asset2, _, _, _, _)).RETURN(df2);
    ALLOW_CALL(*fixture.mockFetcher, Fetch(asset3, _, _, _, _)).RETURN(df3);

    ALLOW_CALL(*fixture.mockCache, AppendWrite(_, asset1, _, _, _, _, _))
        .RETURN(df1);
    ALLOW_CALL(*fixture.mockCache, AppendWrite(_, asset2, _, _, _, _, _))
        .RETURN(df2);
    ALLOW_CALL(*fixture.mockCache, AppendWrite(_, asset3, _, _, _, _, _))
        .RETURN(df3);

    // Setup benchmark asset (SPY)
    auto benchmarkAsset = assets.SPY;
    auto benchmarkDf = fixture.createTestDataFrame(20);

    ALLOW_CALL(*fixture.mockFetcher, Fetch(benchmarkAsset, _, _, _, _))
        .RETURN(benchmarkDf);
    ALLOW_CALL(*fixture.mockCache,
               AppendWrite(_, benchmarkAsset, _, _, _, _, _))
        .RETURN(benchmarkDf);

    ApiCacheDataloader loader(fixture.option, fixture.mockCache,
                              fixture.mockFetcherProvider);

    data_sdk::events::ScopedProgressEmitter emitter;
    loader.LoadData(emitter);

    // Check that all assets were loaded
    auto loadedData = loader.GetStoredData();
    CHECK(loadedData.size() == 3);
    CHECK(loadedData.count(asset1) == 1);
    CHECK(loadedData.count(asset2) == 1);
    CHECK(loadedData.count(asset3) == 1);
  }
}

TEST_CASE("ApiCacheDataloader with different data categories",
          "[api_cache_dataloader]") {
  ApiCacheDataloaderTestFixture fixture;
  const auto &assets = data_sdk::asset::AssetConstants::instance();

  SECTION("handles minute bars") {
    // Clear default DailyBars before adding MinuteBars (can't mix them)
    fixture.option.requests.clear();
    fixture.option.AddRequest(DataCategory::MinuteBars);
    auto asset = assets.AMZN;
    auto testDf = fixture.createTestDataFrame(390); // Full trading day

    ALLOW_CALL(*fixture.mockCache, TryLoad(_, _, _, _, _)).RETURN(std::nullopt);
    ALLOW_CALL(*fixture.mockFetcherProvider, Get(_, _))
        .LR_RETURN(std::ref(*fixture.mockFetcher));

    REQUIRE_CALL(*fixture.mockFetcher,
                 Fetch(asset, DataCategory::MinuteBars, _, _, _))
        .RETURN(testDf);

    REQUIRE_CALL(*fixture.mockCache,
                 AppendWrite(_, asset, DataCategory::MinuteBars, _, _, _, _))
        .RETURN(testDf);

    ApiCacheDataloader loader(fixture.option, fixture.mockCache,
                              fixture.mockFetcherProvider);

    auto result = loader.LoadAssetBars(asset, DataCategory::MinuteBars);

    REQUIRE(result.has_value());
    CHECK(result->num_rows() == 390);
  }

  SECTION("handles daily bars") {
    fixture.option.AddRequest(DataCategory::DailyBars);
    auto asset = assets.IBM;
    auto testDf = fixture.createTestDataFrame(252); // Trading year

    ALLOW_CALL(*fixture.mockCache, TryLoad(_, _, _, _, _)).RETURN(std::nullopt);
    ALLOW_CALL(*fixture.mockFetcherProvider, Get(_, _))
        .LR_RETURN(std::ref(*fixture.mockFetcher));

    REQUIRE_CALL(*fixture.mockFetcher,
                 Fetch(asset, DataCategory::DailyBars, _, _, _))
        .RETURN(testDf);

    REQUIRE_CALL(*fixture.mockCache,
                 AppendWrite(_, asset, DataCategory::DailyBars, _, _, _, _))
        .RETURN(testDf);

    ApiCacheDataloader loader(fixture.option, fixture.mockCache,
                              fixture.mockFetcherProvider);

    auto result = loader.LoadAssetBars(asset, DataCategory::DailyBars);

    REQUIRE(result.has_value());
    CHECK(result->num_rows() == 252);
  }
}

TEST_CASE("ApiCacheDataloader with different asset types",
          "[api_cache_dataloader]") {
  ApiCacheDataloaderTestFixture fixture;
  const auto &assets = data_sdk::asset::AssetConstants::instance();

  SECTION("handles forex assets") {
    auto asset = assets.EUR_USD;
    auto testDf = fixture.createTestDataFrame(1440); // 24 hours of minute data

    ALLOW_CALL(*fixture.mockCache, TryLoad(_, _, _, _, _)).RETURN(std::nullopt);
    ALLOW_CALL(*fixture.mockFetcherProvider, Get(_, _))
        .LR_RETURN(std::ref(*fixture.mockFetcher));

    REQUIRE_CALL(*fixture.mockFetcher,
                 Fetch(asset, DataCategory::MinuteBars, _, _, _))
        .RETURN(testDf);

    REQUIRE_CALL(*fixture.mockCache,
                 AppendWrite(_, asset, DataCategory::MinuteBars, _, _, _, _))
        .RETURN(testDf);

    // Clear default DailyBars before adding MinuteBars (can't mix them)
    fixture.option.requests.clear();
    fixture.option.AddRequest(DataCategory::MinuteBars);
    ApiCacheDataloader loader(fixture.option, fixture.mockCache,
                              fixture.mockFetcherProvider);

    auto result = loader.LoadAssetBars(asset, DataCategory::MinuteBars);

    REQUIRE(result.has_value());
    CHECK(result->num_rows() == 1440);
  }

  SECTION("handles futures assets") {
    auto asset = assets.ES;
    auto testDf = fixture.createTestDataFrame(20);

    ALLOW_CALL(*fixture.mockCache, TryLoad(_, _, _, _, _)).RETURN(std::nullopt);
    ALLOW_CALL(*fixture.mockFetcherProvider, Get(_, _))
        .LR_RETURN(std::ref(*fixture.mockFetcher));

    REQUIRE_CALL(*fixture.mockFetcher,
                 Fetch(asset, DataCategory::DailyBars, _, _, _))
        .RETURN(testDf);

    REQUIRE_CALL(*fixture.mockCache,
                 AppendWrite(_, asset, DataCategory::DailyBars, _, _, _, _))
        .RETURN(testDf);

    ApiCacheDataloader loader(fixture.option, fixture.mockCache,
                              fixture.mockFetcherProvider);

    auto result = loader.LoadAssetBars(asset, DataCategory::DailyBars);

    REQUIRE(result.has_value());
    CHECK(result->num_rows() == 20);
  }

  SECTION("handles crypto assets") {
    auto asset = assets.BTC_USD;
    auto testDf = fixture.createTestDataFrame(31); // Month of daily data

    ALLOW_CALL(*fixture.mockCache, TryLoad(_, _, _, _, _)).RETURN(std::nullopt);
    ALLOW_CALL(*fixture.mockFetcherProvider, Get(_, _))
        .LR_RETURN(std::ref(*fixture.mockFetcher));

    REQUIRE_CALL(*fixture.mockFetcher,
                 Fetch(asset, DataCategory::DailyBars, _, _, _))
        .RETURN(testDf);

    REQUIRE_CALL(*fixture.mockCache,
                 AppendWrite(_, asset, DataCategory::DailyBars, _, _, _, _))
        .RETURN(testDf);

    ApiCacheDataloader loader(fixture.option, fixture.mockCache,
                              fixture.mockFetcherProvider);

    auto result = loader.LoadAssetBars(asset, DataCategory::DailyBars);

    REQUIRE(result.has_value());
    CHECK(result->num_rows() == 31);
  }
}

TEST_CASE("ApiCacheDataloader::LoadData batch processing mode",
          "[api_cache_dataloader][batch]") {
  ApiCacheDataloaderTestFixture fixture;
  const auto &assets = data_sdk::asset::AssetConstants::instance();

  SECTION("processes 15 assets in batches of 5") {
    // Create 15 test assets (stocks + crypto + futures for variety)
    std::vector<asset::Asset> testAssets = {
        assets.AAPL, assets.GOOG, assets.MSFT, assets.AMZN, assets.TSLA,
        assets.IBM,  assets.AA,   assets.BTC_USD, assets.ETH_USD, assets.DOGE_USD,
        assets.LTC_USD, assets.ES, assets.GC, assets.ZC, assets.SPX};

    asset::AssetHashSet assetSet(testAssets.begin(), testAssets.end());
    fixture.option.dataloaderAssets = assetSet;

    // Enable batch mode with batch size of 5
    fixture.option.useBatchFetching = true;
    fixture.option.batchSize = 5;

    // Setup mock responses for all assets
    ALLOW_CALL(*fixture.mockCache, TryLoad(_, _, _, _, _)).RETURN(std::nullopt);
    ALLOW_CALL(*fixture.mockFetcherProvider, Get(_, _))
        .LR_RETURN(std::ref(*fixture.mockFetcher));

    // Create test DataFrame once and use for all assets
    auto testDf = fixture.createTestDataFrame(10);
    // Store expectations to keep them alive outside loop scope
    std::vector<std::unique_ptr<trompeloeil::expectation>> expectations;
    for (const auto &asset : testAssets) {
      expectations.push_back(
          NAMED_ALLOW_CALL(*fixture.mockFetcher, Fetch(asset, _, _, _, _)).RETURN(testDf));
      expectations.push_back(
          NAMED_ALLOW_CALL(*fixture.mockCache, AppendWrite(_, asset, _, _, _, _, _)).RETURN(testDf));
    }

    // Setup benchmark asset (SPY)
    auto benchmarkAsset = assets.SPY;
    auto benchmarkDf = fixture.createTestDataFrame(20);
    ALLOW_CALL(*fixture.mockCache, TryLoad(_, benchmarkAsset, _, _, _))
        .RETURN(std::nullopt);
    ALLOW_CALL(*fixture.mockFetcher, Fetch(benchmarkAsset, _, _, _, _))
        .RETURN(benchmarkDf);
    ALLOW_CALL(*fixture.mockCache,
               AppendWrite(_, benchmarkAsset, _, _, _, _, _))
        .RETURN(benchmarkDf);

    ApiCacheDataloader loader(fixture.option, fixture.mockCache,
                              fixture.mockFetcherProvider);

    // Load data with batch processing
    data_sdk::events::ScopedProgressEmitter emitter;
    loader.LoadData(emitter);

    // Verify all 15 assets were loaded
    auto loadedData = loader.GetStoredData();
    CHECK(loadedData.size() == 15);

    for (const auto &asset : testAssets) {
      CHECK(loadedData.count(asset) == 1);
    }
  }

  SECTION("batch size of 1 processes fully sequentially") {
    std::vector<asset::Asset> testAssets = {assets.AAPL, assets.GOOG,
                                            assets.MSFT};

    asset::AssetHashSet assetSet(testAssets.begin(), testAssets.end());
    fixture.option.dataloaderAssets = assetSet;

    // Set batch size to 1 for fully sequential processing
    fixture.option.useBatchFetching = true;
    fixture.option.batchSize = 1;

    ALLOW_CALL(*fixture.mockCache, TryLoad(_, _, _, _, _)).RETURN(std::nullopt);
    ALLOW_CALL(*fixture.mockFetcherProvider, Get(_, _))
        .LR_RETURN(std::ref(*fixture.mockFetcher));

    auto testDf = fixture.createTestDataFrame(5);
    std::vector<std::unique_ptr<trompeloeil::expectation>> expectations;
    for (const auto &asset : testAssets) {
      expectations.push_back(
          NAMED_ALLOW_CALL(*fixture.mockFetcher, Fetch(asset, _, _, _, _)).RETURN(testDf));
      expectations.push_back(
          NAMED_ALLOW_CALL(*fixture.mockCache, AppendWrite(_, asset, _, _, _, _, _)).RETURN(testDf));
    }

    // Setup benchmark
    auto benchmarkDf = fixture.createTestDataFrame(20);
    ALLOW_CALL(*fixture.mockCache, TryLoad(_, assets.SPY, _, _, _))
        .RETURN(std::nullopt);
    ALLOW_CALL(*fixture.mockFetcher, Fetch(assets.SPY, _, _, _, _))
        .RETURN(benchmarkDf);
    ALLOW_CALL(*fixture.mockCache, AppendWrite(_, assets.SPY, _, _, _, _, _))
        .RETURN(benchmarkDf);

    ApiCacheDataloader loader(fixture.option, fixture.mockCache,
                              fixture.mockFetcherProvider);

    data_sdk::events::ScopedProgressEmitter emitter;
    loader.LoadData(emitter);

    auto loadedData = loader.GetStoredData();
    CHECK(loadedData.size() == 3);
  }

  SECTION("disabling batch mode uses full concurrent processing") {
    std::vector<asset::Asset> testAssets = {assets.AAPL, assets.GOOG,
                                            assets.MSFT, assets.AMZN};

    asset::AssetHashSet assetSet(testAssets.begin(), testAssets.end());
    fixture.option.dataloaderAssets = assetSet;

    // Disable batch mode
    fixture.option.useBatchFetching = false;

    ALLOW_CALL(*fixture.mockCache, TryLoad(_, _, _, _, _)).RETURN(std::nullopt);
    ALLOW_CALL(*fixture.mockFetcherProvider, Get(_, _))
        .LR_RETURN(std::ref(*fixture.mockFetcher));

    auto testDf = fixture.createTestDataFrame(8);
    std::vector<std::unique_ptr<trompeloeil::expectation>> expectations;
    for (const auto &asset : testAssets) {
      expectations.push_back(
          NAMED_ALLOW_CALL(*fixture.mockFetcher, Fetch(asset, _, _, _, _)).RETURN(testDf));
      expectations.push_back(
          NAMED_ALLOW_CALL(*fixture.mockCache, AppendWrite(_, asset, _, _, _, _, _)).RETURN(testDf));
    }

    // Setup benchmark
    auto benchmarkDf = fixture.createTestDataFrame(20);
    ALLOW_CALL(*fixture.mockCache, TryLoad(_, assets.SPY, _, _, _))
        .RETURN(std::nullopt);
    ALLOW_CALL(*fixture.mockFetcher, Fetch(assets.SPY, _, _, _, _))
        .RETURN(benchmarkDf);
    ALLOW_CALL(*fixture.mockCache, AppendWrite(_, assets.SPY, _, _, _, _, _))
        .RETURN(benchmarkDf);

    ApiCacheDataloader loader(fixture.option, fixture.mockCache,
                              fixture.mockFetcherProvider);

    data_sdk::events::ScopedProgressEmitter emitter;
    loader.LoadData(emitter);

    // All assets should still load successfully
    auto loadedData = loader.GetStoredData();
    CHECK(loadedData.size() == 4);
  }

  SECTION("large batch size approximates concurrent behavior") {
    std::vector<asset::Asset> testAssets = {assets.AAPL, assets.GOOG,
                                            assets.MSFT, assets.AMZN,
                                            assets.TSLA};

    asset::AssetHashSet assetSet(testAssets.begin(), testAssets.end());
    fixture.option.dataloaderAssets = assetSet;

    // Set batch size larger than asset count
    fixture.option.useBatchFetching = true;
    fixture.option.batchSize = 100;

    ALLOW_CALL(*fixture.mockCache, TryLoad(_, _, _, _, _)).RETURN(std::nullopt);
    ALLOW_CALL(*fixture.mockFetcherProvider, Get(_, _))
        .LR_RETURN(std::ref(*fixture.mockFetcher));

    auto testDf = fixture.createTestDataFrame(6);
    std::vector<std::unique_ptr<trompeloeil::expectation>> expectations;
    for (const auto &asset : testAssets) {
      expectations.push_back(
          NAMED_ALLOW_CALL(*fixture.mockFetcher, Fetch(asset, _, _, _, _)).RETURN(testDf));
      expectations.push_back(
          NAMED_ALLOW_CALL(*fixture.mockCache, AppendWrite(_, asset, _, _, _, _, _)).RETURN(testDf));
    }

    // Setup benchmark
    auto benchmarkDf = fixture.createTestDataFrame(20);
    ALLOW_CALL(*fixture.mockCache, TryLoad(_, assets.SPY, _, _, _))
        .RETURN(std::nullopt);
    ALLOW_CALL(*fixture.mockFetcher, Fetch(assets.SPY, _, _, _, _))
        .RETURN(benchmarkDf);
    ALLOW_CALL(*fixture.mockCache, AppendWrite(_, assets.SPY, _, _, _, _, _))
        .RETURN(benchmarkDf);

    ApiCacheDataloader loader(fixture.option, fixture.mockCache,
                              fixture.mockFetcherProvider);

    data_sdk::events::ScopedProgressEmitter emitter;
    loader.LoadData(emitter);

    // All assets should load in a single batch
    auto loadedData = loader.GetStoredData();
    CHECK(loadedData.size() == 5);
  }
}
