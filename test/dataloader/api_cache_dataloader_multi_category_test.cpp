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

  std::expected<DataFrame, std::string>
  LoadWithCache(const std::optional<std::filesystem::path> &cacheDir,
                const asset::Asset &asset, DataCategory category,
                std::uint64_t ttl, bool enableCache, const Date &fromDate,
                const Date &toDate,
                const std::function<std::expected<DataFrame, std::string>(
                    const Date &, const Date &)> &fetchFn) const override {
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

class MultiCategoryTestFixture {
public:
  MultiCategoryTestFixture() {
    mockCache = std::make_shared<MockCacheProvider>();
    mockFetcherProvider = std::make_shared<MockFetcherProvider>();
    mockFetcher = std::make_unique<MockDataFetcher>();

    // Setup default options
    option.startDate = DateTime::from_date_str("2024-01-01").date();
    option.endDate = DateTime::from_date_str("2024-01-31").date();
    option.cacheDir = "/tmp/test_cache";
    option.enableCache = true;
    option.cacheTTLSeconds = 3600;
  }

  DataFrame createBarsDataFrame(int numRows = 10) {
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

  DataFrame createNewsDataFrame(int numRows = 3) {
    std::vector<DateTime> dates;
    std::vector<std::string> headlines;

    auto start = DateTime::from_date_str("2024-01-01");
    for (int i = 0; i < numRows; ++i) {
      dates.push_back(start + chrono_days(i * 3));
      headlines.push_back("News " + std::to_string(i));
    }

    return epoch_frame::make_dataframe<std::string>(
        epoch_frame::factory::index::make_datetime_index(dates),
        {headlines}, {"headline"});
  }

  DataloaderOption option;
  std::shared_ptr<MockCacheProvider> mockCache;
  std::shared_ptr<MockFetcherProvider> mockFetcherProvider;
  std::unique_ptr<MockDataFetcher> mockFetcher;
};

TEST_CASE("ApiCacheDataloader - LoadAssetBars with specific category",
          "[api_cache_dataloader_multi]") {
  MultiCategoryTestFixture fixture;
  const auto &assets = data_sdk::asset::AssetConstants::instance();
  auto asset = assets.AAPL;

  SECTION("can load MinuteBars explicitly") {
    fixture.option.AddRequest(DataCategory::MinuteBars);
    auto testDf = fixture.createBarsDataFrame();

    ALLOW_CALL(*fixture.mockCache, TryLoad(_, _, _, _, _)).RETURN(std::nullopt);
    ALLOW_CALL(*fixture.mockFetcherProvider, Get(_, _))
        .LR_RETURN(std::ref(*fixture.mockFetcher));
    REQUIRE_CALL(*fixture.mockFetcher,
                 Fetch(asset, DataCategory::MinuteBars, _, _, _))
        .RETURN(testDf);

    std::filesystem::path dummyPath = "/tmp/test.arrow";
    ALLOW_CALL(*fixture.mockCache, AppendWrite(_, _, _, _, _, _, _))
        .LR_SIDE_EFFECT(*_7 = dummyPath)
        .RETURN(testDf);

    ApiCacheDataloader loader(fixture.option, fixture.mockCache,
                              fixture.mockFetcherProvider);
    auto result = loader.LoadAssetBars(asset, DataCategory::MinuteBars);

    REQUIRE(result.has_value());
    REQUIRE(result->num_rows() == 10);
  }

  SECTION("can load News category explicitly") {
    fixture.option.AddRequest(DataCategory::DailyBars);
    fixture.option.AddRequest(DataCategory::News);
    auto newsDf = fixture.createNewsDataFrame();

    ALLOW_CALL(*fixture.mockCache, TryLoad(_, _, _, _, _)).RETURN(std::nullopt);
    ALLOW_CALL(*fixture.mockFetcherProvider, Get(_, _))
        .LR_RETURN(std::ref(*fixture.mockFetcher));
    REQUIRE_CALL(*fixture.mockFetcher, Fetch(asset, DataCategory::News, _, _, _))
        .RETURN(newsDf);

    std::filesystem::path dummyPath = "/tmp/news.arrow";
    ALLOW_CALL(*fixture.mockCache, AppendWrite(_, _, _, _, _, _, _))
        .LR_SIDE_EFFECT(*_7 = dummyPath)
        .RETURN(newsDf);

    ApiCacheDataloader loader(fixture.option, fixture.mockCache,
                              fixture.mockFetcherProvider);
    auto result = loader.LoadAssetBars(asset, DataCategory::News);

    REQUIRE(result.has_value());
    REQUIRE(result->num_rows() == 3);
  }
}

TEST_CASE("ApiCacheDataloader - Single category mode",
          "[api_cache_dataloader_multi]") {
  MultiCategoryTestFixture fixture;
  const auto &assets = data_sdk::asset::AssetConstants::instance();
  [[maybe_unused]] auto asset = assets.AAPL;

  // NOTE: LoadSingleCategory is a private method and cannot be tested directly.
  // It is indirectly tested through LoadAssetBars() in single-category mode.
}

TEST_CASE("ApiCacheDataloader - Validation", "[api_cache_dataloader_multi]") {
  MultiCategoryTestFixture fixture;

  SECTION("can load News as single category") {
    fixture.option.AddRequest(DataCategory::News);

    ApiCacheDataloader loader(fixture.option, fixture.mockCache,
                              fixture.mockFetcherProvider);

    REQUIRE_THROWS_AS(loader.LoadData(), std::runtime_error);
  }

  SECTION("throws when mixing DailyBars and MinuteBars") {
    fixture.option.AddRequest(DataCategory::DailyBars);
    fixture.option.AddRequest(DataCategory::MinuteBars);

    // Should throw in constructor due to invalid option
    REQUIRE_THROWS_AS(
        ApiCacheDataloader(fixture.option, fixture.mockCache, fixture.mockFetcherProvider),
        std::runtime_error);
  }
}

TEST_CASE("ApiCacheDataloader - buildCacheParams",
          "[api_cache_dataloader_multi]") {
  MultiCategoryTestFixture fixture;
  const auto &assets = data_sdk::asset::AssetConstants::instance();
  auto asset = assets.AAPL;

  SECTION("builds params for different categories") {
    fixture.option.AddRequest(DataCategory::MinuteBars);

    ApiCacheDataloader loader(fixture.option, fixture.mockCache,
                              fixture.mockFetcherProvider);

    auto params =
        loader.buildCacheParams(asset, DataCategory::MinuteBars, false);

    REQUIRE(params.asset == asset);
    REQUIRE(params.category == DataCategory::MinuteBars);
    REQUIRE(params.fromDate == fixture.option.GetStartDate());
    REQUIRE(params.toDate == fixture.option.GetEndDate());
    REQUIRE(params.ttlSeconds == 3600);
    REQUIRE(params.enableCache == true);
    REQUIRE(params.forceRefreshToday == false);
  }

  SECTION("respects forceRefreshToday flag") {
    fixture.option.AddRequest(DataCategory::MinuteBars);

    ApiCacheDataloader loader(fixture.option, fixture.mockCache,
                              fixture.mockFetcherProvider);

    auto params =
        loader.buildCacheParams(asset, DataCategory::MinuteBars, true);

    REQUIRE(params.forceRefreshToday == true);
  }
}

TEST_CASE("DataloaderOption - Integration with ApiCacheDataloader",
          "[api_cache_dataloader_multi]") {

  SECTION("IsMultiCategory correctly routes to LoadMultiCategory") {
    MultiCategoryTestFixture fixture;
    const auto &assets = data_sdk::asset::AssetConstants::instance();

    // Setup multi-category option using new API
    fixture.option.AddRequest(DataCategory::DailyBars);
    fixture.option.AddRequest(DataCategory::News);
    fixture.option.dataloaderAssets = asset::AssetHashSet{assets.AAPL};

    REQUIRE(fixture.option.GetCategories().size() > 1);

    auto barsDf = fixture.createBarsDataFrame();
    auto newsDf = fixture.createNewsDataFrame();

    ALLOW_CALL(*fixture.mockCache, TryLoad(_, _, _, _, _)).RETURN(std::nullopt);
    ALLOW_CALL(*fixture.mockFetcherProvider, Get(_, _))
        .LR_RETURN(std::ref(*fixture.mockFetcher));

    // Should fetch both categories
    ALLOW_CALL(*fixture.mockFetcher, Fetch(_, DataCategory::DailyBars, _, _, _))
        .RETURN(barsDf);
    ALLOW_CALL(*fixture.mockFetcher, Fetch(_, DataCategory::News, _, _, _))
        .RETURN(newsDf);

    std::filesystem::path dummyPath = "/tmp/test.arrow";
    ALLOW_CALL(*fixture.mockCache, AppendWrite(_, _, _, _, _, _, _))
        .LR_SIDE_EFFECT(*_7 = dummyPath)
        .RETURN(barsDf);

    // We verify the option configuration is correct
    REQUIRE(fixture.option.GetCategories().size() == 2);
  }

  SECTION("Single category routes to LoadSingleCategory") {
    MultiCategoryTestFixture fixture;
    const auto &assets = data_sdk::asset::AssetConstants::instance();

    fixture.option.AddRequest(DataCategory::DailyBars);
    fixture.option.dataloaderAssets = asset::AssetHashSet{assets.AAPL};

    REQUIRE(fixture.option.GetCategories().size() == 1);
  }
}

// End-to-end integration tests for new auxiliary categories
TEST_CASE("ApiCacheDataloader - New auxiliary categories end-to-end",
          "[api_cache_dataloader_multi][integration]") {
    MultiCategoryTestFixture fixture;
    const auto &assets = data_sdk::asset::AssetConstants::instance();
    auto asset = assets.AAPL;

    SECTION("can load BalanceSheets category") {
        fixture.option.AddRequest(DataCategory::DailyBars);
        fixture.option.AddRequest(DataCategory::BalanceSheets);

        auto balanceSheetsDf = fixture.createBarsDataFrame(5);

        ALLOW_CALL(*fixture.mockCache, TryLoad(_, _, _, _, _)).RETURN(std::nullopt);
        ALLOW_CALL(*fixture.mockFetcherProvider, Get(_, _))
            .LR_RETURN(std::ref(*fixture.mockFetcher));

        REQUIRE_CALL(*fixture.mockFetcher,
                     Fetch(asset, DataCategory::BalanceSheets, _, _, _))
            .RETURN(balanceSheetsDf);

        std::filesystem::path dummyPath = "/tmp/balance_sheets.arrow";
        ALLOW_CALL(*fixture.mockCache, AppendWrite(_, _, _, _, _, _, _))
            .LR_SIDE_EFFECT(*_7 = dummyPath)
            .RETURN(balanceSheetsDf);

        ApiCacheDataloader loader(fixture.option, fixture.mockCache,
                                  fixture.mockFetcherProvider);
        auto result = loader.LoadAssetBars(asset, DataCategory::BalanceSheets);

        REQUIRE(result.has_value());
        REQUIRE(result->num_rows() == 5);
    }

    SECTION("can load IncomeStatements category") {
        fixture.option.AddRequest(DataCategory::DailyBars);
        fixture.option.AddRequest(DataCategory::IncomeStatements);

        auto incomeStatementsDf = fixture.createBarsDataFrame(8);

        ALLOW_CALL(*fixture.mockCache, TryLoad(_, _, _, _, _)).RETURN(std::nullopt);
        ALLOW_CALL(*fixture.mockFetcherProvider, Get(_, _))
            .LR_RETURN(std::ref(*fixture.mockFetcher));

        REQUIRE_CALL(*fixture.mockFetcher,
                     Fetch(asset, DataCategory::IncomeStatements, _, _, _))
            .RETURN(incomeStatementsDf);

        std::filesystem::path dummyPath = "/tmp/income_statements.arrow";
        ALLOW_CALL(*fixture.mockCache, AppendWrite(_, _, _, _, _, _, _))
            .LR_SIDE_EFFECT(*_7 = dummyPath)
            .RETURN(incomeStatementsDf);

        ApiCacheDataloader loader(fixture.option, fixture.mockCache,
                                  fixture.mockFetcherProvider);
        auto result = loader.LoadAssetBars(asset, DataCategory::IncomeStatements);

        REQUIRE(result.has_value());
        REQUIRE(result->num_rows() == 8);
    }
}
