#include <catch2/catch_test_macros.hpp>
#include "dataloader/archive_fetcher.h"
#include "dataloader/fetcher_provider_default.h"
#include "dataloader/polygon_fetcher.h"
#include <epoch_data_sdk/common/constants.hpp>
#include <epoch_data_sdk/model/asset/asset_constants.hpp>

using namespace data_sdk;
using namespace data_sdk::dataloader;

class FetcherProviderTestFixture {
public:
  FetcherProviderTestFixture() {
    provider = std::make_unique<DefaultFetcherProvider>(std::string(data_sdk::EPOCH_DB_S3));
  }

  // Removed createTestAsset; use constants instead

  std::unique_ptr<DefaultFetcherProvider> provider;
};

TEST_CASE("DefaultFetcherProvider::Get", "[fetcher_provider]") {
  FetcherProviderTestFixture fixture;

  SECTION("returns Polygon fetcher for Polygon vendor") {
    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;

    auto &fetcher1 = fixture.provider->Get(asset, DataCategory::DailyBars);
    auto &fetcher2 = fixture.provider->Get(asset, DataCategory::MinuteBars);

    // Should return the same instance (reference)
    CHECK(&fetcher1 == &fetcher2);
  }

  SECTION("returns Archive fetcher for Archive vendor") {
    auto asset = data_sdk::asset::AssetConstants::instance().ES;

    auto &fetcher1 = fixture.provider->Get(asset, DataCategory::DailyBars);
    auto &fetcher2 = fixture.provider->Get(asset, DataCategory::MinuteBars);

    // Should return the same instance
    CHECK(&fetcher1 == &fetcher2);
  }

  SECTION("returns different fetchers for different vendors/classes") {
    auto polygonAsset =
        data_sdk::asset::AssetConstants::instance().AAPL;
    auto archiveAsset =
        data_sdk::asset::AssetConstants::instance().ES;

    auto &polygonFetcher =
        fixture.provider->Get(polygonAsset, DataCategory::DailyBars);
    auto &archiveFetcher =
        fixture.provider->Get(archiveAsset, DataCategory::DailyBars);

    // Should be different instances (Stocks -> Polygon, Futures -> Archive)
    CHECK(&polygonFetcher != &archiveFetcher);
  }

  SECTION("reuses same fetcher instance across multiple calls") {
    auto asset1 =
        data_sdk::asset::AssetConstants::instance().AAPL;
    auto asset2 =
        data_sdk::asset::AssetConstants::instance().GOOG;

    // Get fetcher multiple times for different assets with same vendor
    auto &fetcher1 = fixture.provider->Get(asset1, DataCategory::DailyBars);
    auto &fetcher2 = fixture.provider->Get(asset2, DataCategory::DailyBars);
    auto &fetcher3 = fixture.provider->Get(asset1, DataCategory::MinuteBars);
    auto &fetcher4 = fixture.provider->Get(asset2, DataCategory::MinuteBars);

    // All should be the same Polygon fetcher instance
    CHECK(&fetcher1 == &fetcher2);
    CHECK(&fetcher2 == &fetcher3);
    CHECK(&fetcher3 == &fetcher4);
  }

  SECTION("handles all data categories") {
    auto asset = data_sdk::asset::AssetConstants::instance().ES;

    // Test with different data categories
    auto &dailyFetcher = fixture.provider->Get(asset, DataCategory::DailyBars);
    auto &minuteFetcher =
        fixture.provider->Get(asset, DataCategory::MinuteBars);

    // Should return the same Archive fetcher regardless of category
    CHECK(&dailyFetcher == &minuteFetcher);
  }
}

TEST_CASE("DefaultFetcherProvider with different asset classes",
          "[fetcher_provider]") {
  FetcherProviderTestFixture fixture;

  SECTION("handles Forex assets") {
    auto asset =
        data_sdk::asset::AssetConstants::instance().EUR_USD;

    REQUIRE_NOTHROW(fixture.provider->Get(asset, DataCategory::MinuteBars));
  }

  SECTION("handles Futures assets") {
    auto asset = data_sdk::asset::AssetConstants::instance().ES;
    REQUIRE_NOTHROW(fixture.provider->Get(asset, DataCategory::DailyBars));

    REQUIRE_NOTHROW(fixture.provider->Get(asset, DataCategory::DailyBars));
  }

  SECTION("handles Crypto assets") {
    auto btc_usd =
        data_sdk::asset::AssetConstants::instance().BTC_USD;
    REQUIRE_NOTHROW(fixture.provider->Get(btc_usd, DataCategory::MinuteBars));
  }
}

TEST_CASE("DefaultFetcherProvider efficiency", "[fetcher_provider]") {
  FetcherProviderTestFixture fixture;

  SECTION("efficiently handles 500+ assets") {
    std::vector<data_sdk::asset::Asset> assets;

    // Create 500 assets with mixed vendors
    for (int i = 0; i < 500; ++i) {
      if (i % 2 == 0) {
        assets.push_back(
            data_sdk::asset::AssetConstants::instance().AAPL);
      } else {
        assets.push_back(
            data_sdk::asset::AssetConstants::instance().ES);
      }
    }

    // Track unique fetcher addresses
    std::set<IDataFetcher *> uniqueFetchers;

    // Get fetchers for all assets
    for (const auto &asset : assets) {
      auto &fetcher = fixture.provider->Get(asset, DataCategory::DailyBars);
      uniqueFetchers.insert(&fetcher);
    }

    // Should only have 2 unique fetchers (Polygon, Archive)
    CHECK(uniqueFetchers.size() == 2);
  }

  SECTION("consistent fetcher reuse across categories and assets") {
    // Create multiple assets with same vendor
    auto asset1 =
        data_sdk::asset::AssetConstants::instance().AAPL;
    auto asset2 =
        data_sdk::asset::AssetConstants::instance().GOOG;
    auto asset3 =
        data_sdk::asset::AssetConstants::instance().MSFT;

    // Get fetchers with different combinations
    auto &f1 = fixture.provider->Get(asset1, DataCategory::DailyBars);
    auto &f2 = fixture.provider->Get(asset2, DataCategory::MinuteBars);
    auto &f3 = fixture.provider->Get(asset3, DataCategory::DailyBars);
    auto &f4 = fixture.provider->Get(asset1, DataCategory::MinuteBars);
    auto &f5 = fixture.provider->Get(asset2, DataCategory::DailyBars);

    // All should be the same Polygon fetcher
    CHECK(&f1 == &f2);
    CHECK(&f2 == &f3);
    CHECK(&f3 == &f4);
    CHECK(&f4 == &f5);
  }
}
