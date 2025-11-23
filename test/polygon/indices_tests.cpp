#include <catch2/catch_test_macros.hpp>
#include <cstdlib>
#include <iostream>

#include <epoch_data_sdk/common/enums.hpp>
#include <epoch_data_sdk/common/async_batch.hpp>
#include <epoch_data_sdk/dataloader/factory.hpp>
#include <epoch_data_sdk/dataloader/options.hpp>
#include <epoch_data_sdk/model/asset/asset_constants.hpp>
#include <epoch_frame/datetime.h>
#include <epoch_data_sdk/dataloader/metadata_registry.hpp>
#include "../../src/dataloader/polygon_indices_fetcher.hpp"

using namespace data_sdk;
using namespace data_sdk::dataloader;
using namespace data_sdk::common;

static std::string getApiKey() {
  const char *env = std::getenv("POLYGON_API_KEY");
  return env ? std::string(env) : "";
}

static bool hasApiKey() {
  return !getApiKey().empty();
}

TEST_CASE("PolygonIndicesFetcher - Basic functionality", "[polygon][indices][fetcher]") {
  if (!hasApiKey()) {
    SKIP("POLYGON_API_KEY not set");
  }

  PolygonIndicesFetcher fetcher;

  SECTION("Fetch SPX data (sync, daily)") {
    auto from = epoch_frame::DateTime::from_date_str("2024-01-01").date();
    auto to = epoch_frame::DateTime::from_date_str("2024-01-31").date();

    auto result = fetcher.Fetch("SPX", from, to, true);

    REQUIRE(result.has_value());
    const auto& df = *result;

    // Verify OHLCV schema
    REQUIRE(df.contains("open"));
    REQUIRE(df.contains("high"));
    REQUIRE(df.contains("low"));
    REQUIRE(df.contains("close"));
    REQUIRE(df.contains("volume"));

    // Verify we have data
    REQUIRE(df.num_rows() > 0);

    std::cout << "SPX data: " << df.num_rows() << " rows\n";
  }

  SECTION("Fetch VIX data (sync, daily)") {
    auto from = epoch_frame::DateTime::from_date_str("2024-01-01").date();
    auto to = epoch_frame::DateTime::from_date_str("2024-01-31").date();

    auto result = fetcher.Fetch("VIX", from, to, true);

    REQUIRE(result.has_value());
    REQUIRE(result->num_rows() > 0);

    std::cout << "VIX data: " << result->num_rows() << " rows\n";
  }

  SECTION("Fetch NDX data (sync, daily)") {
    auto from = epoch_frame::DateTime::from_date_str("2024-01-01").date();
    auto to = epoch_frame::DateTime::from_date_str("2024-01-31").date();

    auto result = fetcher.Fetch("NDX", from, to, true);

    REQUIRE(result.has_value());
    REQUIRE(result->num_rows() > 0);

    std::cout << "NDX data: " << result->num_rows() << " rows\n";
  }
}

TEST_CASE("PolygonIndicesFetcher - Async functionality", "[polygon][indices][fetcher][async]") {
  if (!hasApiKey()) {
    SKIP("POLYGON_API_KEY not set");
  }

  PolygonIndicesFetcher fetcher;

  SECTION("Fetch SPX data (async, daily)") {
    auto from = epoch_frame::DateTime::from_date_str("2024-01-01").date();
    auto to = epoch_frame::DateTime::from_date_str("2024-01-31").date();

    auto task = [&]() -> drogon::Task<void> {
      auto result = co_await fetcher.FetchAsync("SPX", from, to, true);

      REQUIRE(result.has_value());
      const auto& df = *result;

      REQUIRE(df.contains("open"));
      REQUIRE(df.contains("high"));
      REQUIRE(df.contains("low"));
      REQUIRE(df.contains("close"));
      REQUIRE(df.contains("volume"));
      REQUIRE(df.num_rows() > 0);

      std::cout << "SPX data (async): " << df.num_rows() << " rows\n";
      co_return;
    };

    drogon::sync_wait(task());
  }

  SECTION("Fetch VIX data (async, daily)") {
    auto from = epoch_frame::DateTime::from_date_str("2024-01-01").date();
    auto to = epoch_frame::DateTime::from_date_str("2024-03-31").date();

    auto task = [&]() -> drogon::Task<void> {
      auto result = co_await fetcher.FetchAsync("VIX", from, to, true);

      REQUIRE(result.has_value());
      REQUIRE(result->num_rows() > 0);

      std::cout << "VIX data (async): " << result->num_rows() << " rows\n";
      co_return;
    };

    drogon::sync_wait(task());
  }
}

TEST_CASE("PolygonIndicesFetcher - Multiple indices", "[polygon][indices][fetcher][batch]") {
  if (!hasApiKey()) {
    SKIP("POLYGON_API_KEY not set");
  }

  PolygonIndicesFetcher fetcher;
  auto from = epoch_frame::DateTime::from_date_str("2024-01-01").date();
  auto to = epoch_frame::DateTime::from_date_str("2024-01-31").date();

  SECTION("Fetch multiple indices in parallel") {
    auto task = [&]() -> drogon::Task<void> {
      // Fetch multiple indices concurrently
      std::vector<drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>> tasks;
      tasks.push_back(fetcher.FetchAsync("SPX", from, to, true));
      tasks.push_back(fetcher.FetchAsync("NDX", from, to, true));
      tasks.push_back(fetcher.FetchAsync("VIX", from, to, true));
      tasks.push_back(fetcher.FetchAsync("DJI", from, to, true));

      auto results = co_await when_all(std::move(tasks));

      REQUIRE(results.size() == 4);
      REQUIRE(results[0].has_value());  // SPX
      REQUIRE(results[1].has_value());  // NDX
      REQUIRE(results[2].has_value());  // VIX
      REQUIRE(results[3].has_value());  // DJI

      std::cout << "Parallel fetch results:\n";
      std::cout << "  SPX: " << results[0]->num_rows() << " rows\n";
      std::cout << "  NDX: " << results[1]->num_rows() << " rows\n";
      std::cout << "  VIX: " << results[2]->num_rows() << " rows\n";
      std::cout << "  DJI: " << results[3]->num_rows() << " rows\n";
      co_return;
    };

    drogon::sync_wait(task());
  }
}

TEST_CASE("PolygonIndicesFetcher - Intraday support", "[polygon][indices][fetcher][intraday]") {
  if (!hasApiKey()) {
    SKIP("POLYGON_API_KEY not set");
  }

  PolygonIndicesFetcher fetcher;

  SECTION("Fetch SPX minute data") {
    auto from = epoch_frame::DateTime::from_date_str("2024-01-02").date();
    auto to = epoch_frame::DateTime::from_date_str("2024-01-02").date();

    auto result = fetcher.Fetch("SPX", from, to, false);  // is_eod=false for minute bars

    REQUIRE(result.has_value());
    const auto& df = *result;

    // Verify OHLCV schema
    REQUIRE(df.contains("open"));
    REQUIRE(df.contains("high"));
    REQUIRE(df.contains("low"));
    REQUIRE(df.contains("close"));
    REQUIRE(df.contains("volume"));

    // Minute data should have many more rows than daily
    REQUIRE(df.num_rows() > 0);

    std::cout << "SPX minute data: " << df.num_rows() << " rows\n";
  }
}

TEST_CASE("Indices - Metadata", "[polygon][indices][metadata]") {
  SECTION("Get metadata for indices") {
    auto metadata = MetadataRegistry::GetIndicesMetadata("SPX");

    REQUIRE(metadata.data_type == "market_index");
    REQUIRE(metadata.description.find("SPX") != std::string::npos);

    // Verify schema (same as AggsClient - OHLCV)
    REQUIRE(metadata.columns.size() > 0);

    // Find OHLCV columns
    bool has_open = false;
    bool has_high = false;
    bool has_low = false;
    bool has_close = false;
    bool has_volume = false;

    for (const auto& col : metadata.columns) {
      if (col.id == "open") has_open = true;
      if (col.id == "high") has_high = true;
      if (col.id == "low") has_low = true;
      if (col.id == "close") has_close = true;
      if (col.id == "volume") has_volume = true;
    }

    REQUIRE(has_open);
    REQUIRE(has_high);
    REQUIRE(has_low);
    REQUIRE(has_close);
    REQUIRE(has_volume);
  }

  SECTION("Metadata for different indices") {
    auto spx_meta = MetadataRegistry::GetIndicesMetadata("SPX");
    auto vix_meta = MetadataRegistry::GetIndicesMetadata("VIX");

    // Should have same schema but different descriptions
    REQUIRE(spx_meta.columns.size() == vix_meta.columns.size());
    REQUIRE(spx_meta.description != vix_meta.description);
    REQUIRE(spx_meta.description.find("SPX") != std::string::npos);
    REQUIRE(vix_meta.description.find("VIX") != std::string::npos);
  }
}

TEST_CASE("DataLoader Integration - Load assets with market indices", "[polygon][indices][integration][dataloader]") {
  if (!hasApiKey()) {
    SKIP("POLYGON_API_KEY not set");
  }

  SECTION("Load SPY with market indices (daily)") {
    // Setup dataloader option
    DataloaderOption opt;
    opt.startDate = epoch_frame::DateTime::from_date_str("2024-01-01").date();
    opt.endDate = epoch_frame::DateTime::from_date_str("2024-01-31").date();
    opt.categories = {DataCategory::DailyBars};
    opt.dataloaderAssets = {asset::AssetConstants::instance().SPY};
    opt.enableCache = false;  // Disable cache for test

    // Add market indices
    opt.AddIndexTicker("SPX");
    opt.AddIndexTicker("VIX");
    opt.AddIndexTicker("NDX");

    std::cout << "Creating dataloader with " << opt.GetIndicesTickers().size()
              << " market indices\n";

    // Create dataloader
    auto dataloader = CreateApiCacheDataLoader(opt);
    REQUIRE(dataloader != nullptr);

    // Load data
    dataloader->LoadData();

    // Get loaded data
    auto data = dataloader->GetStoredData();
    REQUIRE(!data.empty());
    REQUIRE(data.contains(asset::AssetConstants::instance().SPY));

    // Get SPY DataFrame
    const auto& spy_df = data.at(asset::AssetConstants::instance().SPY);
    REQUIRE(spy_df.num_rows() > 0);

    // Verify indices columns are present with "IDX:" prefix
    REQUIRE(spy_df.contains("IDX:SPX:close"));
    REQUIRE(spy_df.contains("IDX:VIX:close"));
    REQUIRE(spy_df.contains("IDX:NDX:close"));

    // Verify all OHLCV columns for indices
    REQUIRE(spy_df.contains("IDX:SPX:open"));
    REQUIRE(spy_df.contains("IDX:SPX:high"));
    REQUIRE(spy_df.contains("IDX:SPX:low"));
    REQUIRE(spy_df.contains("IDX:SPX:volume"));

    std::cout << "SPY DataFrame has " << spy_df.num_rows() << " rows and "
              << spy_df.num_cols() << " columns (including indices)\n";
    std::cout << "Column names: ";
    for (const auto& col : spy_df.columns()) {
      std::cout << col << " ";
    }
    std::cout << "\n";
  }

  SECTION("Load multiple assets with indices") {
    DataloaderOption opt;
    opt.startDate = epoch_frame::DateTime::from_date_str("2024-01-01").date();
    opt.endDate = epoch_frame::DateTime::from_date_str("2024-01-15").date();
    opt.categories = {DataCategory::DailyBars};
    opt.dataloaderAssets = {
        asset::AssetConstants::instance().SPY,
        asset::AssetConstants::instance().QQQ
    };
    opt.enableCache = false;

    // Add indices
    opt.AddIndexTicker("SPX");
    opt.AddIndexTicker("VIX");

    auto dataloader = CreateApiCacheDataLoader(opt);
    REQUIRE(dataloader != nullptr);

    dataloader->LoadData();
    auto data = dataloader->GetStoredData();

    // Verify both assets have indices merged
    REQUIRE(data.contains(asset::AssetConstants::instance().SPY));
    REQUIRE(data.contains(asset::AssetConstants::instance().QQQ));

    const auto& spy_df = data.at(asset::AssetConstants::instance().SPY);
    const auto& qqq_df = data.at(asset::AssetConstants::instance().QQQ);

    REQUIRE(spy_df.contains("IDX:SPX:close"));
    REQUIRE(spy_df.contains("IDX:VIX:close"));
    REQUIRE(qqq_df.contains("IDX:SPX:close"));
    REQUIRE(qqq_df.contains("IDX:VIX:close"));

    std::cout << "SPY: " << spy_df.num_rows() << " rows, " << spy_df.num_cols() << " cols\n";
    std::cout << "QQQ: " << qqq_df.num_rows() << " rows, " << qqq_df.num_cols() << " cols\n";
  }
}

TEST_CASE("DataLoader - Direct indices loading", "[polygon][indices][dataloader][direct]") {
  if (!hasApiKey()) {
    SKIP("POLYGON_API_KEY not set");
  }

  SECTION("Load index data directly (daily)") {
    DataloaderOption opt;
    opt.startDate = epoch_frame::DateTime::from_date_str("2024-01-01").date();
    opt.endDate = epoch_frame::DateTime::from_date_str("2024-01-31").date();
    opt.categories = {DataCategory::DailyBars};
    opt.dataloaderAssets = {asset::AssetConstants::instance().SPY};

    auto dataloader = CreateApiCacheDataLoader(opt);
    REQUIRE(dataloader != nullptr);

    auto from = epoch_frame::DateTime::from_date_str("2024-01-01").date();
    auto to = epoch_frame::DateTime::from_date_str("2024-01-31").date();

    // Load SPX directly using LoadIndicesData
    auto result = dataloader->LoadIndicesData("SPX", from, to, true);

    REQUIRE(result.has_value());
    const auto& df = *result;
    REQUIRE(df.num_rows() > 0);
    REQUIRE(df.contains("open"));
    REQUIRE(df.contains("close"));

    std::cout << "Direct SPX load: " << df.num_rows() << " rows\n";
  }

  SECTION("Load index data directly (async)") {
    DataloaderOption opt;
    opt.startDate = epoch_frame::DateTime::from_date_str("2024-01-01").date();
    opt.endDate = epoch_frame::DateTime::from_date_str("2024-01-31").date();
    opt.categories = {DataCategory::DailyBars};
    opt.dataloaderAssets = {asset::AssetConstants::instance().SPY};

    auto dataloader = CreateApiCacheDataLoader(opt);
    REQUIRE(dataloader != nullptr);

    auto from = epoch_frame::DateTime::from_date_str("2024-01-01").date();
    auto to = epoch_frame::DateTime::from_date_str("2024-01-31").date();

    auto task = [&]() -> drogon::Task<void> {
      auto result = co_await dataloader->LoadIndicesDataAsync("VIX", from, to, true);

      REQUIRE(result.has_value());
      REQUIRE(result->num_rows() > 0);

      std::cout << "Direct VIX load (async): " << result->num_rows() << " rows\n";
      co_return;
    };

    drogon::sync_wait(task());
  }
}
