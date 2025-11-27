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

    // Verify OHLC schema (lowercase single letters, no volume for indices)
    REQUIRE(df.contains("o"));
    REQUIRE(df.contains("h"));
    REQUIRE(df.contains("l"));
    REQUIRE(df.contains("c"));

    // Verify NO volume columns (indices don't have volume data)
    REQUIRE_FALSE(df.contains("v"));
    REQUIRE_FALSE(df.contains("vw"));
    REQUIRE_FALSE(df.contains("n"));

    // Should only have 4 columns: o, h, l, c
    REQUIRE(df.num_cols() == 4);

    // Verify we have data
    REQUIRE(df.num_rows() > 0);

    std::cout << "SPX data: " << df.num_rows() << " rows, " << df.num_cols() << " cols\n";
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

      REQUIRE(df.contains("o"));
      REQUIRE(df.contains("h"));
      REQUIRE(df.contains("l"));
      REQUIRE(df.contains("c"));
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

    // Verify OHLC schema
    REQUIRE(df.contains("o"));
    REQUIRE(df.contains("h"));
    REQUIRE(df.contains("l"));
    REQUIRE(df.contains("c"));

    // Minute data should have many more rows than daily
    REQUIRE(df.num_rows() > 0);

    std::cout << "SPX minute data: " << df.num_rows() << " rows\n";
  }
}

TEST_CASE("Indices - Metadata", "[polygon][indices][metadata]") {
  SECTION("Get metadata for daily indices") {
    auto metadata = MetadataRegistry::GetIndicesMetadata(true);  // is_eod = true

    REQUIRE(metadata.data_type == "aggregates");
    REQUIRE(!metadata.description.empty());
    REQUIRE(metadata.index_normalized == true);  // Daily = normalized

    // Verify schema (OHLC only - no v, vw, or n)
    REQUIRE(metadata.columns.size() == 4);  // Only o, h, l, c

    bool has_o = false, has_h = false, has_l = false, has_c = false;
    bool has_v = false, has_vw = false, has_n = false;

    for (const auto& col : metadata.columns) {
      if (col.id == "o") has_o = true;
      if (col.id == "h") has_h = true;
      if (col.id == "l") has_l = true;
      if (col.id == "c") has_c = true;
      if (col.id == "v") has_v = true;
      if (col.id == "vw") has_vw = true;
      if (col.id == "n") has_n = true;
    }

    REQUIRE(has_o);
    REQUIRE(has_h);
    REQUIRE(has_l);
    REQUIRE(has_c);
    REQUIRE_FALSE(has_v);   // No volume for indices
    REQUIRE_FALSE(has_vw);  // No VWAP for indices
    REQUIRE_FALSE(has_n);   // No trade count for indices
  }

  SECTION("Get metadata for intraday indices") {
    auto metadata = MetadataRegistry::GetIndicesMetadata(false);  // is_eod = false

    REQUIRE(metadata.data_type == "aggregates");
    REQUIRE(metadata.index_normalized == false);  // Intraday = non-normalized

    // Verify same OHLC schema
    REQUIRE(metadata.columns.size() == 4);
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
    opt.AddRequest(DataCategory::DailyBars);
    opt.dataloaderAssets = {asset::AssetConstants::instance().SPY};
    opt.cacheDir = "/tmp/epoch_test_cache";  // Provide cache directory
    opt.enableCache = true;  // Enable cache

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

    // Verify indices columns are present with "IDX:" prefix (lowercase single letters)
    REQUIRE(spy_df.contains("IDX:SPX:c"));
    REQUIRE(spy_df.contains("IDX:VIX:c"));
    REQUIRE(spy_df.contains("IDX:NDX:c"));

    // Verify all OHLC columns for indices
    REQUIRE(spy_df.contains("IDX:SPX:o"));
    REQUIRE(spy_df.contains("IDX:SPX:h"));
    REQUIRE(spy_df.contains("IDX:SPX:l"));

    // Verify indices data is not all null
    auto spx_close = spy_df["IDX:SPX:c"];
    auto spx_array = spx_close.contiguous_array();
    auto spx_non_null = spx_array.length() - spx_array.null_count();
    REQUIRE(spx_non_null > 0);

    auto vix_close = spy_df["IDX:VIX:c"];
    auto vix_array = vix_close.contiguous_array();
    auto vix_non_null = vix_array.length() - vix_array.null_count();
    REQUIRE(vix_non_null > 0);

    std::cout << "SPY DataFrame has " << spy_df.num_rows() << " rows and "
              << spy_df.num_cols() << " columns (including indices)\n";
    std::cout << "SPX non-null: " << spx_non_null << "/" << spx_array.length() << "\n";
    std::cout << "VIX non-null: " << vix_non_null << "/" << vix_array.length() << "\n";
    std::cout << "Column names: ";
    for (const auto& col : spy_df.column_names()) {
      std::cout << col << " ";
    }
    std::cout << "\n";
  }

  SECTION("Load multiple assets with indices") {
    DataloaderOption opt;
    opt.startDate = epoch_frame::DateTime::from_date_str("2024-01-01").date();
    opt.endDate = epoch_frame::DateTime::from_date_str("2024-01-15").date();
    opt.AddRequest(DataCategory::DailyBars);
    opt.dataloaderAssets = {
        asset::AssetConstants::instance().SPY,
        asset::AssetConstants::instance().QQQ
    };
    opt.cacheDir = "/tmp/epoch_test_cache";
    opt.enableCache = true;

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

    REQUIRE(spy_df.contains("IDX:SPX:c"));
    REQUIRE(spy_df.contains("IDX:VIX:c"));
    REQUIRE(qqq_df.contains("IDX:SPX:c"));
    REQUIRE(qqq_df.contains("IDX:VIX:c"));

    // Verify indices data is not all null for SPY
    auto spy_spx_close = spy_df["IDX:SPX:c"];
    auto spy_spx_array = spy_spx_close.contiguous_array();
    REQUIRE((spy_spx_array.length() - spy_spx_array.null_count()) > 0);

    // Verify indices data is not all null for QQQ
    auto qqq_spx_close = qqq_df["IDX:SPX:c"];
    auto qqq_spx_array = qqq_spx_close.contiguous_array();
    REQUIRE((qqq_spx_array.length() - qqq_spx_array.null_count()) > 0);

    std::cout << "SPY: " << spy_df.num_rows() << " rows, " << spy_df.num_cols() << " cols\n";
    std::cout << "QQQ: " << qqq_df.num_rows() << " rows, " << qqq_df.num_cols() << " cols\n";
  }

  SECTION("Load SPY with market indices (intraday)") {
    // Setup dataloader option for minute bars
    DataloaderOption opt;
    opt.startDate = epoch_frame::DateTime::from_date_str("2024-01-02").date();
    opt.endDate = epoch_frame::DateTime::from_date_str("2024-01-02").date();  // Single day for speed
    opt.AddRequest(DataCategory::MinuteBars);
    opt.dataloaderAssets = {asset::AssetConstants::instance().SPY};
    opt.cacheDir = "/tmp/epoch_test_cache";
    opt.enableCache = true;

    // Add market indices
    opt.AddIndexTicker("SPX");
    opt.AddIndexTicker("VIX");

    std::cout << "Creating dataloader with " << opt.GetIndicesTickers().size()
              << " market indices (INTRADAY)\n";

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
    REQUIRE(spy_df.contains("IDX:SPX:c"));
    REQUIRE(spy_df.contains("IDX:VIX:c"));

    // Verify all OHLC columns for indices
    REQUIRE(spy_df.contains("IDX:SPX:o"));
    REQUIRE(spy_df.contains("IDX:SPX:h"));
    REQUIRE(spy_df.contains("IDX:SPX:l"));

    // Verify indices data is not all null (intraday)
    auto spx_close_intra = spy_df["IDX:SPX:c"];
    auto spx_array_intra = spx_close_intra.contiguous_array();
    REQUIRE((spx_array_intra.length() - spx_array_intra.null_count()) > 0);

    auto vix_close_intra = spy_df["IDX:VIX:c"];
    auto vix_array_intra = vix_close_intra.contiguous_array();
    REQUIRE((vix_array_intra.length() - vix_array_intra.null_count()) > 0);

    // Minute data should have significantly more rows than daily
    std::cout << "SPY intraday DataFrame has " << spy_df.num_rows() << " rows and "
              << spy_df.num_cols() << " columns (including indices)\n";

    // Verify we have intraday data (should have many rows for a single day)
    REQUIRE(spy_df.num_rows() > 30);  // At least 30 minutes of data
  }

  SECTION("Load multiple assets with indices (intraday)") {
    DataloaderOption opt;
    opt.startDate = epoch_frame::DateTime::from_date_str("2024-01-02").date();
    opt.endDate = epoch_frame::DateTime::from_date_str("2024-01-02").date();  // Single day for speed
    opt.AddRequest(DataCategory::MinuteBars);
    opt.dataloaderAssets = {
        asset::AssetConstants::instance().SPY,
        asset::AssetConstants::instance().QQQ
    };
    opt.cacheDir = "/tmp/epoch_test_cache";
    opt.enableCache = true;

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

    // Verify indices columns present in both
    REQUIRE(spy_df.contains("IDX:SPX:c"));
    REQUIRE(spy_df.contains("IDX:VIX:c"));
    REQUIRE(qqq_df.contains("IDX:SPX:c"));
    REQUIRE(qqq_df.contains("IDX:VIX:c"));

    // Verify indices data is not all null for SPY (intraday)
    auto spy_spx_intra = spy_df["IDX:SPX:c"];
    auto spy_spx_intra_array = spy_spx_intra.contiguous_array();
    REQUIRE((spy_spx_intra_array.length() - spy_spx_intra_array.null_count()) > 0);

    // Verify indices data is not all null for QQQ (intraday)
    auto qqq_spx_intra = qqq_df["IDX:SPX:c"];
    auto qqq_spx_intra_array = qqq_spx_intra.contiguous_array();
    REQUIRE((qqq_spx_intra_array.length() - qqq_spx_intra_array.null_count()) > 0);

    // Both should have intraday data
    REQUIRE(spy_df.num_rows() > 30);
    REQUIRE(qqq_df.num_rows() > 30);

    std::cout << "Intraday - SPY: " << spy_df.num_rows() << " rows, " << spy_df.num_cols() << " cols\n";
    std::cout << "Intraday - QQQ: " << qqq_df.num_rows() << " rows, " << qqq_df.num_cols() << " cols\n";
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
    opt.AddRequest(DataCategory::DailyBars);
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

    // Debug: print actual columns
    std::cout << "Direct SPX load: " << df.num_rows() << " rows\n";
    std::cout << "Columns: ";
    for (const auto& col : df.column_names()) {
      std::cout << col << " ";
    }
    std::cout << "\n";

    REQUIRE(df.contains("o"));
    REQUIRE(df.contains("c"));
  }

  SECTION("Load index data directly (async)") {
    DataloaderOption opt;
    opt.startDate = epoch_frame::DateTime::from_date_str("2024-01-01").date();
    opt.endDate = epoch_frame::DateTime::from_date_str("2024-01-31").date();
    opt.AddRequest(DataCategory::DailyBars);
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
