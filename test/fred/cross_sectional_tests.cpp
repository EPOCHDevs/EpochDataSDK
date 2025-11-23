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
#include "../../src/dataloader/fred_cross_sectional_fetcher.hpp"
#include "../../src/fred/cross_sectional_series_map.hpp"

using namespace data_sdk;
using namespace data_sdk::dataloader;
using namespace data_sdk::fred;
using namespace data_sdk::common;

static std::string getApiKey() {
  const char *env = std::getenv("FRED_API_KEY");
  return env ? std::string(env) : "";
}

static bool hasApiKey() {
  return !getApiKey().empty();
}

TEST_CASE("CrossSectionalDataCategory - Series mapping", "[fred][cross_sectional][mapping]") {
  SECTION("All categories have valid series IDs") {
    // Test a sample of categories
    REQUIRE(getSeriesId(CrossSectionalDataCategory::CPI) == "CPIAUCSL");
    REQUIRE(getSeriesId(CrossSectionalDataCategory::CoreCPI) == "CPILFESL");
    REQUIRE(getSeriesId(CrossSectionalDataCategory::FedFunds) == "DFF");
    REQUIRE(getSeriesId(CrossSectionalDataCategory::GDP) == "GDPC1");
    REQUIRE(getSeriesId(CrossSectionalDataCategory::Unemployment) == "UNRATE");
    REQUIRE(getSeriesId(CrossSectionalDataCategory::Treasury10Y) == "DGS10");
    REQUIRE(getSeriesId(CrossSectionalDataCategory::VIX) == "VIXCLS");
  }

  SECTION("Category names are correct") {
    REQUIRE(getCategoryName(CrossSectionalDataCategory::CPI) == "CPI");
    REQUIRE(getCategoryName(CrossSectionalDataCategory::FedFunds) == "FedFunds");
    REQUIRE(getCategoryName(CrossSectionalDataCategory::GDP) == "GDP");
  }
}

TEST_CASE("FredCrossSectionalFetcher - Basic functionality", "[fred][cross_sectional][fetcher]") {
  if (!hasApiKey()) {
    SKIP("FRED_API_KEY not set");
  }

  FredCrossSectionalFetcher fetcher;

  SECTION("Fetch CPI data (sync)") {
    auto from = epoch_frame::DateTime::from_date_str("2023-01-01").date();
    auto to = epoch_frame::DateTime::from_date_str("2023-12-31").date();

    auto result = fetcher.Fetch(CrossSectionalDataCategory::CPI, from, to);

    REQUIRE(result.has_value());
    const auto& df = *result;

    // Verify schema
    REQUIRE(df.contains("observation_date"));
    REQUIRE(df.contains("value"));
    REQUIRE(df.contains("revision"));

    // Verify we have data
    REQUIRE(df.num_rows() > 0);

    std::cout << "CPI data: " << df.num_rows() << " rows\n";
  }

  SECTION("Fetch Federal Funds Rate (sync)") {
    auto from = epoch_frame::DateTime::from_date_str("2024-01-01").date();
    auto to = epoch_frame::DateTime::from_date_str("2024-03-31").date();

    auto result = fetcher.Fetch(CrossSectionalDataCategory::FedFunds, from, to);

    REQUIRE(result.has_value());
    REQUIRE(result->num_rows() > 0);

    std::cout << "Fed Funds data: " << result->num_rows() << " rows\n";
  }

  SECTION("Fetch GDP data (sync)") {
    auto from = epoch_frame::DateTime::from_date_str("2020-01-01").date();
    auto to = epoch_frame::DateTime::from_date_str("2024-01-01").date();

    auto result = fetcher.Fetch(CrossSectionalDataCategory::GDP, from, to);

    REQUIRE(result.has_value());
    REQUIRE(result->num_rows() > 0);

    std::cout << "GDP data: " << result->num_rows() << " rows\n";
  }
}

TEST_CASE("FredCrossSectionalFetcher - Async functionality", "[fred][cross_sectional][fetcher][async]") {
  if (!hasApiKey()) {
    SKIP("FRED_API_KEY not set");
  }

  FredCrossSectionalFetcher fetcher;

  SECTION("Fetch Unemployment data (async)") {
    auto from = epoch_frame::DateTime::from_date_str("2023-01-01").date();
    auto to = epoch_frame::DateTime::from_date_str("2023-12-31").date();

    auto task = [&]() -> drogon::Task<void> {
      auto result = co_await fetcher.FetchAsync(CrossSectionalDataCategory::Unemployment, from, to);

      REQUIRE(result.has_value());
      const auto& df = *result;

      REQUIRE(df.contains("observation_date"));
      REQUIRE(df.contains("value"));
      REQUIRE(df.contains("revision"));
      REQUIRE(df.num_rows() > 0);

      std::cout << "Unemployment data (async): " << df.num_rows() << " rows\n";
      co_return;
    };

    drogon::sync_wait(task());
  }

  SECTION("Fetch 10Y Treasury data (async)") {
    auto from = epoch_frame::DateTime::from_date_str("2024-01-01").date();
    auto to = epoch_frame::DateTime::from_date_str("2024-06-30").date();

    auto task = [&]() -> drogon::Task<void> {
      auto result = co_await fetcher.FetchAsync(CrossSectionalDataCategory::Treasury10Y, from, to);

      REQUIRE(result.has_value());
      REQUIRE(result->num_rows() > 0);

      std::cout << "Treasury 10Y data (async): " << result->num_rows() << " rows\n";
      co_return;
    };

    drogon::sync_wait(task());
  }
}

TEST_CASE("FredCrossSectionalFetcher - Multiple indicators", "[fred][cross_sectional][fetcher][batch]") {
  if (!hasApiKey()) {
    SKIP("FRED_API_KEY not set");
  }

  FredCrossSectionalFetcher fetcher;
  auto from = epoch_frame::DateTime::from_date_str("2023-01-01").date();
  auto to = epoch_frame::DateTime::from_date_str("2023-12-31").date();

  SECTION("Fetch multiple indicators in parallel") {
    auto task = [&]() -> drogon::Task<void> {
      // Fetch multiple indicators concurrently
      std::vector<drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>> tasks;
      tasks.push_back(fetcher.FetchAsync(CrossSectionalDataCategory::CPI, from, to));
      tasks.push_back(fetcher.FetchAsync(CrossSectionalDataCategory::GDP, from, to));
      tasks.push_back(fetcher.FetchAsync(CrossSectionalDataCategory::Unemployment, from, to));
      tasks.push_back(fetcher.FetchAsync(CrossSectionalDataCategory::FedFunds, from, to));

      auto results = co_await when_all(std::move(tasks));

      REQUIRE(results.size() == 4);
      REQUIRE(results[0].has_value());  // CPI
      REQUIRE(results[1].has_value());  // GDP
      REQUIRE(results[2].has_value());  // Unemployment
      REQUIRE(results[3].has_value());  // FedFunds

      std::cout << "Parallel fetch results:\n";
      std::cout << "  CPI: " << results[0]->num_rows() << " rows\n";
      std::cout << "  GDP: " << results[1]->num_rows() << " rows\n";
      std::cout << "  Unemployment: " << results[2]->num_rows() << " rows\n";
      std::cout << "  Fed Funds: " << results[3]->num_rows() << " rows\n";
      co_return;
    };

    drogon::sync_wait(task());
  }
}

TEST_CASE("CrossSectional - Metadata", "[fred][cross_sectional][metadata]") {
  SECTION("Get metadata for cross-sectional categories") {
    auto metadata = MetadataRegistry::GetCrossSectionalMetadata(CrossSectionalDataCategory::CPI);

    REQUIRE(metadata.data_type == "economic_indicator");
    REQUIRE(metadata.description.find("CPI") != std::string::npos);

    // Verify schema (same as ALFRED)
    REQUIRE(metadata.columns.size() == 3);

    // Find columns by id
    bool has_observation_date = false;
    bool has_value = false;
    bool has_revision = false;

    for (const auto& col : metadata.columns) {
      if (col.id == "observation_date") has_observation_date = true;
      if (col.id == "value") has_value = true;
      if (col.id == "revision") has_revision = true;
    }

    REQUIRE(has_observation_date);
    REQUIRE(has_value);
    REQUIRE(has_revision);
  }

  SECTION("Metadata for different categories") {
    auto cpi_meta = MetadataRegistry::GetCrossSectionalMetadata(CrossSectionalDataCategory::CPI);
    auto gdp_meta = MetadataRegistry::GetCrossSectionalMetadata(CrossSectionalDataCategory::GDP);

    // Should have same schema but different descriptions
    REQUIRE(cpi_meta.columns.size() == gdp_meta.columns.size());
    REQUIRE(cpi_meta.description != gdp_meta.description);
    REQUIRE(cpi_meta.description.find("CPI") != std::string::npos);
    REQUIRE(gdp_meta.description.find("GDP") != std::string::npos);
  }
}

TEST_CASE("CrossSectional - All indicators have correct mappings", "[fred][cross_sectional][comprehensive]") {
  // Verify all economic indicators are properly mapped
  std::vector<std::pair<CrossSectionalDataCategory, std::string>> expected_mappings = {
      // Inflation
      {CrossSectionalDataCategory::CPI, "CPIAUCSL"},
      {CrossSectionalDataCategory::CoreCPI, "CPILFESL"},
      {CrossSectionalDataCategory::PCE, "PCEPI"},
      {CrossSectionalDataCategory::CorePCE, "PCEPILFE"},

      // Interest Rates
      {CrossSectionalDataCategory::FedFunds, "DFF"},
      {CrossSectionalDataCategory::Treasury3M, "DTB3"},
      {CrossSectionalDataCategory::Treasury2Y, "DGS2"},
      {CrossSectionalDataCategory::Treasury5Y, "DGS5"},
      {CrossSectionalDataCategory::Treasury10Y, "DGS10"},
      {CrossSectionalDataCategory::Treasury30Y, "DGS30"},

      // Employment
      {CrossSectionalDataCategory::Unemployment, "UNRATE"},
      {CrossSectionalDataCategory::NonfarmPayrolls, "PAYEMS"},
      {CrossSectionalDataCategory::InitialClaims, "ICSA"},

      // Economic Growth
      {CrossSectionalDataCategory::GDP, "GDPC1"},
      {CrossSectionalDataCategory::IndustrialProduction, "INDPRO"},
      {CrossSectionalDataCategory::RetailSales, "RSXFS"},
      {CrossSectionalDataCategory::HousingStarts, "HOUST"},

      // Market Sentiment
      {CrossSectionalDataCategory::ConsumerSentiment, "UMCSENT"},
      {CrossSectionalDataCategory::M2, "M2SL"},
      {CrossSectionalDataCategory::SP500, "SP500"},
      {CrossSectionalDataCategory::VIX, "VIXCLS"}
  };

  for (const auto& [category, expected_id] : expected_mappings) {
    REQUIRE(getSeriesId(category) == expected_id);
  }
}

TEST_CASE("DataLoader Integration - Load assets with economic indicators", "[fred][cross_sectional][integration][dataloader]") {
  if (!hasApiKey()) {
    SKIP("FRED_API_KEY not set");
  }

  // Skip if POLYGON_API_KEY not set (needed for asset data)
  const char* polygon_key = std::getenv("POLYGON_API_KEY");
  if (!polygon_key || std::string(polygon_key).empty()) {
    SKIP("POLYGON_API_KEY not set");
  }

  SECTION("Load SPY with economic indicators") {
    // Setup dataloader option
    DataloaderOption opt;
    opt.startDate = epoch_frame::DateTime::from_date_str("2023-01-01").date();
    opt.endDate = epoch_frame::DateTime::from_date_str("2023-03-31").date();
    opt.categories = {DataCategory::DailyBars};
    opt.dataloaderAssets = {asset::AssetConstants::instance().SPY};
    opt.cacheDir = "/tmp/epoch_test_cache";  // Provide cache directory
    opt.enableCache = true;  // Enable cache

    // Add cross-sectional economic indicators
    opt.AddCrossSectionalCategory(CrossSectionalDataCategory::CPI);
    opt.AddCrossSectionalCategory(CrossSectionalDataCategory::FedFunds);
    opt.AddCrossSectionalCategory(CrossSectionalDataCategory::Unemployment);

    std::cout << "Creating dataloader with " << opt.GetCrossSectionalCategories().size()
              << " economic indicators\n";

    // Create dataloader
    auto dataloader = CreateApiCacheDataLoader(opt);
    REQUIRE(dataloader != nullptr);

    // Load data
    dataloader->LoadData();

    // Get loaded data
    auto data = dataloader->GetStoredData();
    REQUIRE(data.size() > 0);

    // Get SPY DataFrame
    auto spy = asset::AssetConstants::instance().SPY;
    REQUIRE(data.contains(spy));

    const auto& spy_df = data.at(spy);
    std::cout << "SPY DataFrame has " << spy_df.num_rows() << " rows and "
              << spy_df.num_cols() << " columns\n";

    // Verify regular OHLCV columns exist (lowercase from Polygon API)
    REQUIRE(spy_df.contains("o"));
    REQUIRE(spy_df.contains("h"));
    REQUIRE(spy_df.contains("l"));
    REQUIRE(spy_df.contains("c"));
    REQUIRE(spy_df.contains("v"));
    REQUIRE(spy_df.contains("vw"));  // volume-weighted average price
    REQUIRE(spy_df.contains("n"));   // number of trades

    // Verify economic indicator columns were merged
    REQUIRE(spy_df.contains("ECON:CPI:observation_date"));
    REQUIRE(spy_df.contains("ECON:CPI:value"));
    REQUIRE(spy_df.contains("ECON:CPI:revision"));

    REQUIRE(spy_df.contains("ECON:FedFunds:observation_date"));
    REQUIRE(spy_df.contains("ECON:FedFunds:value"));
    REQUIRE(spy_df.contains("ECON:FedFunds:revision"));

    REQUIRE(spy_df.contains("ECON:Unemployment:observation_date"));
    REQUIRE(spy_df.contains("ECON:Unemployment:value"));
    REQUIRE(spy_df.contains("ECON:Unemployment:revision"));

    // Verify economic data has non-null values (not all nulls)
    auto cpi_value = spy_df["ECON:CPI:value"];
    auto cpi_array = cpi_value.contiguous_array();
    auto cpi_non_null_count = cpi_array.length() - cpi_array.null_count();
    REQUIRE(cpi_non_null_count > 0);  // CPI should have some non-null values

    auto fed_funds_value = spy_df["ECON:FedFunds:value"];
    auto fed_funds_array = fed_funds_value.contiguous_array();
    auto fed_funds_non_null_count = fed_funds_array.length() - fed_funds_array.null_count();
    REQUIRE(fed_funds_non_null_count > 0);  // FedFunds should have some non-null values

    std::cout << "Successfully verified economic columns in SPY DataFrame\n";
    std::cout << "CPI non-null values: " << cpi_non_null_count << "/" << cpi_array.length() << "\n";
    std::cout << "FedFunds non-null values: " << fed_funds_non_null_count << "/" << fed_funds_array.length() << "\n";
    std::cout << "Column names: ";
    for (const auto& col : spy_df.column_names()) {
      if (col.find("ECON:") == 0) {
        std::cout << col << ", ";
      }
    }
    std::cout << "\n";
  }

  SECTION("Load multiple assets with GDP and Treasury10Y") {
    DataloaderOption opt;
    opt.startDate = epoch_frame::DateTime::from_date_str("2023-06-01").date();
    opt.endDate = epoch_frame::DateTime::from_date_str("2023-06-30").date();
    opt.categories = {DataCategory::DailyBars};
    opt.dataloaderAssets = {
        asset::AssetConstants::instance().SPY,
        asset::AssetConstants::instance().QQQ
    };
    opt.cacheDir = "/tmp/epoch_test_cache";  // Provide cache directory
    opt.enableCache = true;  // Enable cache

    // Add fewer indicators for faster test
    opt.AddCrossSectionalCategory(CrossSectionalDataCategory::GDP);
    opt.AddCrossSectionalCategory(CrossSectionalDataCategory::Treasury10Y);

    auto dataloader = CreateApiCacheDataLoader(opt);
    dataloader->LoadData();

    auto data = dataloader->GetStoredData();

    // Both assets should be loaded
    REQUIRE(data.size() >= 1);  // At least one should succeed

    // Check each loaded asset has economic columns
    for (const auto& [asset, df] : data) {
      std::cout << "Checking " << asset.GetSymbolStr() << " - "
                << df.num_rows() << " rows, " << df.num_cols() << " columns\n";

      // Should have price data (lowercase from Polygon API)
      REQUIRE(df.contains("c"));

      // Should have economic data
      REQUIRE(df.contains("ECON:GDP:observation_date"));
      REQUIRE(df.contains("ECON:GDP:value"));
      REQUIRE(df.contains("ECON:Treasury10Y:observation_date"));
      REQUIRE(df.contains("ECON:Treasury10Y:value"));
    }

    std::cout << "Successfully loaded " << data.size() << " assets with economic indicators\n";
  }

  SECTION("Load SPY with economic indicators (minute bars)") {
    // Test that economic data (daily frequency) merges correctly with minute-level data
    DataloaderOption opt;
    opt.startDate = epoch_frame::DateTime::from_date_str("2024-01-02").date();
    opt.endDate = epoch_frame::DateTime::from_date_str("2024-01-02").date();  // Single day
    opt.categories = {DataCategory::MinuteBars};
    opt.dataloaderAssets = {asset::AssetConstants::instance().SPY};
    opt.cacheDir = "/tmp/epoch_test_cache_minute";  // Use different cache dir for minute bars
    opt.enableCache = true;  // Enable cache

    // Add cross-sectional economic indicators (daily frequency)
    opt.AddCrossSectionalCategory(CrossSectionalDataCategory::CPI);
    opt.AddCrossSectionalCategory(CrossSectionalDataCategory::FedFunds);
    opt.AddCrossSectionalCategory(CrossSectionalDataCategory::Unemployment);

    std::cout << "Creating dataloader with minute bars and "
              << opt.GetCrossSectionalCategories().size()
              << " economic indicators\\n";

    // Create dataloader
    auto dataloader = CreateApiCacheDataLoader(opt);
    REQUIRE(dataloader != nullptr);

    // Load data
    dataloader->LoadData();

    // Get loaded data
    auto data = dataloader->GetStoredData();
    REQUIRE(data.size() > 0);

    // Get SPY DataFrame
    auto spy = asset::AssetConstants::instance().SPY;
    REQUIRE(data.contains(spy));

    const auto& spy_df = data.at(spy);
    std::cout << "SPY MinuteBars DataFrame has " << spy_df.num_rows() << " rows and "
              << spy_df.num_cols() << " columns\\n";

    // Debug: print column names
    std::cout << "Columns: ";
    for (const auto& col : spy_df.column_names()) {
      std::cout << col << ", ";
    }
    std::cout << "\\n";

    // Verify we have minute-level data (should be many rows for a trading day)
    REQUIRE(spy_df.num_rows() > 100);  // At least 100 minute bars

    // Verify regular OHLCV columns exist (lowercase from Polygon API)
    REQUIRE(spy_df.contains("o"));
    REQUIRE(spy_df.contains("h"));
    REQUIRE(spy_df.contains("l"));
    REQUIRE(spy_df.contains("c"));
    REQUIRE(spy_df.contains("v"));
    REQUIRE(spy_df.contains("vw"));  // volume-weighted average price
    REQUIRE(spy_df.contains("n"));   // number of trades

    // Verify economic indicator columns were merged (daily data merged into minute data)
    REQUIRE(spy_df.contains("ECON:CPI:observation_date"));
    REQUIRE(spy_df.contains("ECON:CPI:value"));
    REQUIRE(spy_df.contains("ECON:CPI:revision"));

    REQUIRE(spy_df.contains("ECON:FedFunds:observation_date"));
    REQUIRE(spy_df.contains("ECON:FedFunds:value"));
    REQUIRE(spy_df.contains("ECON:FedFunds:revision"));

    REQUIRE(spy_df.contains("ECON:Unemployment:observation_date"));
    REQUIRE(spy_df.contains("ECON:Unemployment:value"));
    REQUIRE(spy_df.contains("ECON:Unemployment:revision"));

    std::cout << "Successfully verified economic columns in SPY MinuteBars DataFrame\\n";
    std::cout << "Economic data (daily frequency) correctly merged with minute-level bars\\n";
  }
}
