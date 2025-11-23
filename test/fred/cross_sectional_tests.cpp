#include <catch2/catch_test_macros.hpp>
#include <cstdlib>
#include <iostream>

#include <epoch_data_sdk/common/enums.hpp>
#include <epoch_frame/datetime.h>
#include <epoch_data_sdk/dataloader/metadata_registry.hpp>
#include "../../src/dataloader/fred_cross_sectional_fetcher.hpp"
#include "../../src/fred/cross_sectional_series_map.hpp"

using namespace data_sdk;
using namespace data_sdk::dataloader;
using namespace data_sdk::fred;

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
    auto from = epoch_frame::Date(2020, 1, 1);
    auto to = epoch_frame::Date(2024, 1, 1);

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
    auto from = epoch_frame::Date(2023, 1, 1);
    auto to = epoch_frame::Date(2023, 12, 31);

    auto task = [&]() -> drogon::Task<void> {
      auto result = co_await fetcher.FetchAsync(CrossSectionalDataCategory::Unemployment, from, to);

      REQUIRE(result.has_value());
      const auto& df = *result;

      REQUIRE(df.contains("observation_date"));
      REQUIRE(df.contains("value"));
      REQUIRE(df.contains("revision"));
      REQUIRE(df.num_rows() > 0);

      std::cout << "Unemployment data (async): " << df.num_rows() << " rows\n";
    };

    drogon::sync_wait(task());
  }

  SECTION("Fetch 10Y Treasury data (async)") {
    auto from = epoch_frame::Date(2024, 1, 1);
    auto to = epoch_frame::Date(2024, 6, 30);

    auto task = [&]() -> drogon::Task<void> {
      auto result = co_await fetcher.FetchAsync(CrossSectionalDataCategory::Treasury10Y, from, to);

      REQUIRE(result.has_value());
      REQUIRE(result->num_rows() > 0);

      std::cout << "Treasury 10Y data (async): " << result->num_rows() << " rows\n";
    };

    drogon::sync_wait(task());
  }
}

TEST_CASE("FredCrossSectionalFetcher - Multiple indicators", "[fred][cross_sectional][fetcher][batch]") {
  if (!hasApiKey()) {
    SKIP("FRED_API_KEY not set");
  }

  FredCrossSectionalFetcher fetcher;
  auto from = epoch_frame::Date(2023, 1, 1);
  auto to = epoch_frame::Date(2023, 12, 31);

  SECTION("Fetch multiple indicators in parallel") {
    auto task = [&]() -> drogon::Task<void> {
      // Fetch multiple indicators concurrently
      auto cpi_task = fetcher.FetchAsync(CrossSectionalDataCategory::CPI, from, to);
      auto gdp_task = fetcher.FetchAsync(CrossSectionalDataCategory::GDP, from, to);
      auto unemployment_task = fetcher.FetchAsync(CrossSectionalDataCategory::Unemployment, from, to);
      auto fedfunds_task = fetcher.FetchAsync(CrossSectionalDataCategory::FedFunds, from, to);

      auto [cpi, gdp, unemployment, fedfunds] = co_await drogon::when_all(
          std::move(cpi_task),
          std::move(gdp_task),
          std::move(unemployment_task),
          std::move(fedfunds_task)
      );

      REQUIRE(cpi.has_value());
      REQUIRE(gdp.has_value());
      REQUIRE(unemployment.has_value());
      REQUIRE(fedfunds.has_value());

      std::cout << "Parallel fetch results:\n";
      std::cout << "  CPI: " << cpi->num_rows() << " rows\n";
      std::cout << "  GDP: " << gdp->num_rows() << " rows\n";
      std::cout << "  Unemployment: " << unemployment->num_rows() << " rows\n";
      std::cout << "  Fed Funds: " << fedfunds->num_rows() << " rows\n";
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
