#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <cstdlib>
#include <iostream>

#include <epoch_data_sdk/common/async_batch.hpp>
#include "../src/fred/series_client.hpp"

using namespace data_sdk::fred;
using namespace data_sdk::common;

static std::string getApiKey() {
  const char *env = std::getenv("FRED_API_KEY");
  return env ? std::string(env) : "";
}

static Options makeOptions() {
  Options opts;
  opts.api_key = getApiKey();
  return opts;
}

// Helper to check if we have a valid API key
static bool hasApiKey() {
  return !getApiKey().empty();
}


TEST_CASE("FRED SeriesClient - convenience methods", "[fred][series][integration]") {
  if (!hasApiKey()) {
    SKIP("FRED_API_KEY not set");
  }

  SeriesClient client(makeOptions());

  SECTION("getCPI") {
    auto df = client.getCPI("2023-01-01", "2023-03-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);

    // With ALFRED enabled by default, we should have observation_date and value
    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getFedFunds") {
    auto df = client.getFedFunds("2023-01-01", "2023-01-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);

    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getGDP") {
    auto df = client.getGDP("2022-01-01", "2023-12-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);

    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getUnemployment") {
    auto df = client.getUnemployment("2023-01-01", "2023-12-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);

    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getTreasury10Y") {
    auto df = client.getTreasury10Y("2023-01-01", "2023-12-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);

    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getCoreCPI") {
    auto df = client.getCoreCPI("2023-01-01", "2023-03-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);

    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getPCE") {
    auto df = client.getPCE("2023-01-01", "2023-03-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);
    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getCorePCE") {
    auto df = client.getCorePCE("2023-01-01", "2023-03-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);
    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getTreasury2Y") {
    auto df = client.getTreasury2Y("2023-01-01", "2023-12-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);
    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getTreasury5Y") {
    auto df = client.getTreasury5Y("2023-01-01", "2023-12-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);
    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getTreasury30Y") {
    auto df = client.getTreasury30Y("2023-01-01", "2023-12-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);
    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getTreasury3M") {
    auto df = client.getTreasury3M("2023-01-01", "2023-12-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);
    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getInitialClaims") {
    auto df = client.getInitialClaims("2023-01-01", "2023-01-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);
    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getNonfarmPayrolls") {
    auto df = client.getNonfarmPayrolls("2023-01-01", "2023-12-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);
    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getIndustrialProduction") {
    auto df = client.getIndustrialProduction("2023-01-01", "2023-12-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);
    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getConsumerSentiment") {
    auto df = client.getConsumerSentiment("2023-01-01", "2023-12-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);
    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getRetailSales") {
    auto df = client.getRetailSales("2023-01-01", "2023-12-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);
    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getHousingStarts") {
    auto df = client.getHousingStarts("2023-01-01", "2023-12-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);
    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getM2MoneySupply") {
    auto df = client.getM2MoneySupply("2023-01-01", "2023-12-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);
    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getSP500") {
    auto df = client.getSP500("2023-01-01", "2023-12-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);
    // SP500 not available in ALFRED, so only value column
    std::vector<std::string> expected_cols = {"value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getVIX") {
    auto df = client.getVIX("2023-01-01", "2023-12-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);
    // VIX not available in ALFRED, so only value column
    std::vector<std::string> expected_cols = {"value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }
}

TEST_CASE("FRED SeriesClient - error handling", "[fred][series][error]") {
  Options opts;
  opts.api_key = "invalid_key_12345";
  SeriesClient client(std::move(opts));

  SECTION("Invalid API key") {
    auto df = client.getCPI("2023-01-01", "2023-12-31");
    REQUIRE_FALSE(df.has_value());
    REQUIRE(df.error().http_status == 400);
  }
}

TEST_CASE("FRED SeriesClient - async batch requests", "[fred][series][async][integration]") {
  if (!hasApiKey()) {
    SKIP("FRED_API_KEY not set");
  }

  SeriesClient client(makeOptions());

  SECTION("Concurrent async requests for multiple economic indicators") {
    INFO("Creating concurrent async tasks for CPI, Fed Funds, and GDP...");

    // Create 3 concurrent tasks for different economic indicators
    std::vector<drogon::Task<Expected<epoch_frame::DataFrame>>> tasks;
    tasks.push_back(client.getCPIAsync("2023-01-01", "2023-12-31", true));
    tasks.push_back(client.getFedFundsAsync("2023-01-01", "2023-12-31", true));
    tasks.push_back(client.getGDPAsync("2023-01-01", "2023-12-31", true));

    INFO("Executing all 3 tasks concurrently with syncJoinAll()...");

    auto start = std::chrono::steady_clock::now();
    auto results = syncJoinAll(std::move(tasks));
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start).count();

    std::cout << "\n=== FRED Async Test Results ===\n";
    std::cout << "Completed in " << elapsed << "ms\n";

    // Validate all succeeded
    REQUIRE(results.size() == 3);

    INFO("Validating CPI result (index 0)...");
    REQUIRE(results[0].has_value());
    if (!results[0].has_value()) {
      INFO("CPI error: " << results[0].error().message);
    }

    INFO("Validating Fed Funds result (index 1)...");
    REQUIRE(results[1].has_value());
    if (!results[1].has_value()) {
      INFO("Fed Funds error: " << results[1].error().message);
    }

    INFO("Validating GDP result (index 2)...");
    REQUIRE(results[2].has_value());
    if (!results[2].has_value()) {
      INFO("GDP error: " << results[2].error().message);
    }

    // Check that we got meaningful amounts of data
    auto cpi_rows = results[0]->num_rows();
    auto fedfunds_rows = results[1]->num_rows();
    auto gdp_rows = results[2]->num_rows();

    std::cout << "CPI rows: " << cpi_rows << "\n";
    std::cout << "Fed Funds rows: " << fedfunds_rows << "\n";
    std::cout << "GDP rows: " << gdp_rows << "\n";

    // Sanity check - should have at least some data for 2023
    REQUIRE(cpi_rows > 0);
    REQUIRE(fedfunds_rows > 0);
    REQUIRE(gdp_rows > 0);

    // Verify DataFrames have expected columns with ALFRED enabled
    REQUIRE(results[0]->contains("observation_date"));
    REQUIRE(results[0]->contains("value"));
    REQUIRE(results[1]->contains("observation_date"));
    REQUIRE(results[1]->contains("value"));
    REQUIRE(results[2]->contains("observation_date"));
    REQUIRE(results[2]->contains("value"));

    std::cout << "=== All validations passed! ===\n\n";
  }

  SECTION("SeriesOptions struct usage with async") {
    INFO("Testing SeriesOptions struct with async API...");

    SeriesOptions opts{"UNRATE", "2023-01-01", "2023-12-31", true};
    auto result = drogon::sync_wait(client.getSeriesAsync(opts));

    REQUIRE(result.has_value());
    REQUIRE(result->num_rows() > 0);

    std::cout << "SeriesOptions async test: Unemployment data for 2023\n";
    std::cout << "Rows: " << result->num_rows() << "\n";
  }
}
