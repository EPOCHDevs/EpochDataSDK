#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <unordered_map>

#include <epoch_data_sdk/common/async_batch.hpp>
#include "../common/test_utils.hpp"
#include "../src/fred/series_client.hpp"

using namespace data_sdk::fred;
using namespace data_sdk::common;
using namespace data_sdk;
using namespace data_sdk::test;

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

    REQUIRE(df->index()->dtype()->ToString() ==
        arrow::timestamp(arrow::TimeUnit::NANO, "UTC")->ToString());
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

TEST_CASE("Real API: FRED Series metadata verification", "[fred][real_api][metadata]") {
  if (!hasApiKey()) {
    SKIP("FRED_API_KEY not set");
  }

  Options opt;
  opt.api_key = getApiKey();

  SeriesClient client(opt);

  SECTION("CPI Series with ALFRED mode") {
    // Get CPI data with ALFRED mode (default)
    auto result = client.getCPI("2023-01-01", "2023-12-31", true);
    REQUIRE(result.has_value());

    std::cout << "FRED CPI (ALFRED mode): Got " << result->num_rows() << " rows\n";

    // Get metadata
    auto metadata = SeriesClient::getMetadata();

    // Verify DataFrame matches metadata
    validateDataFrameAgainstMetadata(*result, metadata, "SeriesClient(CPI)");

    // Verify ALFRED-specific structure
    REQUIRE(result->contains("observation_date"));
    REQUIRE(result->contains("value"));

    std::cout << "FRED CPI metadata verification passed!\n";
  }

  SECTION("SP500 Series without ALFRED mode") {
    // Get SP500 data without ALFRED mode (not available for this series)
    auto result = client.getSP500("2023-01-01", "2023-12-31", false);
    REQUIRE(result.has_value());

    std::cout << "FRED SP500 (non-ALFRED mode): Got " << result->num_rows() << " rows\n";

    // Without ALFRED, only the value column should be present (no observation_date)
    REQUIRE(result->contains("value"));
    REQUIRE_FALSE(result->contains("observation_date"));

    std::cout << "FRED SP500 metadata verification passed!\n";
  }
}

TEST_CASE("Real API: FRED Batch historical series 2004-2024",
          "[fred][real_api][long_range]") {
  if (!hasApiKey()) {
    SKIP("FRED_API_KEY not set");
  }

  Options opt;
  opt.api_key = getApiKey();
  opt.request_timeout_sec = 60.0;  // Increase timeout for long requests with pagination

  SeriesClient client(opt);

  INFO("Creating 3 concurrent tasks for 20 years of economic data (2004-2024)...");

  // Create 3 concurrent tasks for 20 years of economic data (2004-2024)
  // Note: FRED uses offset/limit pagination (max 100,000 per page)
  // Daily series like Fed Funds can have >200k observations over 20 years
  // The client now automatically handles pagination to fetch all data
  // This will test:
  // - Concurrent execution of multiple requests
  // - ALFRED revision tracking over long periods
  // - Automatic pagination handling
  // - Coroutine parameter lifetime correctness
  // - Order preservation in results
  std::vector<drogon::Task<Expected<epoch_frame::DataFrame>>> tasks;
  tasks.push_back(client.getCPIAsync("2004-01-01", "2024-12-31", true));      // CPI with ALFRED
  tasks.push_back(client.getFedFundsAsync("2004-01-01", "2024-12-31", true)); // Fed Funds with ALFRED
  tasks.push_back(client.getUnemploymentAsync("2004-01-01", "2024-12-31", true)); // Unemployment with ALFRED

  INFO("Executing all 3 tasks concurrently with syncJoinAll()...");

  // Measure execution time
  auto start = std::chrono::steady_clock::now();

  // Execute all concurrently
  auto results = syncJoinAll(std::move(tasks));

  auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
    std::chrono::steady_clock::now() - start).count();

  std::cout << "\n=== Real API Test Results (FRED 20-year data 2004-2024) ===\n";
  std::cout << "Completed in " << elapsed << " seconds\n";

  // Validate all succeeded
  REQUIRE(results.size() == 3);

  INFO("Validating CPI result (index 0)...");
  if (!results[0].has_value()) {
    std::cout << "CPI error: " << results[0].error().message << "\n";
    std::cout << "CPI HTTP status: " << results[0].error().http_status << "\n";
    WARN("CPI error: " << results[0].error().message);
  }
  REQUIRE(results[0].has_value());

  INFO("Validating Fed Funds result (index 1)...");
  if (!results[1].has_value()) {
    std::cout << "Fed Funds error: " << results[1].error().message << "\n";
    std::cout << "Fed Funds HTTP status: " << results[1].error().http_status << "\n";
    WARN("Fed Funds error: " << results[1].error().message);
  }
  REQUIRE(results[1].has_value());

  INFO("Validating Unemployment result (index 2)...");
  if (!results[2].has_value()) {
    std::cout << "Unemployment error: " << results[2].error().message << "\n";
    std::cout << "Unemployment HTTP status: " << results[2].error().http_status << "\n";
    WARN("Unemployment error: " << results[2].error().message);
  }
  REQUIRE(results[2].has_value());

  // Check that we got meaningful amounts of data
  auto cpi_rows = results[0]->num_rows();
  auto fedfunds_rows = results[1]->num_rows();
  auto unemployment_rows = results[2]->num_rows();

  std::cout << "CPI rows: " << cpi_rows << "\n";
  std::cout << "Fed Funds rows: " << fedfunds_rows << "\n";
  std::cout << "Unemployment rows: " << unemployment_rows << "\n";
  std::cout << "(Use SPDLOG_LEVEL=debug to see pagination details)\n\n";

  // Debug: Investigate DataFrame structure and check for index issues
  if (results[0].has_value() && cpi_rows > 0) {
    auto cpi_table = results[0]->table();
    auto cpi_schema = cpi_table->schema();

    std::cout << "\n=== CPI DataFrame Structure ===\n";
    std::cout << "Columns: " << cpi_schema->num_fields() << "\n";
    for (int i = 0; i < cpi_schema->num_fields(); i++) {
      auto field = cpi_schema->field(i);
      std::cout << "  [" << i << "]: " << field->name()
                << " (type: " << field->type()->ToString() << ")\n";
    }

    // Sample first 10 rows to check for duplicates
    std::cout << "\nFirst 10 rows:\n";
    auto obs_date_col = cpi_table->GetColumnByName("observation_date");
    auto value_col = cpi_table->GetColumnByName("value");

    if (obs_date_col && value_col) {
      auto obs_array = std::static_pointer_cast<arrow::StringArray>(obs_date_col->chunk(0));
      auto val_array = std::static_pointer_cast<arrow::DoubleArray>(value_col->chunk(0));

      for (int i = 0; i < std::min(10, static_cast<int>(obs_array->length())); i++) {
        std::cout << "  Row " << i << ": obs_date=" << obs_array->GetString(i)
                  << ", value=" << val_array->Value(i) << "\n";
      }

      // Check for duplicate observation dates
      std::unordered_map<std::string, int> obs_date_counts;
      for (int64_t i = 0; i < obs_array->length(); i++) {
        obs_date_counts[obs_array->GetString(i)]++;
      }

      int duplicates = 0;
      for (const auto& [date, count] : obs_date_counts) {
        if (count > 1) {
          duplicates++;
        }
      }
      std::cout << "\nUnique observation_dates: " << obs_date_counts.size()
                << " out of " << obs_array->length() << " rows\n";
      std::cout << "Observation dates with duplicates: " << duplicates << "\n";
      if (duplicates > 0) {
        std::cout << "⚠️  ALFRED mode creates multiple rows per observation_date (one per revision)\n";
        std::cout << "   This is expected behavior - use the index (published_at) to distinguish revisions\n";
      }
    }
  }

  if (results[1].has_value() && fedfunds_rows > 0) {
    auto ff_table = results[1]->table();
    std::cout << "\n=== Fed Funds DataFrame Structure ===\n";
    std::cout << "Columns: " << ff_table->schema()->num_fields() << "\n";
    std::cout << "Rows: " << ff_table->num_rows() << "\n";

    // Just show first 5 rows for Fed Funds (less verbose)
    auto obs_date_col = ff_table->GetColumnByName("observation_date");
    if (obs_date_col) {
      auto obs_array = std::static_pointer_cast<arrow::StringArray>(obs_date_col->chunk(0));
      std::cout << "First 5 observation_dates: ";
      for (int i = 0; i < std::min(5, static_cast<int>(obs_array->length())); i++) {
        std::cout << obs_array->GetString(i);
        if (i < 4) std::cout << ", ";
      }
      std::cout << "\n";
    }
  }

  // Sanity check - 10 years of data with ALFRED revisions
  // CPI: monthly data (~120 obs * multiple revisions per obs)
  // Fed Funds: daily data (~2500 obs * multiple revisions per obs)
  // Unemployment: monthly data (~120 obs * multiple revisions per obs)
  // With pagination, we should successfully fetch all data
  REQUIRE(cpi_rows > 100);
  REQUIRE(fedfunds_rows > 100);  // Much more due to daily frequency
  REQUIRE(unemployment_rows > 100);

  // Verify DataFrames have expected columns with ALFRED enabled
  REQUIRE(results[0]->contains("observation_date"));
  REQUIRE(results[0]->contains("value"));
  REQUIRE(results[1]->contains("observation_date"));
  REQUIRE(results[1]->contains("value"));
  REQUIRE(results[2]->contains("observation_date"));
  REQUIRE(results[2]->contains("value"));

  std::cout << "=== All validations passed! ===\n\n";
}
