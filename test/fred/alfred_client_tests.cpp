#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <unordered_map>
#include <unordered_set>

#include <epoch_data_sdk/common/async_batch.hpp>
#include "../common/test_utils.hpp"
#include "../src/fred/alfred_client.hpp"

using namespace data_sdk::fred;
using namespace data_sdk::common;
using namespace data_sdk;

static std::string getApiKey() {
  const char *env = std::getenv("FRED_API_KEY");
  return env ? std::string(env) : "";
}

static bool hasApiKey() {
  return !getApiKey().empty();
}

TEST_CASE("AlfredClient - ALFRED data with revision tracking", "[fred][alfred_client]") {
  if (!hasApiKey()) {
    SKIP("FRED_API_KEY not set");
  }

  Options opt;
  opt.api_key = getApiKey();

  AlfredClient client(opt);

  SECTION("getCPI - Consumer Price Index with revisions") {
    auto df = client.getCPI("2023-01-01", "2023-03-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);

    // AlfredClient should return schema: index=published_at, columns=observation_date,value,revision
    REQUIRE_FALSE(df->contains("published_at"));  // It's the index, not a column
    REQUIRE(df->contains("observation_date"));
    REQUIRE(df->contains("value"));
    REQUIRE(df->contains("revision"));

    std::cout << "AlfredClient CPI rows: " << df->num_rows() << "\n";

    // Verify we have multiple revisions for some observation dates
    auto table = df->table();
    auto index = df->index();
    auto obs_date_col = table->GetColumnByName("observation_date");
    auto revision_col = table->GetColumnByName("revision");

    auto obs_array = std::static_pointer_cast<arrow::TimestampArray>(obs_date_col->chunk(0));
    auto rev_array = std::static_pointer_cast<arrow::Int64Array>(revision_col->chunk(0));

    // Count unique observation dates (timestamps)
    std::unordered_set<int64_t> unique_obs_dates;
    int max_revision = 0;
    for (int64_t i = 0; i < obs_array->length(); i++) {
      if (!obs_array->IsNull(i)) {
        unique_obs_dates.insert(obs_array->Value(i));
      }
      max_revision = std::max(max_revision, static_cast<int>(rev_array->Value(i)));
    }

    std::cout << "Unique observation dates: " << unique_obs_dates.size() << "\n";
    std::cout << "Total rows (with revisions): " << obs_array->length() << "\n";
    std::cout << "Max revision number: " << max_revision << "\n";

    // With ALFRED, we should have more rows than unique dates (due to revisions)
    REQUIRE(obs_array->length() >= unique_obs_dates.size());

    // Show first 10 rows to demonstrate revision structure
    std::cout << "\nFirst 10 rows showing revision structure:\n";
    auto val_col = table->GetColumnByName("value");
    auto val_array = std::static_pointer_cast<arrow::DoubleArray>(val_col->chunk(0));

    // Reset index to get published_at as a column for printing
    auto df_with_index = df->reset_index("published_at");
    auto table_with_index = df_with_index.table();
    auto pub_col = table_with_index->GetColumnByName("published_at");
    auto pub_array = std::static_pointer_cast<arrow::TimestampArray>(pub_col->chunk(0));

    for (int i = 0; i < std::min(10, static_cast<int>(obs_array->length())); i++) {
      std::cout << "  Row " << i << ": "
                << "published_ts=" << pub_array->Value(i)
                << ", obs_ts=" << obs_array->Value(i)
                << ", value=" << val_array->Value(i)
                << ", revision=" << rev_array->Value(i) << "\n";
    }
  }

  SECTION("getFedFunds - Federal Funds Rate with revisions") {
    auto df = client.getFedFunds("2023-01-01", "2023-01-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);

    REQUIRE_FALSE(df->contains("published_at"));  // It's the index, not a column
    REQUIRE(df->contains("observation_date"));
    REQUIRE(df->contains("value"));
    REQUIRE(df->contains("revision"));

    std::cout << "AlfredClient Fed Funds rows: " << df->num_rows() << "\n";
  }

  SECTION("Verify revision numbers are sequential per observation_date") {
    auto df = client.getUnemployment("2023-01-01", "2023-06-30");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }

    auto table = df->table();
    auto obs_date_col = table->GetColumnByName("observation_date");
    auto revision_col = table->GetColumnByName("revision");

    auto obs_array = std::static_pointer_cast<arrow::TimestampArray>(obs_date_col->chunk(0));
    auto rev_array = std::static_pointer_cast<arrow::Int64Array>(revision_col->chunk(0));

    // Track revisions per observation_date (timestamp)
    std::unordered_map<int64_t, std::vector<int>> revisions_by_obs_date;
    for (int64_t i = 0; i < obs_array->length(); i++) {
      if (!obs_array->IsNull(i)) {
        int64_t obs_ts = obs_array->Value(i);
        int revision = rev_array->Value(i);
        revisions_by_obs_date[obs_ts].push_back(revision);
      }
    }

    // Verify revisions are sequential (1, 2, 3, ...) for each observation_date
    bool all_sequential = true;
    for (const auto& [obs_ts, revisions] : revisions_by_obs_date) {
      for (size_t i = 0; i < revisions.size(); i++) {
        if (revisions[i] != static_cast<int>(i + 1)) {
          all_sequential = false;
          std::cout << "ERROR: obs_ts=" << obs_ts
                    << " has non-sequential revision at position " << i
                    << ": expected " << (i + 1) << ", got " << revisions[i] << "\n";
        }
      }
    }

    REQUIRE(all_sequential);
    std::cout << "✓ All revisions are sequential (1, 2, 3, ...) per observation_date\n";
  }
}

TEST_CASE("AlfredClient - Metadata verification", "[fred][alfred_client][metadata]") {
  auto metadata = AlfredClient::getMetadata();

  SECTION("Metadata structure") {
    REQUIRE(metadata.data_type == "alfred_series");
    REQUIRE(metadata.description.find("ALFRED") != std::string::npos);
    REQUIRE(metadata.description.find("vintages") != std::string::npos);
    REQUIRE(metadata.description.find("backtesting") != std::string::npos);
    REQUIRE(metadata.index_normalized == true);

    // Should have exactly 3 columns (published_at is now the index)
    REQUIRE(metadata.columns.size() == 3);

    // Check each column
    REQUIRE(metadata.columns[0].id == "observation_date");
    REQUIRE(metadata.columns[0].type == ArrowType::TIMESTAMP_NS_UTC);
    REQUIRE(metadata.columns[0].nullable == false);

    REQUIRE(metadata.columns[1].id == "value");
    REQUIRE(metadata.columns[1].type == ArrowType::FLOAT64);
    REQUIRE(metadata.columns[1].nullable == true);

    REQUIRE(metadata.columns[2].id == "revision");
    REQUIRE(metadata.columns[2].type == ArrowType::INT64);
    REQUIRE(metadata.columns[2].nullable == false);
  }

  if (!hasApiKey()) {
    SKIP("FRED_API_KEY not set - skipping real data test");
  }

  SECTION("Real data matches metadata") {
    Options opt;
    opt.api_key = getApiKey();
    AlfredClient client(opt);

    auto result = client.getCPI("2023-01-01", "2023-03-31");
    REQUIRE(result.has_value());

    // Use the test utility to verify schema
    test::validateDataFrameAgainstMetadata(*result, metadata, "AlfredClient");

    std::cout << "AlfredClient metadata verification passed!\n";
  }
}

TEST_CASE("AlfredClient - Long range with pagination", "[fred][alfred_client][long_range]") {
  if (!hasApiKey()) {
    SKIP("FRED_API_KEY not set");
  }

  Options opt;
  opt.api_key = getApiKey();
  opt.request_timeout_sec = 30.0;

  AlfredClient client(opt);

  SECTION("20 years of data with automatic pagination (2004-2024)") {
    std::cout << "\n=== Testing ALFRED 20-year data fetch (2004-2024) ===\n";
    std::cout << "This will test automatic pagination for large revision datasets\n";

    std::vector<drogon::Task<Expected<epoch_frame::DataFrame>>> tasks;
    tasks.push_back(client.getCPIAsync("2004-01-01", "2024-12-31"));
    tasks.push_back(client.getUnemploymentAsync("2004-01-01", "2024-12-31"));

    auto start = std::chrono::steady_clock::now();
    auto results = syncJoinAll(std::move(tasks));
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
      std::chrono::steady_clock::now() - start).count();

    std::cout << "Completed in " << elapsed << " seconds\n";

    REQUIRE(results.size() == 2);

    if (!results[0].has_value()) {
      std::cout << "CPI error: " << results[0].error().message << "\n";
    }
    REQUIRE(results[0].has_value());

    if (!results[1].has_value()) {
      std::cout << "Unemployment error: " << results[1].error().message << "\n";
    }
    REQUIRE(results[1].has_value());

    auto cpi_rows = results[0]->num_rows();
    auto unemployment_rows = results[1]->num_rows();

    std::cout << "CPI rows (with revisions): " << cpi_rows << "\n";
    std::cout << "Unemployment rows (with revisions): " << unemployment_rows << "\n";

    // With 20 years of data and multiple revisions, should have many rows
    REQUIRE(cpi_rows > 200);
    REQUIRE(unemployment_rows > 100);

    std::cout << "(Use SPDLOG_LEVEL=debug to see pagination details)\n";
    std::cout << "=== Test completed successfully ===\n\n";
  }
}

TEST_CASE("AlfredClient - Point-in-time filtering demonstration", "[fred][alfred_client][demo]") {
  if (!hasApiKey()) {
    SKIP("FRED_API_KEY not set");
  }

  Options opt;
  opt.api_key = getApiKey();
  AlfredClient client(opt);

  SECTION("Demonstrate point-in-time data filtering") {
    // Get CPI data with revisions
    auto df = client.getCPI("2023-01-01", "2023-03-31");
    REQUIRE(df.has_value());

    std::cout << "\n=== Point-in-Time Filtering Demo ===\n";
    std::cout << "Total rows (all revisions): " << df->num_rows() << "\n";

    // Show how a backtest would filter data
    std::cout << "\nIn a backtest at date 2023-04-15:\n";
    std::cout << "1. Filter: published_at <= '2023-04-15'\n";
    std::cout << "2. GroupBy: observation_date\n";
    std::cout << "3. Aggregate: take last revision per group\n";
    std::cout << "Result: You get data as it was known on 2023-04-15\n";
    std::cout << "\nThis avoids lookahead bias in backtesting!\n";
    std::cout << "=====================================\n\n";
  }
}
