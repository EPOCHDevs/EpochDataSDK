#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <iomanip>

#include <epoch_data_sdk/common/async_batch.hpp>
#include "../common/test_utils.hpp"
#include "../src/fred/fred_client.hpp"
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

TEST_CASE("FredClient - Simple FRED data (latest revisions only)", "[fred][fred_client]") {
  if (!hasApiKey()) {
    SKIP("FRED_API_KEY not set");
  }

  Options opt;
  opt.api_key = getApiKey();

  FredClient client(opt);

  SECTION("getCPI - Consumer Price Index") {
    auto df = client.getCPI("2023-01-01", "2023-03-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);

    // FredClient should return simple schema: just value column
    // Index is observation_date
    REQUIRE(df->contains("value"));
    REQUIRE_FALSE(df->contains("observation_date"));  // It's the index
    REQUIRE_FALSE(df->contains("published_at"));      // No revision tracking
    REQUIRE_FALSE(df->contains("revision"));           // No revision tracking

    std::cout << "FredClient CPI rows: " << df->num_rows() << "\n";
  }

  SECTION("getFedFunds - Federal Funds Rate") {
    auto df = client.getFedFunds("2023-01-01", "2023-01-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);

    REQUIRE(df->contains("value"));
    REQUIRE_FALSE(df->contains("published_at"));

    std::cout << "FredClient Fed Funds rows: " << df->num_rows() << "\n";
  }

  SECTION("getGDP - Gross Domestic Product") {
    auto df = client.getGDP("2022-01-01", "2023-12-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);

    REQUIRE(df->contains("value"));

    std::cout << "FredClient GDP rows: " << df->num_rows() << "\n";
  }

  SECTION("getUnemployment - Unemployment Rate") {
    auto df = client.getUnemployment("2023-01-01", "2023-12-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);

    REQUIRE(df->contains("value"));

    std::cout << "FredClient Unemployment rows: " << df->num_rows() << "\n";
  }
}

TEST_CASE("FredClient - Metadata verification", "[fred][fred_client][metadata]") {
  auto metadata = FredClient::getMetadata();

  SECTION("Metadata structure") {
    REQUIRE(metadata.data_type == "fred_series");
    REQUIRE(metadata.description.find("FRED") != std::string::npos);
    REQUIRE(metadata.description.find("revised") != std::string::npos);
    REQUIRE(metadata.index_normalized == true);

    // Should have exactly 1 column (value)
    REQUIRE(metadata.columns.size() == 1);

    // Check value column
    auto& value_col = metadata.columns[0];
    REQUIRE(value_col.id == "value");
    REQUIRE(value_col.type == ArrowType::FLOAT64);
    REQUIRE(value_col.nullable == true);
  }

  if (!hasApiKey()) {
    SKIP("FRED_API_KEY not set - skipping real data test");
  }

  SECTION("Real data matches metadata") {
    Options opt;
    opt.api_key = getApiKey();
    FredClient client(opt);

    auto result = client.getCPI("2023-01-01", "2023-03-31");
    REQUIRE(result.has_value());

    // Use the test utility to verify schema
    test::validateDataFrameAgainstMetadata(*result, metadata, "FredClient");

    std::cout << "FredClient metadata verification passed!\n";
  }
}

TEST_CASE("FredClient - Async batch requests", "[fred][fred_client][async]") {
  if (!hasApiKey()) {
    SKIP("FRED_API_KEY not set");
  }

  Options opt;
  opt.api_key = getApiKey();
  FredClient client(opt);

  SECTION("Concurrent async requests") {
    INFO("Creating concurrent async tasks for CPI, Fed Funds, and GDP...");

    std::vector<drogon::Task<Expected<epoch_frame::DataFrame>>> tasks;
    tasks.push_back(client.getCPIAsync("2023-01-01", "2023-12-31"));
    tasks.push_back(client.getFedFundsAsync("2023-01-01", "2023-12-31"));
    tasks.push_back(client.getGDPAsync("2023-01-01", "2023-12-31"));

    auto start = std::chrono::steady_clock::now();
    auto results = syncJoinAll(std::move(tasks));
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start).count();

    std::cout << "\nFredClient async test completed in " << elapsed << "ms\n";

    REQUIRE(results.size() == 3);
    REQUIRE(results[0].has_value());
    REQUIRE(results[1].has_value());
    REQUIRE(results[2].has_value());

    std::cout << "CPI rows: " << results[0]->num_rows() << "\n";
    std::cout << "Fed Funds rows: " << results[1]->num_rows() << "\n";
    std::cout << "GDP rows: " << results[2]->num_rows() << "\n";

    // All should have simple schema
    REQUIRE(results[0]->contains("value"));
    REQUIRE(results[1]->contains("value"));
    REQUIRE(results[2]->contains("value"));
  }
}

TEST_CASE("FredClient vs AlfredClient - Side-by-side comparison", "[fred][fred_client][comparison]") {
  if (!hasApiKey()) {
    SKIP("FRED_API_KEY not set");
  }

  Options opt;
  opt.api_key = getApiKey();

  FredClient fred_client(opt);
  AlfredClient alfred_client(opt);

  SECTION("Same period comparison - CPI") {
    std::cout << "\n========================================\n";
    std::cout << "FRED vs ALFRED Comparison - CPI Data\n";
    std::cout << "Period: 2023-01-01 to 2023-03-31\n";
    std::cout << "========================================\n\n";

    // Fetch from both clients
    auto fred_df = fred_client.getCPI("2023-01-01", "2023-03-31");
    auto alfred_df = alfred_client.getCPI("2023-01-01", "2023-03-31");

    if (!fred_df.has_value()) {
      std::cout << "FredClient error: " << fred_df.error().message << "\n";
      std::cout << "HTTP status: " << fred_df.error().http_status << "\n";
    }
    REQUIRE(fred_df.has_value());

    if (!alfred_df.has_value()) {
      std::cout << "AlfredClient error: " << alfred_df.error().message << "\n";
      std::cout << "HTTP status: " << alfred_df.error().http_status << "\n";
    }
    REQUIRE(alfred_df.has_value());

    std::cout << "--- FRED CLIENT (Latest Revisions Only) ---\n";
    std::cout << "Total rows: " << fred_df->num_rows() << "\n";
    std::cout << "Columns: " << fred_df->column_names().size() << " (";
    for (size_t i = 0; i < fred_df->column_names().size(); i++) {
      if (i > 0) std::cout << ", ";
      std::cout << fred_df->column_names()[i];
    }
    std::cout << ")\n";
    std::cout << "Schema: observation_date (index), value (column)\n\n";

    // Reset index to make observation_date a column for easier printing
    auto fred_with_index = fred_df->reset_index("observation_date");
    auto fred_table = fred_with_index.table();
    auto fred_obs_col = fred_table->GetColumnByName("observation_date");
    auto fred_value_col = fred_table->GetColumnByName("value");
    auto fred_obs_array = std::static_pointer_cast<arrow::TimestampArray>(fred_obs_col->chunk(0));
    auto fred_value_array = std::static_pointer_cast<arrow::DoubleArray>(fred_value_col->chunk(0));

    std::cout << "All FRED rows:\n";
    std::cout << std::setw(25) << "observation_date_ns" << std::setw(15) << "value" << "\n";
    std::cout << std::string(40, '-') << "\n";

    for (int64_t i = 0; i < fred_with_index.num_rows(); i++) {
      int64_t obs_ts = fred_obs_array->Value(i);
      double value = fred_value_array->Value(i);
      std::cout << std::setw(25) << obs_ts << std::setw(15) << std::fixed << std::setprecision(3) << value << "\n";
    }

    std::cout << "\n--- ALFRED CLIENT (Full Revision History) ---\n";
    std::cout << "Total rows: " << alfred_df->num_rows() << "\n";
    std::cout << "Columns: " << alfred_df->column_names().size() << " (";
    for (size_t i = 0; i < alfred_df->column_names().size(); i++) {
      if (i > 0) std::cout << ", ";
      std::cout << alfred_df->column_names()[i];
    }
    std::cout << ")\n";
    std::cout << "Schema: published_at (index), observation_date, value, revision (columns)\n\n";

    // Print all ALFRED rows - reset index to get published_at as a column
    auto alfred_with_index = alfred_df->reset_index("published_at");
    auto alfred_table = alfred_with_index.table();
    auto alfred_pub_col = alfred_table->GetColumnByName("published_at");
    auto alfred_obs_col = alfred_table->GetColumnByName("observation_date");
    auto alfred_val_col = alfred_table->GetColumnByName("value");
    auto alfred_rev_col = alfred_table->GetColumnByName("revision");

    auto alfred_pub_array = std::static_pointer_cast<arrow::TimestampArray>(alfred_pub_col->chunk(0));
    auto alfred_obs_array = std::static_pointer_cast<arrow::TimestampArray>(alfred_obs_col->chunk(0));
    auto alfred_val_array = std::static_pointer_cast<arrow::DoubleArray>(alfred_val_col->chunk(0));
    auto alfred_rev_array = std::static_pointer_cast<arrow::Int64Array>(alfred_rev_col->chunk(0));

    std::cout << "All ALFRED rows:\n";
    std::cout << std::setw(21) << "published_at_ns"
              << std::setw(21) << "observation_ts_ns"
              << std::setw(15) << "value"
              << std::setw(10) << "revision" << "\n";
    std::cout << std::string(67, '-') << "\n";

    for (int64_t i = 0; i < alfred_with_index.num_rows(); i++) {
      int64_t pub_ts = alfred_pub_array->Value(i);
      int64_t obs_ts = alfred_obs_array->Value(i);
      double value = alfred_val_array->Value(i);
      int64_t revision = alfred_rev_array->Value(i);

      std::cout << std::setw(21) << pub_ts
                << std::setw(21) << obs_ts
                << std::setw(15) << std::fixed << std::setprecision(3) << value
                << std::setw(10) << revision << "\n";
    }

    std::cout << "\n--- ANALYSIS ---\n";
    std::cout << "Key Differences:\n";
    std::cout << "1. FRED: " << fred_df->num_rows() << " rows (one per observation_date)\n";
    std::cout << "2. ALFRED: " << alfred_df->num_rows() << " rows (multiple revisions per observation_date)\n";
    std::cout << "3. FRED shows latest value for each observation_date\n";
    std::cout << "4. ALFRED shows ALL published revisions with published_at timestamps\n";
    std::cout << "\nNote: observation_date represents the economic period being measured,\n";
    std::cout << "while published_at shows when that measurement was published/revised.\n";
    std::cout << "It's normal for published_at to be AFTER observation_date because\n";
    std::cout << "economic data is published with a lag and gets revised over time.\n";
    std::cout << "========================================\n\n";
  }
}
