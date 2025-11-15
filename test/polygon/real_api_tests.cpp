#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <cstdlib>
#include <iostream>

#include <epoch_data_sdk/common/async_batch.hpp>
#include "../common/test_utils.hpp"
#include "../src/polygon/aggs_client.hpp"
#include "../src/polygon/news_client.hpp"
#include "../src/polygon/ipo_client.hpp"
#include "../src/polygon/splits_client.hpp"
#include "../src/polygon/dividends_client.hpp"
#include "../src/polygon/ticker_events_client.hpp"
#include "../src/polygon/trades_client.hpp"
#include "../src/polygon/quotes_client.hpp"
#include "../src/polygon/short_volume_client.hpp"
#include "../src/polygon/short_interest_client.hpp"
#include "../src/polygon/ratios_client.hpp"

using namespace data_sdk::polygon;
using namespace data_sdk::common;
using namespace data_sdk;
using namespace data_sdk::test;

// Real API tests require POLYGON_API_KEY environment variable
// Tagged with [!hide] so they only run when explicitly requested
// Run with: ./test/epoch_data_sdk_test "[real_api][long_range]"

TEST_CASE("Real API: Batch AAPL, MSFT, NVDA 2010-2020 daily data",
          "[polygon][real_api][long_range]") {

  // Read API key from environment
  const char* api_key = std::getenv("POLYGON_API_KEY");
  REQUIRE(api_key != nullptr);

  INFO("Using Polygon API key from environment");

  Options opt;
  opt.api_key = api_key;
  opt.request_timeout_sec = 10.0;  // Increase timeout for long requests

  AggsClient cli(opt);

  INFO("Creating 3 concurrent tasks for 10 years of daily data...");

  // Create 3 concurrent tasks for 10 years of DAILY data (2010-2020)
  // Using daily instead of minute to avoid rate limiting and reduce data volume
  // This will test:
  // - Concurrent execution of multiple requests
  // - Pagination handling for moderate datasets
  // - Coroutine parameter lifetime correctness
  // - Order preservation in results
  std::vector<drogon::Task<Expected<epoch_frame::DataFrame>>> tasks;
  tasks.push_back(cli.getAggregatesAsync("AAPL", "2010-01-01", "2020-12-31", true));  // true = daily
  tasks.push_back(cli.getAggregatesAsync("MSFT", "2010-01-01", "2020-12-31", true));
  tasks.push_back(cli.getAggregatesAsync("NVDA", "2010-01-01", "2020-12-31", true));

  INFO("Executing all 3 tasks concurrently with syncJoinAll()...");

  // Measure execution time
  auto start = std::chrono::steady_clock::now();

  // Execute all concurrently
  auto results = syncJoinAll(std::move(tasks));

  auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
    std::chrono::steady_clock::now() - start).count();

  std::cout << "\n=== Real API Test Results ===\n";
  std::cout << "Completed in " << elapsed << " seconds\n";

  // Validate all succeeded
  REQUIRE(results.size() == 3);

  INFO("Validating AAPL result (index 0)...");
  if (!results[0].has_value()) {
    WARN("AAPL error: " << results[0].error().message);
  }
  REQUIRE(results[0].has_value());

  INFO("Validating MSFT result (index 1)...");
  if (!results[1].has_value()) {
    WARN("MSFT error: " << results[1].error().message);
  }
  REQUIRE(results[1].has_value());

  INFO("Validating NVDA result (index 2)...");
  if (!results[2].has_value()) {
    WARN("NVDA error: " << results[2].error().message);
  }
  REQUIRE(results[2].has_value());

  // Check that we got meaningful amounts of data
  auto aapl_rows = results[0]->num_rows();
  auto msft_rows = results[1]->num_rows();
  auto nvda_rows = results[2]->num_rows();

  std::cout << "AAPL rows: " << aapl_rows << "\n";
  std::cout << "MSFT rows: " << msft_rows << "\n";
  std::cout << "NVDA rows: " << nvda_rows << "\n";

  // Sanity check - 10 years of daily data should have ~2500 trading days
  // (252 trading days/year * 10 years = ~2520 days)
  // With market holidays, expecting at least 2000 rows
  REQUIRE(aapl_rows > 2000);
  REQUIRE(msft_rows > 2000);
  REQUIRE(nvda_rows > 2000);

  // Verify DataFrames have expected columns for daily aggregates
  REQUIRE(results[0]->contains("o"));  // open
  REQUIRE(results[0]->contains("h"));  // high
  REQUIRE(results[0]->contains("l"));  // low
  REQUIRE(results[0]->contains("c"));  // close
  REQUIRE(results[0]->contains("v"));  // volume

  std::cout << "=== All validations passed! ===\n\n";
}

TEST_CASE("Real API: AggsOptions struct usage",
          "[polygon][real_api][options]") {

  const char* api_key = std::getenv("POLYGON_API_KEY");
  REQUIRE(api_key != nullptr);

  Options opt;
  opt.api_key = api_key;

  AggsClient cli(opt);

  // Test using the options struct
  AggsOptions opts{"AAPL", "2024-01-01", "2024-01-31", true, true};

  auto result = cli.getAggregates(opts);

  REQUIRE(result.has_value());
  REQUIRE(result->num_rows() > 0);

  std::cout << "Options struct test: AAPL daily data for Jan 2024\n";
  std::cout << "Rows: " << result->num_rows() << "\n";
}

TEST_CASE("Real API: News metadata verification", "[polygon][real_api][metadata]") {
  const char* api_key = std::getenv("POLYGON_API_KEY");
  REQUIRE(api_key != nullptr);

  Options opt;
  opt.api_key = api_key;

  NewsClient client(opt);

  // Get news data
  auto result = client.getNews("AAPL", "2024-01-01", "2024-01-31", 10);
  REQUIRE(result.has_value());

  std::cout << "News: Got " << result->num_rows() << " rows\n";

  // Get metadata
  auto metadata = client.getMetadata();

  // Verify DataFrame matches metadata
  validateDataFrameAgainstMetadata(*result, metadata, "AggsClient(AAPL)");

  std::cout << "News metadata verification passed!\n";
}

TEST_CASE("Real API: IPO metadata verification", "[polygon][real_api][metadata]") {
  const char* api_key = std::getenv("POLYGON_API_KEY");
  REQUIRE(api_key != nullptr);

  Options opt;
  opt.api_key = api_key;

  IPOClient client(opt);

  // Get IPO data
  auto result = client.getIPOs("2024-01-01", "2024-12-31", std::nullopt, 100);
  REQUIRE(result.has_value());

  std::cout << "IPOs: Got " << result->num_rows() << " rows\n";

  // Get metadata
  auto metadata = client.getMetadata();

  // Verify DataFrame matches metadata
  validateDataFrameAgainstMetadata(*result, metadata, "AggsClient(AAPL)");

  std::cout << "IPO metadata verification passed!\n";
}

TEST_CASE("Real API: Splits metadata verification", "[polygon][real_api][metadata]") {
  const char* api_key = std::getenv("POLYGON_API_KEY");
  REQUIRE(api_key != nullptr);

  Options opt;
  opt.api_key = api_key;

  SplitsClient client(opt);

  // Get splits data for AAPL
  auto result = client.getSplits("AAPL", std::nullopt, "2020-01-01", "2024-12-31", std::nullopt, 100);
  REQUIRE(result.has_value());

  std::cout << "Splits: Got " << result->num_rows() << " rows\n";

  // Get metadata
  auto metadata = client.getMetadata();

  // Verify DataFrame matches metadata
  validateDataFrameAgainstMetadata(*result, metadata, "AggsClient(AAPL)");

  std::cout << "Splits metadata verification passed!\n";
}

TEST_CASE("Real API: Dividends metadata verification", "[polygon][real_api][metadata]") {
  const char* api_key = std::getenv("POLYGON_API_KEY");
  REQUIRE(api_key != nullptr);

  Options opt;
  opt.api_key = api_key;

  DividendsClient client(opt);

  // Get dividends data for AAPL
  auto result = client.getDividends("AAPL", std::nullopt, "2023-01-01", "2024-12-31", std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt, 100);
  if (!result.has_value()) {
    std::cout << "Dividends API error: " << result.error().message << " (HTTP " << result.error().http_status << ")\n";
  }
  REQUIRE(result.has_value());

  std::cout << "Dividends: Got " << result->num_rows() << " rows\n";

  // Get metadata
  auto metadata = client.getMetadata();

  // Verify DataFrame matches metadata
  validateDataFrameAgainstMetadata(*result, metadata, "AggsClient(AAPL)");

  std::cout << "Dividends metadata verification passed!\n";
}

TEST_CASE("Real API: Ticker Events metadata verification", "[polygon][real_api][metadata]") {
  const char* api_key = std::getenv("POLYGON_API_KEY");
  REQUIRE(api_key != nullptr);

  Options opt;
  opt.api_key = api_key;

  TickerEventsClient client(opt);

  // Get ticker events for AAPL
  auto result = client.getTickerEvents("AAPL");
  REQUIRE(result.has_value());

  std::cout << "Ticker Events: Got " << result->num_rows() << " rows\n";

  // Get metadata
  auto metadata = client.getMetadata();

  // Verify DataFrame matches metadata
  validateDataFrameAgainstMetadata(*result, metadata, "AggsClient(AAPL)");

  std::cout << "Ticker Events metadata verification passed!\n";
}

TEST_CASE("Real API: Aggs metadata verification", "[polygon][real_api][metadata]") {
  const char* api_key = std::getenv("POLYGON_API_KEY");
  REQUIRE(api_key != nullptr);

  Options opt;
  opt.api_key = api_key;

  AggsClient client(opt);

  // Get AAPL daily data for January 2024
  auto result = client.getAggregates("AAPL", "2024-01-01", "2024-01-31", true);
  REQUIRE(result.has_value());

  std::cout << "Aggs: Got " << result->num_rows() << " rows\n";

  // Get metadata
  auto metadata = AggsClient::getMetadata();

  // Verify DataFrame matches metadata
  validateDataFrameAgainstMetadata(*result, metadata, "AggsClient(AAPL)");

  std::cout << "Aggs metadata verification passed!\n";
}

TEST_CASE("Real API: Trades metadata verification", "[polygon][real_api][metadata]") {
  const char* api_key = std::getenv("POLYGON_API_KEY");
  REQUIRE(api_key != nullptr);

  Options opt;
  opt.api_key = api_key;

  TradesClient client(opt);

  // Get AAPL trades for a specific day
  auto result = client.getTrades("AAPL", "2024-01-02", "2024-01-02", 100);
  REQUIRE(result.has_value());

  std::cout << "Trades: Got " << result->num_rows() << " rows\n";

  // Get metadata
  auto metadata = TradesClient::getMetadata();

  // Verify DataFrame matches metadata
  validateDataFrameAgainstMetadata(*result, metadata, "AggsClient(AAPL)");

  std::cout << "Trades metadata verification passed!\n";
}

TEST_CASE("Real API: Quotes metadata verification", "[polygon][real_api][metadata]") {
  const char* api_key = std::getenv("POLYGON_API_KEY");
  REQUIRE(api_key != nullptr);

  Options opt;
  opt.api_key = api_key;

  QuotesClient client(opt);

  // Get AAPL quotes for a specific day
  auto result = client.getQuotes("AAPL", "2024-01-02", "2024-01-02", 100);
  REQUIRE(result.has_value());

  std::cout << "Quotes: Got " << result->num_rows() << " rows\n";

  // Get metadata
  auto metadata = QuotesClient::getMetadata();

  // Verify DataFrame matches metadata
  validateDataFrameAgainstMetadata(*result, metadata, "AggsClient(AAPL)");

  std::cout << "Quotes metadata verification passed!\n";
}

TEST_CASE("Real API: Short Volume metadata verification", "[polygon][real_api][metadata]") {
  const char* api_key = std::getenv("POLYGON_API_KEY");
  REQUIRE(api_key != nullptr);

  Options opt;
  opt.api_key = api_key;

  ShortVolumeClient client(opt);

  // Get AAPL short volume data
  auto result = client.getShortVolume("AAPL", "2024-01-01", "2024-01-31");
  REQUIRE(result.has_value());

  std::cout << "Short Volume: Got " << result->num_rows() << " rows\n";

  // Get metadata
  auto metadata = ShortVolumeClient::getMetadata();

  // Verify DataFrame matches metadata
  validateDataFrameAgainstMetadata(*result, metadata, "AggsClient(AAPL)");

  std::cout << "Short Volume metadata verification passed!\n";
}

TEST_CASE("Real API: Short Interest metadata verification", "[polygon][real_api][metadata]") {
  const char* api_key = std::getenv("POLYGON_API_KEY");
  REQUIRE(api_key != nullptr);

  Options opt;
  opt.api_key = api_key;

  ShortInterestClient client(opt);

  // Get AAPL short interest data
  auto result = client.getShortInterest("AAPL", "2024-01-01", "2024-01-31");
  REQUIRE(result.has_value());

  std::cout << "Short Interest: Got " << result->num_rows() << " rows\n";

  // Get metadata
  auto metadata = ShortInterestClient::getMetadata();

  // Verify DataFrame matches metadata
  validateDataFrameAgainstMetadata(*result, metadata, "AggsClient(AAPL)");

  std::cout << "Short Interest metadata verification passed!\n";
}

TEST_CASE("Real API: Ratios metadata verification", "[polygon][real_api][metadata]") {
  const char* api_key = std::getenv("POLYGON_API_KEY");
  REQUIRE(api_key != nullptr);

  Options opt;
  opt.api_key = api_key;

  RatiosClient client(opt);

  // Get AAPL financial ratios
  auto result = client.getRatios("AAPL", 10, std::nullopt);
  REQUIRE(result.has_value());

  std::cout << "Ratios: Got " << result->num_rows() << " rows\n";

  // Get metadata
  auto metadata = RatiosClient::getMetadata();

  // Verify DataFrame matches metadata
  validateDataFrameAgainstMetadata(*result, metadata, "AggsClient(AAPL)");

  std::cout << "Ratios metadata verification passed!\n";
}
