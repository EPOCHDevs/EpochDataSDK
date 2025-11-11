#include <catch2/catch_test_macros.hpp>
#include "../src/sec/insider_trading_client.hpp"
#include "../src/sec/form13f_client.hpp"
#include <epoch_frame/series.h>

using namespace data_sdk::sec;

TEST_CASE("InsiderTradingClient - Pagination with multiple pages", "[sec][insider][pagination]") {
  Options opts;
  opts.api_key = "test_key";
  opts.enable_rate_limiting = false;

  int call_count = 0;

  opts.http_post_override = [&call_count](const std::string &path,
                                const std::string &body,
                                const std::vector<std::pair<std::string, std::string>> &query) {
    call_count++;

    // Parse the 'from' parameter from the query
    size_t from_pos = body.find("\"from\": \"");
    int from_value = 0;
    if (from_pos != std::string::npos) {
      from_pos += 9; // strlen("\"from\": \"")
      size_t end_pos = body.find("\"", from_pos);
      from_value = std::stoi(body.substr(from_pos, end_pos - from_pos));
    }

    // Simulate 150 total results (3 pages of 50)
    if (from_value == 0) {
      // Page 1: return 50 results
      std::string json_response = R"({
        "data": [)";
      for (int i = 0; i < 50; i++) {
        if (i > 0) json_response += ",";
        json_response += R"({
          "filingUrl": "https://example.com/filing)" + std::to_string(i) + R"(",
          "filedAt": "2024-01-)" + std::to_string(i % 28 + 1) + R"(T10:00:00-05:00",
          "issuerCik": "320193",
          "issuerName": "Apple Inc",
          "issuerTicker": "AAPL",
          "ownerCik": "123456",
          "ownerName": "Insider )" + std::to_string(i) + R"(",
          "transactionDate": "2024-01-)" + std::to_string(i % 28 + 1) + R"(",
          "transactionCode": "P",
          "securityTitle": "Common Stock",
          "transactionShares": 1000.0,
          "transactionPricePerShare": 175.0,
          "sharesOwnedFollowingTransaction": 10000.0
        })";
      }
      json_response += R"(],
        "total": {"value": 150, "relation": "eq"}
      })";
      return Expected<std::string>(json_response);
    }
    else if (from_value == 50) {
      // Page 2: return 50 results
      std::string json_response = R"({
        "data": [)";
      for (int i = 50; i < 100; i++) {
        if (i > 50) json_response += ",";
        json_response += R"({
          "filingUrl": "https://example.com/filing)" + std::to_string(i) + R"(",
          "filedAt": "2024-02-)" + std::to_string((i - 50) % 28 + 1) + R"(T10:00:00-05:00",
          "issuerCik": "320193",
          "issuerName": "Apple Inc",
          "issuerTicker": "AAPL",
          "ownerCik": "123456",
          "ownerName": "Insider )" + std::to_string(i) + R"(",
          "transactionDate": "2024-02-)" + std::to_string((i - 50) % 28 + 1) + R"(",
          "transactionCode": "P",
          "securityTitle": "Common Stock",
          "transactionShares": 1000.0,
          "transactionPricePerShare": 175.0,
          "sharesOwnedFollowingTransaction": 10000.0
        })";
      }
      json_response += R"(],
        "total": {"value": 150, "relation": "eq"}
      })";
      return Expected<std::string>(json_response);
    }
    else if (from_value == 100) {
      // Page 3: return 50 results (last page)
      std::string json_response = R"({
        "data": [)";
      for (int i = 100; i < 150; i++) {
        if (i > 100) json_response += ",";
        json_response += R"({
          "filingUrl": "https://example.com/filing)" + std::to_string(i) + R"(",
          "filedAt": "2024-03-)" + std::to_string((i - 100) % 28 + 1) + R"(T10:00:00-05:00",
          "issuerCik": "320193",
          "issuerName": "Apple Inc",
          "issuerTicker": "AAPL",
          "ownerCik": "123456",
          "ownerName": "Insider )" + std::to_string(i) + R"(",
          "transactionDate": "2024-03-)" + std::to_string((i - 100) % 28 + 1) + R"(",
          "transactionCode": "P",
          "securityTitle": "Common Stock",
          "transactionShares": 1000.0,
          "transactionPricePerShare": 175.0,
          "sharesOwnedFollowingTransaction": 10000.0
        })";
      }
      json_response += R"(],
        "total": {"value": 150, "relation": "eq"}
      })";
      return Expected<std::string>(json_response);
    }

    // Shouldn't get here
    return Expected<std::string>(R"({"data": [], "total": {"value": 0, "relation": "eq"}})");
  };

  InsiderTradingClient client(opts);

  SECTION("Paginate through 150 results (3 pages) - no aggregation") {
    // Use is_eod=false to see raw pagination results without daily aggregation
    auto result = client.getTransactionsDataFrame("AAPL", "2024-01-01", "2024-03-31",
                                                   std::nullopt, false);

    REQUIRE(result.has_value());
    auto df = result.value();

    // Verify all 150 results were fetched (no aggregation)
    REQUIRE(df.num_rows() == 150);

    // Verify 3 API calls were made (one per page)
    REQUIRE(call_count == 3);

    // Verify DataFrame has expected columns
    REQUIRE(df.num_cols() == 6);
  }

  SECTION("Paginate through 150 results with daily aggregation") {
    call_count = 0; // Reset counter

    // Use is_eod=true (default) to aggregate to daily
    auto result = client.getTransactionsDataFrame("AAPL", "2024-01-01", "2024-03-31");

    REQUIRE(result.has_value());
    auto df = result.value();

    // 150 transactions spanning Jan-March should aggregate to fewer days
    REQUIRE(df.num_rows() < 150);
    REQUIRE(df.num_rows() > 0);

    // Still 3 API calls for pagination
    REQUIRE(call_count == 3);
  }
}

TEST_CASE("Form13FClient - Pagination with multiple pages", "[sec][13f][pagination]") {
  Options opts;
  opts.api_key = "test_key";
  opts.enable_rate_limiting = false;

  int call_count = 0;

  opts.http_post_override = [&call_count](const std::string &path,
                                const std::string &body,
                                const std::vector<std::pair<std::string, std::string>> &query) {
    call_count++;

    // Parse the 'from' parameter
    size_t from_pos = body.find("\"from\": \"");
    int from_value = 0;
    if (from_pos != std::string::npos) {
      from_pos += 9;
      size_t end_pos = body.find("\"", from_pos);
      from_value = std::stoi(body.substr(from_pos, end_pos - from_pos));
    }

    // Simulate 120 total results (3 pages: 50, 50, 20)
    int results_this_page = 50;
    if (from_value == 100) {
      results_this_page = 20; // Last page has fewer results
    }

    std::string json_response = R"({"data": [)";
    for (int i = 0; i < results_this_page; i++) {
      if (i > 0) json_response += ",";
      int idx = from_value + i;
      json_response += R"({
        "filedAt": "2024-)" + std::to_string((idx / 30) + 1) + R"(-15T16:30:00-05:00",
        "periodOfReport": "2023-12-31",
        "cik": "106798)" + std::to_string(idx % 10) + R"(",
        "nameOfIssuer": "Apple Inc",
        "titleOfClass": "COM",
        "cusip": "037833100",
        "value": 1000000.0,
        "shares": 5000,
        "shOrPrn": "SH",
        "putOrCall": "",
        "investmentDiscretion": "SOLE",
        "votingAuthoritySole": "5000",
        "votingAuthorityShared": "0",
        "votingAuthorityNone": "0"
      })";
    }
    json_response += R"(],
      "total": {"value": 120, "relation": "eq"}
    })";
    return Expected<std::string>(json_response);
  };

  Form13FClient client(opts);

  SECTION("Paginate through 120 results (2.4 pages) - no aggregation") {
    // Use is_eod=false to see raw pagination results
    auto result = client.getHoldingsDataFrame("AAPL", "2024-01-01", "2024-12-31", false);

    REQUIRE(result.has_value());
    auto df = result.value();

    // Verify all 120 results were fetched (no aggregation)
    REQUIRE(df.num_rows() == 120);

    // Verify 3 API calls were made
    REQUIRE(call_count == 3);

    // Verify DataFrame structure
    REQUIRE(df.num_cols() == 6);
  }

  SECTION("Paginate through 120 results with daily aggregation") {
    call_count = 0; // Reset counter

    // Use is_eod=true (default) to aggregate
    auto result = client.getHoldingsDataFrame("AAPL", "2024-01-01", "2024-12-31");

    REQUIRE(result.has_value());
    auto df = result.value();

    // 120 holdings should aggregate to fewer days
    REQUIRE(df.num_rows() < 120);
    REQUIRE(df.num_rows() > 0);

    // Still 3 API calls
    REQUIRE(call_count == 3);
  }
}

TEST_CASE("InsiderTradingClient - Comma-separated string aggregation", "[sec][insider][aggregation]") {
  Options opts;
  opts.api_key = "test_key";
  opts.enable_rate_limiting = false;

  // Mock data with multiple transactions on the same day by different insiders
  opts.http_post_override = [](const std::string &path,
                                const std::string &body,
                                const std::vector<std::pair<std::string, std::string>> &query) {
    // Return 3 transactions on the same day (2024-01-15) with different owners and transaction codes
    std::string json_response = R"({
      "data": [
        {
          "filingUrl": "https://example.com/filing1",
          "filedAt": "2024-01-15T09:30:00-05:00",
          "issuerCik": "320193",
          "issuerName": "Apple Inc",
          "issuerTicker": "AAPL",
          "ownerCik": "1111111",
          "ownerName": "Alice Smith",
          "transactionDate": "2024-01-14",
          "transactionCode": "P",
          "securityTitle": "Common Stock",
          "transactionShares": 1000.0,
          "transactionPricePerShare": 175.0,
          "sharesOwnedFollowingTransaction": 10000.0
        },
        {
          "filingUrl": "https://example.com/filing2",
          "filedAt": "2024-01-15T14:45:00-05:00",
          "issuerCik": "320193",
          "issuerName": "Apple Inc",
          "issuerTicker": "AAPL",
          "ownerCik": "2222222",
          "ownerName": "Bob Johnson",
          "transactionDate": "2024-01-14",
          "transactionCode": "S",
          "securityTitle": "Common Stock",
          "transactionShares": 500.0,
          "transactionPricePerShare": 176.0,
          "sharesOwnedFollowingTransaction": 5000.0
        },
        {
          "filingUrl": "https://example.com/filing3",
          "filedAt": "2024-01-15T16:00:00-05:00",
          "issuerCik": "320193",
          "issuerName": "Apple Inc",
          "issuerTicker": "AAPL",
          "ownerCik": "3333333",
          "ownerName": "Charlie Brown",
          "transactionDate": "2024-01-13",
          "transactionCode": "P",
          "securityTitle": "Common Stock",
          "transactionShares": 2000.0,
          "transactionPricePerShare": 174.5,
          "sharesOwnedFollowingTransaction": 20000.0
        }
      ],
      "total": {"value": 3, "relation": "eq"}
    })";
    return Expected<std::string>(json_response);
  };

  InsiderTradingClient client(opts);

  SECTION("Aggregated strings are comma-separated") {
    auto result = client.getTransactionsDataFrame("AAPL", "2024-01-01", "2024-01-31");

    REQUIRE(result.has_value());
    auto df = result.value();

    // Should aggregate to 1 row (all filed on same day)
    REQUIRE(df.num_rows() == 1);

    // Check comma-separated owner names
    auto owner_col = df["owner_name"];
    auto owner_str = owner_col.iloc(0).repr();
    REQUIRE(owner_str.find(',') != std::string::npos); // Contains comma
    REQUIRE(owner_str.find("Alice Smith") != std::string::npos);
    REQUIRE(owner_str.find("Bob Johnson") != std::string::npos);
    REQUIRE(owner_str.find("Charlie Brown") != std::string::npos);

    // Check comma-separated transaction codes (P and S)
    auto code_col = df["transaction_code"];
    auto code_str = code_col.iloc(0).repr();
    REQUIRE(code_str.find("P") != std::string::npos);
    REQUIRE(code_str.find("S") != std::string::npos);

    // Check comma-separated transaction dates
    auto date_col = df["transaction_date"];
    auto date_str = date_col.iloc(0).repr();
    REQUIRE(date_str.find("2024-01-13") != std::string::npos);
    REQUIRE(date_str.find("2024-01-14") != std::string::npos);

    // Check numeric aggregation (sum of shares)
    auto shares_col = df["shares"];
    auto total_shares = shares_col.iloc(0).as_double();
    REQUIRE(total_shares == 3500.0); // 1000 + 500 + 2000

    // Check average price
    auto price_col = df["price"];
    auto avg_price = price_col.iloc(0).as_double();
    double expected_price = (175.0 + 176.0 + 174.5) / 3.0;
    REQUIRE(std::abs(avg_price - expected_price) < 0.001);
  }
}

TEST_CASE("InsiderTradingClient - Warn on 10k limit", "[sec][insider][pagination][10k]") {
  Options opts;
  opts.api_key = "test_key";
  opts.enable_rate_limiting = false;

  opts.http_post_override = [](const std::string &path,
                                const std::string &body,
                                const std::vector<std::pair<std::string, std::string>> &query) {
    // Simulate hitting the 10k API limit
    std::string json_response = R"({
      "data": [)";
    for (int i = 0; i < 50; i++) {
      if (i > 0) json_response += ",";
      json_response += R"({
        "filingUrl": "https://example.com/filing",
        "filedAt": "2024-01-01T10:00:00-05:00",
        "issuerCik": "320193",
        "issuerName": "Apple Inc",
        "issuerTicker": "AAPL",
        "ownerCik": "123456",
        "ownerName": "Test Insider",
        "transactionDate": "2024-01-01",
        "transactionCode": "P",
        "securityTitle": "Common Stock",
        "transactionShares": 1000.0,
        "transactionPricePerShare": 175.0,
        "sharesOwnedFollowingTransaction": 10000.0
      })";
    }
    json_response += R"(],
      "total": {"value": 10000, "relation": "gte"}
    })";
    return Expected<std::string>(json_response);
  };

  InsiderTradingClient client(opts);

  SECTION("Stop and warn when hitting 10k limit") {
    // Use is_eod=false to see raw results without aggregation
    auto result = client.getTransactionsDataFrame("AAPL", "2020-01-01", "2024-12-31",
                                                   std::nullopt, false);

    REQUIRE(result.has_value());
    auto df = result.value();

    // Should stop after first page when relation="gte"
    REQUIRE(df.num_rows() == 50);
  }
}
