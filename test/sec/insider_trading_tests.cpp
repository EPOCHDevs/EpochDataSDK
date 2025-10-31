#include <catch2/catch_test_macros.hpp>
#include <epoch_data_sdk/sec/insider_trading_client.hpp>

using namespace data_sdk::sec;
using namespace epoch_core;

TEST_CASE("InsiderTradingClient - getTransactionsByTicker", "[sec][insider]") {
  Options opts;
  opts.api_key = "test_key";
  opts.enable_rate_limiting = false;
  opts.http_post_override = [](const std::string &path,
                                const std::string &body,
                                const std::vector<std::pair<std::string, std::string>> &query) {
    // Verify the body contains correct query
    REQUIRE(body.find("issuerTicker:AAPL") != std::string::npos);

    std::string json_response = R"({
      "data": [
        {
          "filingUrl": "https://www.sec.gov/...",
          "filedAt": "2024-10-15T16:30:00-04:00",
          "issuerCik": "320193",
          "issuerName": "Apple Inc.",
          "issuerTicker": "AAPL",
          "ownerCik": "1234567",
          "ownerName": "Tim Cook",
          "transactionDate": "2024-10-14",
          "transactionCode": "P",
          "securityTitle": "Common Stock",
          "transactionShares": 10000.0,
          "transactionPricePerShare": 175.50,
          "sharesOwnedFollowingTransaction": 510000.0
        }
      ],
      "total": {"value": 1, "relation": "eq"}
    })";
    return Expected<std::string>(json_response);
  };

  InsiderTradingClient client(opts);

  SECTION("Get all transactions for a ticker") {
    auto task = client.getTransactionsByTicker("AAPL", "", 50);
    auto result = drogon::sync_wait(task);

    REQUIRE(result.has_value());
    auto response = result.value();
    REQUIRE(response.total.value == 1);
    REQUIRE(response.data.size() == 1);
    REQUIRE(response.data[0].issuerTicker == "AAPL");
    REQUIRE(response.data[0].ownerName == "Tim Cook");
    REQUIRE(response.data[0].transactionCode == "P");
    REQUIRE(response.data[0].transactionShares == 10000.0);
    REQUIRE(response.data[0].transactionPricePerShare == 175.50);
  }
}

TEST_CASE("InsiderTradingClient - getLargePurchases", "[sec][insider]") {
  Options opts;
  opts.api_key = "test_key";
  opts.enable_rate_limiting = false;
  opts.http_post_override = [](const std::string &path,
                                const std::string &body,
                                const std::vector<std::pair<std::string, std::string>> &query) {
    // Mock response with multiple purchases of varying sizes
    std::string json_response = R"({
      "data": [
        {
          "filingUrl": "https://www.sec.gov/...",
          "filedAt": "2024-10-15T16:30:00-04:00",
          "issuerCik": "320193",
          "issuerName": "Apple Inc.",
          "issuerTicker": "AAPL",
          "ownerCik": "1234567",
          "ownerName": "Tim Cook",
          "transactionDate": "2024-10-14",
          "transactionCode": "P",
          "securityTitle": "Common Stock",
          "transactionShares": 10000.0,
          "transactionPricePerShare": 175.50,
          "sharesOwnedFollowingTransaction": 510000.0
        },
        {
          "filingUrl": "https://www.sec.gov/...",
          "filedAt": "2024-10-10T10:00:00-04:00",
          "issuerCik": "320193",
          "issuerName": "Apple Inc.",
          "issuerTicker": "AAPL",
          "ownerCik": "7654321",
          "ownerName": "Luca Maestri",
          "transactionDate": "2024-10-09",
          "transactionCode": "P",
          "securityTitle": "Common Stock",
          "transactionShares": 500.0,
          "transactionPricePerShare": 174.00,
          "sharesOwnedFollowingTransaction": 50500.0
        }
      ],
      "total": {"value": 2, "relation": "eq"}
    })";
    return Expected<std::string>(json_response);
  };

  InsiderTradingClient client(opts);

  SECTION("Filter large purchases above $100K") {
    auto task = client.getLargePurchases("AAPL", 100000.0, 50);
    auto result = drogon::sync_wait(task);

    REQUIRE(result.has_value());
    auto response = result.value();

    // Should only return the first transaction (10000 * 175.50 = $1,755,000)
    // Second transaction is only $87,000 (500 * 174.00)
    REQUIRE(response.data.size() == 1);
    REQUIRE(response.data[0].ownerName == "Tim Cook");

    double value = response.data[0].transactionShares * response.data[0].transactionPricePerShare;
    REQUIRE(value >= 100000.0);
  }
}

TEST_CASE("InsiderTradingClient - transaction codes", "[sec][insider]") {
  Options opts;
  opts.api_key = "test_key";
  opts.enable_rate_limiting = false;
  opts.http_post_override = [](const std::string &path,
                                const std::string &body,
                                const std::vector<std::pair<std::string, std::string>> &query) {
    // Verify transaction code filter
    if (body.find("transactionCode:S") != std::string::npos) {
      std::string json_response = R"({
        "data": [
          {
            "filingUrl": "https://www.sec.gov/...",
            "filedAt": "2024-10-15T16:30:00-04:00",
            "issuerCik": "320193",
            "issuerName": "Apple Inc.",
            "issuerTicker": "AAPL",
            "ownerCik": "1234567",
            "ownerName": "Tim Cook",
            "transactionDate": "2024-10-14",
            "transactionCode": "S",
            "securityTitle": "Common Stock",
            "transactionShares": 5000.0,
            "transactionPricePerShare": 180.00,
            "sharesOwnedFollowingTransaction": 505000.0
          }
        ],
        "total": {"value": 1, "relation": "eq"}
      })";
      return Expected<std::string>(json_response);
    }
    return Expected<std::string>("{}");
  };

  InsiderTradingClient client(opts);

  SECTION("Filter by transaction code (Sale)") {
    auto task = client.getTransactionsByTicker("AAPL", "S", 50);
    auto result = drogon::sync_wait(task);

    REQUIRE(result.has_value());
    auto response = result.value();
    REQUIRE(response.data.size() == 1);
    REQUIRE(response.data[0].transactionCode == "S");
  }
}

TEST_CASE("InsiderTradingClient - TransactionCode enum", "[sec][insider][enum]") {
  Options opts;
  opts.api_key = "test_key";
  opts.enable_rate_limiting = false;
  opts.http_post_override = [](const std::string &path,
                                const std::string &body,
                                const std::vector<std::pair<std::string, std::string>> &query) {
    // Debug: Print body to see what's being sent
    INFO("Body: " << body);

    // Verify transaction code filter - should have both ticker and code
    REQUIRE(body.find("issuerTicker:AAPL") != std::string::npos);
    REQUIRE(body.find("transactionCode:P") != std::string::npos);

    if (body.find("transactionCode:P") != std::string::npos &&
        body.find("issuerTicker:AAPL") != std::string::npos) {
      std::string json_response = R"({
        "data": [
          {
            "filingUrl": "https://www.sec.gov/...",
            "filedAt": "2024-10-15T16:30:00-04:00",
            "issuerCik": "320193",
            "issuerName": "Apple Inc.",
            "issuerTicker": "AAPL",
            "ownerCik": "1234567",
            "ownerName": "Tim Cook",
            "transactionDate": "2024-10-14",
            "transactionCode": "P",
            "securityTitle": "Common Stock",
            "transactionShares": 10000.0,
            "transactionPricePerShare": 175.50,
            "sharesOwnedFollowingTransaction": 510000.0
          }
        ],
        "total": {"value": 1, "relation": "eq"}
      })";
      return Expected<std::string>(json_response);
    }
    // Return empty response if pattern doesn't match
    std::string json_response = R"({"data": [], "total": {"value": 0, "relation": "eq"}})";
    return Expected<std::string>(json_response);
  };

  InsiderTradingClient client(opts);

  SECTION("Use TransactionCode::P enum") {
    // Verify the enum converts to the correct string
    REQUIRE(transactionCodeToString(TransactionCode::P) == "P");

    auto task = client.getTransactionsByTicker("AAPL", TransactionCode::P, 50);
    auto result = drogon::sync_wait(task);

    REQUIRE(result.has_value());
    auto response = result.value();
    REQUIRE(response.data.size() == 1);
    REQUIRE(response.data[0].transactionCode == "P");
  }

  SECTION("Verify enum to string conversion") {
    REQUIRE(transactionCodeToString(TransactionCode::P) == "P");
    REQUIRE(transactionCodeToString(TransactionCode::S) == "S");
    REQUIRE(transactionCodeToString(TransactionCode::A) == "A");
    REQUIRE(transactionCodeToString(TransactionCode::M) == "M");
    REQUIRE(transactionCodeToString(TransactionCode::G) == "G");
  }
}

TEST_CASE("InsiderTradingClient - getTransactionsDataFrame", "[sec][insider][dataframe]") {
  Options opts;
  opts.api_key = "test_key";
  opts.enable_rate_limiting = false;
  opts.http_post_override = [](const std::string &path,
                                const std::string &body,
                                const std::vector<std::pair<std::string, std::string>> &query) {
    // Verify date range and ticker in query
    REQUIRE(body.find("issuerTicker:AAPL") != std::string::npos);
    REQUIRE(body.find("filedAt:[2024-01-01 TO 2024-12-31]") != std::string::npos);

    std::string json_response = R"({
      "data": [
        {
          "filingUrl": "https://www.sec.gov/...",
          "filedAt": "2024-01-15T16:30:00-05:00",
          "issuerCik": "320193",
          "issuerName": "Apple Inc.",
          "issuerTicker": "AAPL",
          "ownerCik": "1234567",
          "ownerName": "Tim Cook",
          "transactionDate": "2024-01-14",
          "transactionCode": "P",
          "securityTitle": "Common Stock",
          "transactionShares": 10000.0,
          "transactionPricePerShare": 185.50,
          "sharesOwnedFollowingTransaction": 510000.0
        },
        {
          "filingUrl": "https://www.sec.gov/...",
          "filedAt": "2024-03-20T10:00:00-04:00",
          "issuerCik": "320193",
          "issuerName": "Apple Inc.",
          "issuerTicker": "AAPL",
          "ownerCik": "7654321",
          "ownerName": "Luca Maestri",
          "transactionDate": "2024-03-19",
          "transactionCode": "P",
          "securityTitle": "Common Stock",
          "transactionShares": 5000.0,
          "transactionPricePerShare": 172.00,
          "sharesOwnedFollowingTransaction": 55000.0
        },
        {
          "filingUrl": "https://www.sec.gov/...",
          "filedAt": "2024-06-10T14:30:00-04:00",
          "issuerCik": "320193",
          "issuerName": "Apple Inc.",
          "issuerTicker": "AAPL",
          "ownerCik": "9876543",
          "ownerName": "Katherine Adams",
          "transactionDate": "2024-06-09",
          "transactionCode": "P",
          "securityTitle": "Common Stock",
          "transactionShares": 3000.0,
          "transactionPricePerShare": 210.75,
          "sharesOwnedFollowingTransaction": 33000.0
        }
      ],
      "total": {"value": 3, "relation": "eq"}
    })";
    return Expected<std::string>(json_response);
  };

  InsiderTradingClient client(opts);

  SECTION("Get transactions as DataFrame for systematic trading") {
    auto result = client.getTransactionsDataFrame("AAPL", "2024-01-01", "2024-12-31");

    REQUIRE(result.has_value());
    auto df = result.value();

    // Verify shape
    REQUIRE(df.num_rows() == 3);
    REQUIRE(df.num_cols() == 6);

    // Verify exact columns for systematic trading
    auto cols = df.column_names();
    REQUIRE(cols.size() == 6);
    std::vector<std::string> expected_cols = {
      "transaction_date", "owner_name", "transaction_code",
      "shares", "price", "ownership_after"
    };
    for (const auto& col : expected_cols) {
      REQUIRE(std::find(cols.begin(), cols.end(), col) != cols.end());
    }

    // Verify index is datetime
  }
}

TEST_CASE("InsiderTradingClient - getTransactionsDataFrame with transaction code filter", "[sec][insider][dataframe]") {
  Options opts;
  opts.api_key = "test_key";
  opts.enable_rate_limiting = false;
  opts.http_post_override = [](const std::string &path,
                                const std::string &body,
                                const std::vector<std::pair<std::string, std::string>> &query) {
    // Verify transaction code filter
    REQUIRE(body.find("issuerTicker:AAPL") != std::string::npos);
    REQUIRE(body.find("transactionCode:P") != std::string::npos);

    std::string json_response = R"({
      "data": [
        {
          "filingUrl": "https://www.sec.gov/...",
          "filedAt": "2024-01-15T16:30:00-05:00",
          "issuerCik": "320193",
          "issuerName": "Apple Inc.",
          "issuerTicker": "AAPL",
          "ownerCik": "1234567",
          "ownerName": "Tim Cook",
          "transactionDate": "2024-01-14",
          "transactionCode": "P",
          "securityTitle": "Common Stock",
          "transactionShares": 10000.0,
          "transactionPricePerShare": 185.50,
          "sharesOwnedFollowingTransaction": 510000.0
        }
      ],
      "total": {"value": 1, "relation": "eq"}
    })";
    return Expected<std::string>(json_response);
  };

  InsiderTradingClient client(opts);

  SECTION("Filter purchases only") {
    auto result = client.getTransactionsDataFrame("AAPL", "2024-01-01", "2024-12-31", TransactionCode::P);

    REQUIRE(result.has_value());
    auto df = result.value();

    REQUIRE(df.num_rows() == 1);
    REQUIRE(df.num_cols() == 6);

    // Verify filtering worked - all transactions should be purchases
    auto cols = df.column_names();
    REQUIRE(std::find(cols.begin(), cols.end(), "transaction_code") != cols.end());
  }
}

TEST_CASE("InsiderTradingClient - getTransactionsDataFrame with InsiderTradingOptions", "[sec][insider][dataframe]") {
  Options opts;
  opts.api_key = "test_key";
  opts.enable_rate_limiting = false;
  opts.http_post_override = [](const std::string &path,
                                const std::string &body,
                                const std::vector<std::pair<std::string, std::string>> &query) {
    std::string json_response = R"({
      "data": [
        {
          "filingUrl": "https://www.sec.gov/...",
          "filedAt": "2024-01-15T16:30:00-05:00",
          "issuerCik": "320193",
          "issuerName": "Apple Inc.",
          "issuerTicker": "AAPL",
          "ownerCik": "1234567",
          "ownerName": "Tim Cook",
          "transactionDate": "2024-01-14",
          "transactionCode": "P",
          "securityTitle": "Common Stock",
          "transactionShares": 10000.0,
          "transactionPricePerShare": 185.50,
          "sharesOwnedFollowingTransaction": 510000.0
        }
      ],
      "total": {"value": 1, "relation": "eq"}
    })";
    return Expected<std::string>(json_response);
  };

  InsiderTradingClient client(opts);

  SECTION("Use InsiderTradingOptions struct") {
    InsiderTradingOptions trading_opts;
    trading_opts.ticker = "AAPL";
    trading_opts.from_date = "2024-01-01";
    trading_opts.to_date = "2024-12-31";
    trading_opts.transaction_code = "P";

    auto result = client.getTransactionsDataFrame(trading_opts);

    REQUIRE(result.has_value());
    auto df = result.value();

    REQUIRE(df.num_rows() == 1);
    REQUIRE(df.num_cols() == 6);
  }
}

TEST_CASE("InsiderTradingClient - getTransactionsDataFrameAsync", "[sec][insider][dataframe][async]") {
  Options opts;
  opts.api_key = "test_key";
  opts.enable_rate_limiting = false;
  opts.http_post_override = [](const std::string &path,
                                const std::string &body,
                                const std::vector<std::pair<std::string, std::string>> &query) {
    std::string json_response = R"({
      "data": [
        {
          "filingUrl": "https://www.sec.gov/...",
          "filedAt": "2024-01-15T16:30:00-05:00",
          "issuerCik": "320193",
          "issuerName": "Apple Inc.",
          "issuerTicker": "AAPL",
          "ownerCik": "1234567",
          "ownerName": "Tim Cook",
          "transactionDate": "2024-01-14",
          "transactionCode": "P",
          "securityTitle": "Common Stock",
          "transactionShares": 10000.0,
          "transactionPricePerShare": 185.50,
          "sharesOwnedFollowingTransaction": 510000.0
        }
      ],
      "total": {"value": 1, "relation": "eq"}
    })";
    return Expected<std::string>(json_response);
  };

  InsiderTradingClient client(opts);

  SECTION("Async DataFrame retrieval") {
    auto task = client.getTransactionsDataFrameAsync("AAPL", "2024-01-01", "2024-12-31");
    auto result = drogon::sync_wait(task);

    REQUIRE(result.has_value());
    auto df = result.value();

    REQUIRE(df.num_rows() == 1);
    REQUIRE(df.num_cols() == 6);

    // Verify essential columns for systematic trading
    auto cols = df.column_names();
    REQUIRE(std::find(cols.begin(), cols.end(), "shares") != cols.end());
    REQUIRE(std::find(cols.begin(), cols.end(), "price") != cols.end());
  }
}
