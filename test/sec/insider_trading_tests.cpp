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
      "total": 1
    })";
    return Expected<std::string>(json_response);
  };

  InsiderTradingClient client(opts);

  SECTION("Get all transactions for a ticker") {
    auto task = client.getTransactionsByTicker("AAPL", "", 50);
    auto result = drogon::sync_wait(task);

    REQUIRE(result.has_value());
    auto response = result.value();
    REQUIRE(response.total == 1);
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
      "total": 2
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
        "total": 1
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
        "total": 1
      })";
      return Expected<std::string>(json_response);
    }
    // Return empty response if pattern doesn't match
    std::string json_response = R"({"data": [], "total": 0})";
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
