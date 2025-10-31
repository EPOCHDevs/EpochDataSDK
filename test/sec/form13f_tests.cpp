#include <catch2/catch_test_macros.hpp>
#include <epoch_data_sdk/sec/form13f_client.hpp>

using namespace data_sdk::sec;

TEST_CASE("Form13FClient - getHoldersByTicker", "[sec][13f]") {
  Options opts;
  opts.api_key = "test_key";
  opts.enable_rate_limiting = false;
  opts.http_post_override = [](const std::string &path,
                                const std::string &body,
                                const std::vector<std::pair<std::string, std::string>> &query) {
    std::string json_response = R"({
      "data": [
        {
          "nameOfIssuer": "Apple Inc",
          "titleOfClass": "COM",
          "cusip": "037833100",
          "value": 70000000000.0,
          "shares": 400000000,
          "shOrPrn": "SH",
          "putOrCall": "",
          "investmentDiscretion": "SOLE",
          "votingAuthoritySole": "400000000",
          "votingAuthorityShared": "0",
          "votingAuthorityNone": "0"
        }
      ],
      "total": 1
    })";
    return Expected<std::string>(json_response);
  };

  Form13FClient client(opts);

  SECTION("Get holders for a ticker") {
    auto task = client.getHoldersByTicker("AAPL", 100);
    auto result = drogon::sync_wait(task);

    REQUIRE(result.has_value());
    auto response = result.value();
    REQUIRE(response.total == 1);
    REQUIRE(response.data.size() == 1);
    REQUIRE(response.data[0].cusip == "037833100");
    REQUIRE(response.data[0].shares == 400000000);
    REQUIRE(response.data[0].value == 70000000000.0);
  }
}

TEST_CASE("Form13FClient - getHoldersByCUSIP", "[sec][13f]") {
  Options opts;
  opts.api_key = "test_key";
  opts.enable_rate_limiting = false;
  opts.http_post_override = [](const std::string &path,
                                const std::string &body,
                                const std::vector<std::pair<std::string, std::string>> &query) {
    // Verify CUSIP in query
    REQUIRE(body.find("cusip:037833100") != std::string::npos);

    std::string json_response = R"({
      "data": [
        {
          "nameOfIssuer": "Apple Inc",
          "titleOfClass": "COM",
          "cusip": "037833100",
          "value": 70000000000.0,
          "shares": 400000000,
          "shOrPrn": "SH",
          "putOrCall": "",
          "investmentDiscretion": "SOLE",
          "votingAuthoritySole": "400000000",
          "votingAuthorityShared": "0",
          "votingAuthorityNone": "0"
        },
        {
          "nameOfIssuer": "Apple Inc",
          "titleOfClass": "COM",
          "cusip": "037833100",
          "value": 25000000000.0,
          "shares": 142857143,
          "shOrPrn": "SH",
          "putOrCall": "",
          "investmentDiscretion": "SOLE",
          "votingAuthoritySole": "142857143",
          "votingAuthorityShared": "0",
          "votingAuthorityNone": "0"
        }
      ],
      "total": 2
    })";
    return Expected<std::string>(json_response);
  };

  Form13FClient client(opts);

  SECTION("Get holders by CUSIP") {
    auto task = client.getHoldersByCUSIP("037833100", 100);
    auto result = drogon::sync_wait(task);

    REQUIRE(result.has_value());
    auto response = result.value();
    REQUIRE(response.total == 2);
    REQUIRE(response.data.size() == 2);

    // Should be sorted by value descending
    REQUIRE(response.data[0].value >= response.data[1].value);
  }
}

TEST_CASE("Form13FClient - getLargePositions", "[sec][13f]") {
  Options opts;
  opts.api_key = "test_key";
  opts.enable_rate_limiting = false;
  opts.http_post_override = [](const std::string &path,
                                const std::string &body,
                                const std::vector<std::pair<std::string, std::string>> &query) {
    // Mock response with positions of varying sizes
    std::string json_response = R"({
      "data": [
        {
          "nameOfIssuer": "Apple Inc",
          "titleOfClass": "COM",
          "cusip": "037833100",
          "value": 70000000000.0,
          "shares": 400000000,
          "shOrPrn": "SH",
          "putOrCall": "",
          "investmentDiscretion": "SOLE",
          "votingAuthoritySole": "400000000",
          "votingAuthorityShared": "0",
          "votingAuthorityNone": "0"
        },
        {
          "nameOfIssuer": "Apple Inc",
          "titleOfClass": "COM",
          "cusip": "037833100",
          "value": 5000000.0,
          "shares": 28571,
          "shOrPrn": "SH",
          "putOrCall": "",
          "investmentDiscretion": "SOLE",
          "votingAuthoritySole": "28571",
          "votingAuthorityShared": "0",
          "votingAuthorityNone": "0"
        },
        {
          "nameOfIssuer": "Apple Inc",
          "titleOfClass": "COM",
          "cusip": "037833100",
          "value": 25000000000.0,
          "shares": 142857143,
          "shOrPrn": "SH",
          "putOrCall": "",
          "investmentDiscretion": "SOLE",
          "votingAuthoritySole": "142857143",
          "votingAuthorityShared": "0",
          "votingAuthorityNone": "0"
        }
      ],
      "total": 3
    })";
    return Expected<std::string>(json_response);
  };

  Form13FClient client(opts);

  SECTION("Filter large positions above $10M") {
    auto task = client.getLargePositions("AAPL", 10000000.0, 50);
    auto result = drogon::sync_wait(task);

    REQUIRE(result.has_value());
    auto response = result.value();

    // Should filter out the $5M position, keeping only $70B and $25B positions
    REQUIRE(response.data.size() == 2);

    for (const auto &holding : response.data) {
      REQUIRE(holding.value >= 10000000.0);
    }
  }
}

TEST_CASE("Form13FClient - getHoldingsByInstitution", "[sec][13f]") {
  Options opts;
  opts.api_key = "test_key";
  opts.enable_rate_limiting = false;
  opts.http_post_override = [](const std::string &path,
                                const std::string &body,
                                const std::vector<std::pair<std::string, std::string>> &query) {
    // Verify institution CIK in query
    REQUIRE(body.find("cik:1067983") != std::string::npos);

    std::string json_response = R"({
      "data": [
        {
          "nameOfIssuer": "Apple Inc",
          "titleOfClass": "COM",
          "cusip": "037833100",
          "value": 70000000000.0,
          "shares": 400000000,
          "shOrPrn": "SH",
          "putOrCall": "",
          "investmentDiscretion": "SOLE",
          "votingAuthoritySole": "400000000",
          "votingAuthorityShared": "0",
          "votingAuthorityNone": "0"
        },
        {
          "nameOfIssuer": "Coca Cola Co",
          "titleOfClass": "COM",
          "cusip": "191216100",
          "value": 25000000000.0,
          "shares": 400000000,
          "shOrPrn": "SH",
          "putOrCall": "",
          "investmentDiscretion": "SOLE",
          "votingAuthoritySole": "400000000",
          "votingAuthorityShared": "0",
          "votingAuthorityNone": "0"
        }
      ],
      "total": 2
    })";
    return Expected<std::string>(json_response);
  };

  Form13FClient client(opts);

  SECTION("Get Berkshire Hathaway's holdings") {
    auto task = client.getHoldingsByInstitution("1067983", 100);
    auto result = drogon::sync_wait(task);

    REQUIRE(result.has_value());
    auto response = result.value();
    REQUIRE(response.total == 2);
    REQUIRE(response.data.size() == 2);

    // Should have AAPL and KO
    REQUIRE(response.data[0].cusip == "037833100");  // AAPL
    REQUIRE(response.data[1].cusip == "191216100");  // KO
  }
}
