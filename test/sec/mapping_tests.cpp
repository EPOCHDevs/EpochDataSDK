#include <catch2/catch_test_macros.hpp>
#include <epoch_data_sdk/sec/mapping_client.hpp>

using namespace data_sdk::sec;

TEST_CASE("MappingClient - resolveByTicker", "[sec][mapping]") {
  Options opts;
  opts.api_key = "test_key";
  opts.enable_rate_limiting = false;
  opts.http_get_override = [](const std::string &path,
                               const std::vector<std::pair<std::string, std::string>> &query) {
    std::string json_response = R"([{
      "cik": "320193",
      "ticker": "AAPL",
      "name": "Apple Inc.",
      "exchange": "NASDAQ",
      "cusip": "037833100",
      "sector": "Technology",
      "industry": "Consumer Electronics",
      "sic": "3571"
    }])";
    return Expected<std::string>(json_response);
  };

  MappingClient client(opts);

  SECTION("Resolve ticker to CIK") {
    auto task = client.resolveByTicker("AAPL");
    auto result = drogon::sync_wait(task);

    REQUIRE(result.has_value());
    auto mapping = result.value();
    REQUIRE(mapping.ticker == "AAPL");
    REQUIRE(mapping.cik == "320193");
    REQUIRE(mapping.name == "Apple Inc.");
    REQUIRE(mapping.cusip == "037833100");
  }
}

TEST_CASE("MappingClient - resolveByCIK", "[sec][mapping]") {
  Options opts;
  opts.api_key = "test_key";
  opts.enable_rate_limiting = false;
  opts.http_get_override = [](const std::string &path,
                               const std::vector<std::pair<std::string, std::string>> &query) {
    std::string json_response = R"([{
      "cik": "789019",
      "ticker": "MSFT",
      "name": "Microsoft Corporation",
      "exchange": "NASDAQ",
      "cusip": "594918104",
      "sector": "Technology",
      "industry": "Software",
      "sic": "7372"
    }])";
    return Expected<std::string>(json_response);
  };

  MappingClient client(opts);

  SECTION("Resolve CIK to ticker") {
    auto task = client.resolveByCIK("789019");
    auto result = drogon::sync_wait(task);

    REQUIRE(result.has_value());
    auto mapping = result.value();
    REQUIRE(mapping.ticker == "MSFT");
    REQUIRE(mapping.cik == "789019");
  }
}
