#include "epoch_data_sdk/trading_economics/historical_indicators_client.hpp"
#include "epoch_data_sdk/trading_economics/options.hpp"
#include <catch2/catch_all.hpp>

using namespace data_sdk::trading_economics;

TEST_CASE("HistoricalIndicatorsClient::getHistoricalData with mock",
          "[trading_economics][historical]") {
  Options opt;
  opt.api_key = "test";
  opt.http_get_override =
      [](const std::string& path,
         const std::vector<std::pair<std::string, std::string>>& /*q*/) {
        REQUIRE(path.find("/historical/country/") != std::string::npos);
        std::string body = R"([
          {
            "Country": "United States",
            "Category": "GDP Growth Rate",
            "DateTime": "2024-01-01",
            "Value": 3.1
          },
          {
            "Country": "United States",
            "Category": "GDP Growth Rate",
            "DateTime": "2024-02-01",
            "Value": 3.3
          }
        ])";
        return std::expected<std::string, HttpError>(body);
      };

  HistoricalIndicatorsClient client(std::move(opt));
  auto res = client.getHistoricalData("United States", "GDP Growth Rate");

  REQUIRE(res.has_value());
  auto& df = *res;
  REQUIRE(df.num_rows() == 2);
  REQUIRE(df.has_column("Country"));
  REQUIRE(df.has_column("Category"));
  REQUIRE(df.has_column("DateTime"));
  REQUIRE(df.has_column("Value"));
}

TEST_CASE("HistoricalIndicatorsClient::getHistoricalByTicker with mock",
          "[trading_economics][historical]") {
  Options opt;
  opt.api_key = "test";
  opt.http_get_override =
      [](const std::string& path,
         const std::vector<std::pair<std::string, std::string>>& /*q*/) {
        REQUIRE(path.find("/historical/ticker/USURTOT") != std::string::npos);
        std::string body = R"([
          {
            "Symbol": "USURTOT",
            "Date": "2024-01-01",
            "Value": 3.7
          },
          {
            "Symbol": "USURTOT",
            "Date": "2024-02-01",
            "Value": 3.9
          }
        ])";
        return std::expected<std::string, HttpError>(body);
      };

  HistoricalIndicatorsClient client(std::move(opt));
  auto res = client.getHistoricalByTicker("USURTOT");

  REQUIRE(res.has_value());
  auto& df = *res;
  REQUIRE(df.num_rows() == 2);
  REQUIRE(df.has_column("Symbol"));
  REQUIRE(df.has_column("Date"));
  REQUIRE(df.has_column("Value"));
}

TEST_CASE("HistoricalIndicatorsClient handles HTTP error",
          "[trading_economics][historical]") {
  Options opt;
  opt.api_key = "test";
  opt.http_get_override =
      [](const std::string& /*path*/,
         const std::vector<std::pair<std::string, std::string>>& /*q*/) {
        HttpError err;
        err.http_status = 404;
        err.message = "Not found";
        return std::expected<std::string, HttpError>(std::unexpected(err));
      };

  HistoricalIndicatorsClient client(std::move(opt));
  auto res = client.getHistoricalData("Unknown", "Unknown Indicator");

  REQUIRE(!res.has_value());
  REQUIRE(res.error().http_status == 404);
}
