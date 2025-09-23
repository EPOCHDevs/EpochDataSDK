#include "epoch_data_sdk/tradingeconomics/options.hpp"
#include "epoch_data_sdk/tradingeconomics/tradingeconomics_client.hpp"
#include <catch2/catch_all.hpp>

using namespace data_sdk::tradingeconomics;

TEST_CASE("historical series parses successfully", "[tradingeconomics][rest]") {
  Options opt;
  opt.api_key = "test";
  opt.http_get_override =
      [](const std::string &path,
         const std::vector<std::pair<std::string, std::string>> & /*q*/) {
        REQUIRE(path.find("/historical/series/USURTOT") != std::string::npos);
        std::string body = R"([
      {
        "Symbol": "USURTOT",
        "Category": "Unemployment Rate",
        "Country": "United States",
        "HistoricalData": [
          {"Date": "2024-01-01", "Value": 3.7},
          {"Date": "2024-02-01", "Value": 3.9}
        ]
      }
    ])";
        return std::expected<std::string, HttpError>(body);
      };

  TradingEconomicsClient cli(opt);
  auto res = cli.getHistoricalSeries("USURTOT");
  REQUIRE(res.has_value());
  REQUIRE(!res->empty());
  REQUIRE(res->front().Symbol == "USURTOT");
  REQUIRE(res->front().HistoricalData.size() == 2);
  REQUIRE(res->front().HistoricalData[1].Value == Catch::Approx(3.9));
}

TEST_CASE("calendar parses successfully", "[tradingeconomics][rest]") {
  Options opt;
  opt.api_key = "test";
  opt.http_get_override =
      [](const std::string &path,
         const std::vector<std::pair<std::string, std::string>> & /*q*/) {
        REQUIRE(path.find("/calendar") != std::string::npos);
        std::string body = R"([
      {"Country":"United States","Category":"Inflation Rate","Date":"2024-06-12","Actual":3.3,"Previous":3.4,"Forecast":3.4,"Unit":"%"}
    ])";
        return std::expected<std::string, HttpError>(body);
      };

  TradingEconomicsClient cli(opt);
  auto res = cli.getCalendar("United States", "Inflation Rate");
  REQUIRE(res.has_value());
  REQUIRE(!res->empty());
  REQUIRE(res->front().Country == "United States");
  REQUIRE(res->front().Category == "Inflation Rate");
  REQUIRE(res->front().Actual.has_value());
}
