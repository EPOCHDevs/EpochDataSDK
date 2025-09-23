#include "epoch_data_sdk/polygon/options.hpp"
#include "epoch_data_sdk/polygon/polygon_client.hpp"
#include <catch2/catch_all.hpp>
#include <epoch_frame/dataframe.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/series.h>

using namespace data_sdk::polygon;

static std::string getenv_or(const char *key,
                             const std::string &fallback = {}) {
  const char *v = ::getenv(key);
  return v ? std::string(v) : fallback;
}

TEST_CASE("getQuotes builds query with timestamp filters", "[polygon][rest]") {
  Options opt;
  opt.api_key = "test";
  std::string capturedPath;
  std::vector<std::pair<std::string, std::string>> capturedQuery;
  opt.http_get_override =
      [&](const std::string &path,
          const std::vector<std::pair<std::string, std::string>> &q) {
        capturedPath = path;
        capturedQuery = q;
        return std::expected<std::string, HttpError>(
            R"({"results":[],"status":"OK","request_id":"req"})");
      };

  PolygonClient cli(opt);
  auto res = cli.getQuotes("AAPL", "2021-01-01", "2021-01-02", 10,
                           std::string("asc"), std::string("timestamp"));
  REQUIRE(res.has_value());
  REQUIRE(capturedPath == "/v3/quotes/AAPL");
  // Verify presence of API query params
  auto has = [&](std::string k, std::string v) {
    return std::find(capturedQuery.begin(), capturedQuery.end(),
                     std::make_pair(k, v)) != capturedQuery.end();
  };
  REQUIRE(has("timestamp.gte", "2021-01-01"));
  REQUIRE(has("timestamp.lte", "2021-01-02"));
  REQUIRE(has("order", "asc"));
  REQUIRE(has("sort", "timestamp"));
  REQUIRE(has("limit", "10"));
}

TEST_CASE("getTrades builds query with timestamp filters", "[polygon][rest]") {
  Options opt;
  opt.api_key = "test";
  std::string capturedPath;
  std::vector<std::pair<std::string, std::string>> capturedQuery;
  opt.http_get_override =
      [&](const std::string &path,
          const std::vector<std::pair<std::string, std::string>> &q) {
        capturedPath = path;
        capturedQuery = q;
        return std::expected<std::string, HttpError>(
            R"({"results":[],"status":"OK","request_id":"req"})");
      };

  PolygonClient cli(opt);
  auto res = cli.getTrades("AAPL", "2021-01-01", "2021-01-02", 5,
                           std::string("asc"), std::string("timestamp"));
  REQUIRE(res.has_value());
  REQUIRE(capturedPath == "/v3/trades/AAPL");
  auto has = [&](std::string k, std::string v) {
    return std::find(capturedQuery.begin(), capturedQuery.end(),
                     std::make_pair(k, v)) != capturedQuery.end();
  };
  REQUIRE(has("timestamp.gte", "2021-01-01"));
  REQUIRE(has("timestamp.lte", "2021-01-02"));
  REQUIRE(has("order", "asc"));
  REQUIRE(has("sort", "timestamp"));
  REQUIRE(has("limit", "5"));
}

TEST_CASE("integration: real API call when POLYGON_API_KEY is set",
          "[polygon][rest][integration]") {
    auto api_key = getenv_or("POLYGON_API_KEY");
    if (api_key.empty()) {
        SKIP("POLYGON_API_KEY not set; skipping integration test");
    }

    Options opt;
    opt.api_key = api_key;
    // keep network timeout short
    opt.request_timeout_sec = 5.0;

    PolygonClient cli(opt);
    for (std::string const& asset : {"AAPL", "C:EURUSD", "X:BTCUSD", "^SPX"}) {
        SECTION(asset) {
            // A tiny query window to minimize data size
            auto df_eod = cli.getAggregates(asset, "2020-04-25", "2021-04-25", true);
            if (!df_eod.has_value()) {
                FAIL(df_eod.error().message);
            }
            INFO(df_eod->head().repr());
            REQUIRE(df_eod.has_value());

            auto df_minute = cli.getAggregates(asset, "2021-04-25", "2021-04-25", false);
            if (!df_minute.has_value()) {
                FAIL(df_minute.error().message);
            }
            INFO(df_minute->head().repr());
            REQUIRE(df_minute.has_value());

            auto dfq = cli.getQuotes(asset, "2021-04-25", "2021-04-25", 1);
            if (!dfq.has_value()) {
                FAIL(dfq.error().message);
            }
            INFO(dfq->head().repr());
            REQUIRE(dfq.has_value());

            auto dft = cli.getTrades(asset, "2021-04-25", "2021-04-25", 1);
            if (!dft.has_value()) {
                FAIL(dft.error().message);
            }
            INFO(dft->head().repr());
            REQUIRE(dft.has_value());
        }
    }
}


TEST_CASE("aggregates parses successfully", "[polygon][rest]") {
  Options opt;
  opt.api_key = "test";
  opt.http_get_override =
      [](const std::string &path,
         const std::vector<std::pair<std::string, std::string>> &) {
        REQUIRE(path.find(
                    "/v2/aggs/ticker/SPY/range/1/day/2020-01-01/2020-01-02") !=
                std::string::npos);
        std::string body = R"({
      "ticker":"SPY","adjusted":true,"status":"OK","request_id":"req",
      "results":[{"v":1000,"vw":320.1,"o":320.0,"c":321.0,"h":322.0,"l":319.5,"t":1577836800000,"n":123}]
    })";
        return std::expected<std::string, HttpError>(body);
      };

  PolygonClient cli(opt);
  auto res = cli.getAggregates("SPY", "2020-01-01", "2020-01-02", true);
  REQUIRE(res.has_value());
  // Expect a single row DataFrame with o,h,l,c,v columns
  REQUIRE(res->num_rows() == 1);
  REQUIRE(res->contains("c"));
  REQUIRE(res->operator[]("c").iloc(0).as_double() == Catch::Approx(321.0));
}
