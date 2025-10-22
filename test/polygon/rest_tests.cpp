#include "epoch_data_sdk/polygon/options.hpp"
#include "epoch_data_sdk/polygon/aggs_client.hpp"
#include "epoch_data_sdk/polygon/quotes_client.hpp"
#include "epoch_data_sdk/polygon/trades_client.hpp"
#include "epoch_data_sdk/polygon/financials_client.hpp"
#include "epoch_data_sdk/polygon/short_volume_client.hpp"
#include "epoch_data_sdk/polygon/short_interest_client.hpp"
#include "epoch_data_sdk/polygon/ipo_client.hpp"
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

  QuotesClient cli(opt);
  auto res = cli.getQuotes("AAPL", "2021-01-01", "2021-01-02", 10);
  REQUIRE(res.has_value());
  REQUIRE(capturedPath == "/v3/quotes/AAPL");
  // Verify presence of API query params
  auto has = [&](std::string k, std::string v) {
    return std::find(capturedQuery.begin(), capturedQuery.end(),
                     std::make_pair(k, v)) != capturedQuery.end();
  };
  REQUIRE(has("timestamp.gte", "2021-01-01"));
  REQUIRE(has("timestamp.lte", "2021-01-02"));
  REQUIRE(has("order", "asc"));  // Now always enforced
  REQUIRE(has("sort", "timestamp"));  // Now always enforced
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

  TradesClient cli(opt);
  auto res = cli.getTrades("AAPL", "2021-01-01", "2021-01-02", 5);
  REQUIRE(res.has_value());
  REQUIRE(capturedPath == "/v3/trades/AAPL");
  auto has = [&](std::string k, std::string v) {
    return std::find(capturedQuery.begin(), capturedQuery.end(),
                     std::make_pair(k, v)) != capturedQuery.end();
  };
  REQUIRE(has("timestamp.gte", "2021-01-01"));
  REQUIRE(has("timestamp.lte", "2021-01-02"));
  REQUIRE(has("order", "asc"));  // Now always enforced
  REQUIRE(has("sort", "timestamp"));  // Now always enforced
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

    AggsClient aggs_cli(opt);
    QuotesClient quotes_cli(opt);
    TradesClient trades_cli(opt);

    for (std::string const& asset : {"AAPL", "C:EURUSD", "X:BTCUSD"}) {
        SECTION(asset) {
            // A tiny query window to minimize data size
            auto df_eod = aggs_cli.getAggregates(asset, "2020-04-25", "2021-04-25", true);
            if (!df_eod.has_value()) {
                FAIL(df_eod.error().message);
            }
            INFO(df_eod->head().repr());
            REQUIRE(df_eod.has_value());
            REQUIRE(df_eod->num_rows() > 0);
            // Aggregates required columns
            REQUIRE(df_eod->contains("o"));
            REQUIRE(df_eod->contains("h"));
            REQUIRE(df_eod->contains("l"));
            REQUIRE(df_eod->contains("c"));
            REQUIRE(df_eod->contains("v"));

            // Only test minute-level aggregates, quotes, and trades for stocks
            // FX and crypto have limited historical granular data availability
            if (asset == "AAPL") {
                auto df_minute = aggs_cli.getAggregates(asset, "2021-04-26", "2021-04-26", false);
                if (!df_minute.has_value()) {
                    FAIL(df_minute.error().message);
                }
                INFO(df_minute->head().repr());
                REQUIRE(df_minute.has_value());
                REQUIRE(df_minute->num_rows() > 0);
                // Aggregates required columns
                REQUIRE(df_minute->contains("o"));
                REQUIRE(df_minute->contains("h"));
                REQUIRE(df_minute->contains("l"));
                REQUIRE(df_minute->contains("c"));
                REQUIRE(df_minute->contains("v"));

                auto dfq = quotes_cli.getQuotes(asset, "2021-04-26", "2021-04-26", 1);
                if (!dfq.has_value()) {
                    FAIL(dfq.error().message);
                }
                INFO(dfq->head().repr());
                REQUIRE(dfq.has_value());
                REQUIRE(dfq->num_rows() > 0);
                // Quotes expected columns (at minimum ask/bid prices)
                REQUIRE(dfq->contains("ap"));
                REQUIRE(dfq->contains("bp"));

                auto dft = trades_cli.getTrades(asset, "2021-04-26", "2021-04-26", 1);
                if (!dft.has_value()) {
                    FAIL(dft.error().message);
                }
                INFO(dft->head().repr());
                REQUIRE(dft.has_value());
                REQUIRE(dft->num_rows() > 0);
                // Trades expected columns (at minimum price and size)
                REQUIRE(dft->contains("p"));
                REQUIRE(dft->contains("s"));
            }
        }
    }
}

TEST_CASE("integration: financials API calls", "[polygon][rest][integration]") {
    auto api_key = getenv_or("POLYGON_API_KEY");
    if (api_key.empty()) {
        SKIP("POLYGON_API_KEY not set; skipping integration test");
    }

    Options opt;
    opt.api_key = api_key;
    opt.request_timeout_sec = 5.0;

    FinancialsClient financials_cli(opt);

    SECTION("Balance Sheets") {
        auto df = financials_cli.getBalanceSheets("AAPL", "2020-01-01", "2023-12-31", 10);
        if (!df.has_value()) {
            FAIL(df.error().message);
        }
        INFO(df->head().repr());
        REQUIRE(df.has_value());
        REQUIRE(df->num_rows() > 0);
        // Check required columns
        REQUIRE(df->contains("ticker"));
        REQUIRE(df->contains("period_end"));
        REQUIRE(df->contains("fiscal_year"));
        REQUIRE(df->contains("fiscal_quarter"));
        REQUIRE(df->contains("cash"));
        REQUIRE(df->contains("lt_debt"));
    }

    SECTION("Cash Flow Statements") {
        auto df = financials_cli.getCashFlowStatements("AAPL", "2020-01-01", "2023-12-31", 10);
        if (!df.has_value()) {
            FAIL(df.error().message);
        }
        INFO(df->head().repr());
        REQUIRE(df.has_value());
        REQUIRE(df->num_rows() > 0);
        // Check required columns
        REQUIRE(df->contains("ticker"));
        REQUIRE(df->contains("period_end"));
        REQUIRE(df->contains("cfo"));
        REQUIRE(df->contains("capex"));
        REQUIRE(df->contains("ncf_operating"));
    }

    SECTION("Income Statements") {
        auto df = financials_cli.getIncomeStatements("AAPL", "2020-01-01", "2023-12-31", 10);
        if (!df.has_value()) {
            FAIL(df.error().message);
        }
        INFO(df->head().repr());
        REQUIRE(df.has_value());
        REQUIRE(df->num_rows() > 0);
        // Check required columns
        REQUIRE(df->contains("ticker"));
        REQUIRE(df->contains("period_end"));
        REQUIRE(df->contains("revenue"));
        REQUIRE(df->contains("net_income"));
        REQUIRE(df->contains("basic_eps"));
    }
}

TEST_CASE("integration: short volume API call", "[polygon][rest][integration]") {
    auto api_key = getenv_or("POLYGON_API_KEY");
    if (api_key.empty()) {
        SKIP("POLYGON_API_KEY not set; skipping integration test");
    }

    Options opt;
    opt.api_key = api_key;
    opt.request_timeout_sec = 5.0;

    ShortVolumeClient short_volume_cli(opt);

    auto df = short_volume_cli.getShortVolume("AAPL", "2024-01-01", "2024-12-31", 10);
    if (!df.has_value()) {
        FAIL(df.error().message);
    }
    INFO(df->head().repr());
    REQUIRE(df.has_value());
    REQUIRE(df->num_rows() > 0);
    // Check required columns
    REQUIRE(df->contains("ticker"));
    REQUIRE(df->contains("short_volume"));
    REQUIRE(df->contains("total_volume"));
    REQUIRE(df->contains("short_volume_ratio"));
}

TEST_CASE("integration: short interest API call", "[polygon][rest][integration]") {
    auto api_key = getenv_or("POLYGON_API_KEY");
    if (api_key.empty()) {
        SKIP("POLYGON_API_KEY not set; skipping integration test");
    }

    Options opt;
    opt.api_key = api_key;
    opt.request_timeout_sec = 5.0;

    ShortInterestClient short_interest_cli(opt);

    SECTION("With pagination (limit=2)") {
        // Use small limit to force pagination
        auto df = short_interest_cli.getShortInterest("AAPL", "2020-01-01", "2024-12-31", 2);
        if (!df.has_value()) {
            FAIL(df.error().message);
        }
        INFO(df->head().repr());
        REQUIRE(df.has_value());
        REQUIRE(df->num_rows() > 2); // Should have fetched multiple pages
        // Check required columns
        REQUIRE(df->contains("ticker"));
        REQUIRE(df->contains("short_interest"));
        REQUIRE(df->contains("avg_daily_volume"));
        REQUIRE(df->contains("days_to_cover"));
    }

    SECTION("Normal query") {
        auto df = short_interest_cli.getShortInterest("AAPL", "2024-01-01", "2024-12-31", 10);
        if (!df.has_value()) {
            FAIL(df.error().message);
        }
        INFO(df->head().repr());
        REQUIRE(df.has_value());
        REQUIRE(df->num_rows() > 0);
        // Check required columns
        REQUIRE(df->contains("ticker"));
        REQUIRE(df->contains("short_interest"));
        REQUIRE(df->contains("avg_daily_volume"));
        REQUIRE(df->contains("days_to_cover"));
    }
}

TEST_CASE("integration: IPO API call", "[polygon][rest][integration]") {
    auto api_key = getenv_or("POLYGON_API_KEY");
    if (api_key.empty()) {
        SKIP("POLYGON_API_KEY not set; skipping integration test");
    }

    Options opt;
    opt.api_key = api_key;
    opt.request_timeout_sec = 5.0;

    IPOClient ipo_cli(opt);

    SECTION("Get all IPOs in date range") {
        // Get all IPOs from 2024 (no ticker filter)
        auto df = ipo_cli.getIPOs("2024-01-01", "2024-12-31", std::nullopt, 10);
        if (!df.has_value()) {
            FAIL(df.error().message);
        }
        INFO(df->head().repr());
        REQUIRE(df.has_value());
        REQUIRE(df->num_rows() > 0);
        // Check required columns
        REQUIRE(df->contains("ticker"));
        REQUIRE(df->contains("issuer_name"));
        REQUIRE(df->contains("listing_date"));
        REQUIRE(df->contains("ipo_status"));
        REQUIRE(df->contains("exchange"));
        REQUIRE(df->contains("final_price"));
    }

    SECTION("Filter by ticker") {
        // Example: Get specific ticker's IPO (if any)
        // Note: This might return 0 rows if ticker didn't IPO in this range
        auto df = ipo_cli.getIPOs("2020-01-01", "2024-12-31", "SNOW", 10);
        if (df.has_value()) {
            INFO(df->head().repr());
            if (df->num_rows() > 0) {
                REQUIRE(df->contains("ticker"));
                REQUIRE(df->contains("listing_date"));
            }
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

  AggsClient cli(opt);
  auto res = cli.getAggregates("SPY", "2020-01-01", "2020-01-02", true);
  REQUIRE(res.has_value());
  // Expect a single row DataFrame with o,h,l,c,v columns
  REQUIRE(res->num_rows() == 1);
  REQUIRE(res->contains("c"));
  REQUIRE(res->operator[]("c").iloc(0).as_double() == Catch::Approx(321.0));
}
