#include "epoch_data_sdk/polygon/options.hpp"
#include "epoch_data_sdk/polygon/aggs_client.hpp"
#include "epoch_data_sdk/polygon/quotes_client.hpp"
#include "epoch_data_sdk/polygon/trades_client.hpp"
#include "epoch_data_sdk/polygon/financials_client.hpp"
#include "epoch_data_sdk/polygon/dividends_client.hpp"
#include "epoch_data_sdk/polygon/splits_client.hpp"
#include "epoch_data_sdk/polygon/short_volume_client.hpp"
#include "epoch_data_sdk/polygon/short_interest_client.hpp"
#include "epoch_data_sdk/polygon/ipo_client.hpp"
#include "epoch_data_sdk/polygon/economy_client.hpp"
#include "epoch_data_sdk/polygon/news_client.hpp"
#include "epoch_data_sdk/common/async_batch.hpp"
#include <catch2/catch_all.hpp>
#include <epoch_frame/dataframe.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/series.h>
#include <drogon/utils/coroutine.h>


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

    // Test STOCKS (AAPL) - Full field support
    SECTION("Stocks: AAPL") {
        SECTION("Get eod aggregates") {
            auto df_eod = aggs_cli.getAggregates("AAPL", "2020-04-25", "2021-04-25", true);
            if (!df_eod.has_value()) {
                FAIL(df_eod.error().message);
            }
            INFO(df_eod->head().repr());
            REQUIRE(df_eod.has_value());
            REQUIRE(df_eod->num_rows() > 0);
            // Stocks should have all aggregate fields including vw and n
            std::vector<std::string> expected_cols = {"o", "h", "l", "c", "v", "vw", "n"};
            for (const auto& col : expected_cols) {
                REQUIRE(df_eod->contains(col));
            }
        }

        SECTION("Get minute aggregates") {
            auto df_minute = aggs_cli.getAggregates("AAPL", "2021-04-26", "2021-04-26", false);
            if (!df_minute.has_value()) {
                FAIL(df_minute.error().message);
            }
            INFO(df_minute->head().repr());
            REQUIRE(df_minute.has_value());
            REQUIRE(df_minute->num_rows() > 0);
            // Stocks should have all aggregate fields including vw and n
            std::vector<std::string> expected_cols = {"o", "h", "l", "c", "v", "vw", "n"};
            for (const auto& col : expected_cols) {
                REQUIRE(df_minute->contains(col));
            }
        }

        SECTION("Get quotes") {
            auto dfq = quotes_cli.getQuotes("AAPL", "2021-04-26", "2021-04-26", 1);
            if (!dfq.has_value()) {
                FAIL(dfq.error().message);
            }
            INFO(dfq->head().repr());
            REQUIRE(dfq.has_value());
            REQUIRE(dfq->num_rows() > 0);
            // Stock quotes should have at minimum: ap, bp
            REQUIRE(dfq->contains("ap"));
            REQUIRE(dfq->contains("bp"));
        }

        SECTION("Get trades") {
            auto dft = trades_cli.getTrades("AAPL", "2021-04-26", "2021-04-26", 1);
            if (!dft.has_value()) {
                FAIL(dft.error().message);
            }
            INFO(dft->head().repr());
            REQUIRE(dft.has_value());
            REQUIRE(dft->num_rows() > 0);
            // Stock trades should have at minimum: p, s
            REQUIRE(dft->contains("p"));
            REQUIRE(dft->contains("s"));
        }
    }

    // Test FOREX (C:EURUSD) - Limited field support
    SECTION("Forex: C:EURUSD") {
        SECTION("Get eod aggregates") {
            auto df_eod = aggs_cli.getAggregates("C:EURUSD", "2020-04-25", "2021-04-25", true);
            if (!df_eod.has_value()) {
                FAIL(df_eod.error().message);
            }
            INFO(df_eod->head().repr());
            REQUIRE(df_eod.has_value());
            REQUIRE(df_eod->num_rows() > 0);
            // Forex may not have vw and n - only check core OHLCV
            std::vector<std::string> expected_cols = {"o", "h", "l", "c", "v"};
            for (const auto& col : expected_cols) {
                REQUIRE(df_eod->contains(col));
            }
        }

        SECTION("Get minute aggregates") {
            auto df_minute = aggs_cli.getAggregates("C:EURUSD", "2021-04-26", "2021-04-26", false);
            if (!df_minute.has_value()) {
                FAIL(df_minute.error().message);
            }
            INFO(df_minute->head().repr());
            REQUIRE(df_minute.has_value());
            REQUIRE(df_minute->num_rows() > 0);
            // Forex may not have vw and n - only check core OHLCV
            std::vector<std::string> expected_cols = {"o", "h", "l", "c", "v"};
            for (const auto& col : expected_cols) {
                REQUIRE(df_minute->contains(col));
            }
        }

        SECTION("Get quotes") {
            auto dfq = quotes_cli.getQuotes("C:EURUSD", "2021-04-26", "2021-04-26", 1);
            if (!dfq.has_value()) {
                FAIL(dfq.error().message);
            }
            INFO(dfq->head().repr());
            REQUIRE(dfq.has_value());
            REQUIRE(dfq->num_rows() > 0);
            // Forex quotes should have at minimum: ap, bp
            REQUIRE(dfq->contains("ap"));
            REQUIRE(dfq->contains("bp"));
        }
    }

    // Test CRYPTO (X:BTCUSD) - Limited field support
    SECTION("Crypto: X:BTCUSD") {
        SECTION("Get eod aggregates") {
            auto df_eod = aggs_cli.getAggregates("X:BTCUSD", "2020-04-25", "2021-04-25", true);
            if (!df_eod.has_value()) {
                FAIL(df_eod.error().message);
            }
            INFO(df_eod->head().repr());
            REQUIRE(df_eod.has_value());
            REQUIRE(df_eod->num_rows() > 0);
            // Crypto may not have vw and n - only check core OHLCV
            std::vector<std::string> expected_cols = {"o", "h", "l", "c", "v"};
            for (const auto& col : expected_cols) {
                REQUIRE(df_eod->contains(col));
            }
        }

        SECTION("Get minute aggregates") {
            auto df_minute = aggs_cli.getAggregates("X:BTCUSD", "2021-04-26", "2021-04-26", false);
            if (!df_minute.has_value()) {
                FAIL(df_minute.error().message);
            }
            INFO(df_minute->head().repr());
            REQUIRE(df_minute.has_value());
            REQUIRE(df_minute->num_rows() > 0);
            // Crypto may not have vw and n - only check core OHLCV
            std::vector<std::string> expected_cols = {"o", "h", "l", "c", "v"};
            for (const auto& col : expected_cols) {
                REQUIRE(df_minute->contains(col));
            }
        }

        SECTION("Get trades") {
            auto dft = trades_cli.getTrades("X:BTCUSD", "2021-04-26", "2021-04-26", 1);
            if (!dft.has_value()) {
                FAIL(dft.error().message);
            }
            INFO(dft->head().repr());
            REQUIRE(dft.has_value());
            REQUIRE(dft->num_rows() > 0);
            // Crypto trades should have at minimum: p, s
            REQUIRE(dft->contains("p"));
            REQUIRE(dft->contains("s"));
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
        std::vector<std::string> expected_cols = {
            "ticker", "period_end", "fiscal_year", "fiscal_quarter", "timeframe",
            "accounts_payable", "accrued_liabilities", "aoci", "cash",
            "debt_current", "deferred_revenue", "inventories", "lt_debt",
            "ppe_net", "receivables", "retained_earnings"
        };
        for (const auto& col : expected_cols) {
            REQUIRE(df->contains(col));
        }
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
        std::vector<std::string> expected_cols = {
            "ticker", "period_end", "fiscal_year", "fiscal_quarter", "timeframe",
            "cfo", "change_cash", "change_assets", "dda", "dividends",
            "lt_debt_issuances", "ncf_financing", "ncf_investing",
            "ncf_operating", "net_income", "capex"
        };
        for (const auto& col : expected_cols) {
            REQUIRE(df->contains(col));
        }
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
        std::vector<std::string> expected_cols = {
            "ticker", "period_end", "fiscal_year", "fiscal_quarter", "timeframe",
            "basic_eps", "diluted_eps", "revenue", "cogs", "gross_profit",
            "operating_income", "net_income", "rd", "sga"
        };
        for (const auto& col : expected_cols) {
            REQUIRE(df->contains(col));
        }
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
    std::vector<std::string> expected_cols = {
        "ticker", "short_volume", "total_volume", "short_volume_ratio",
        "exempt_volume", "non_exempt_volume"
    };
    for (const auto& col : expected_cols) {
        REQUIRE(df->contains(col));
    }
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
        std::vector<std::string> expected_cols = {
            "ticker", "short_interest", "avg_daily_volume", "days_to_cover"
        };
        for (const auto& col : expected_cols) {
            REQUIRE(df->contains(col));
        }
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
        std::vector<std::string> expected_cols = {
            "ticker", "short_interest", "avg_daily_volume", "days_to_cover"
        };
        for (const auto& col : expected_cols) {
            REQUIRE(df->contains(col));
        }
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
        std::vector<std::string> expected_cols = {
            "ticker", "issuer_name", "listing_date", "announced_date", "ipo_status",
            "exchange", "us_code", "isin", "final_price", "highest_price",
            "lowest_price", "total_offer_size", "shares_outstanding",
            "min_shares_offered", "max_shares_offered"
        };
        for (const auto& col : expected_cols) {
            REQUIRE(df->contains(col));
        }
    }

    SECTION("Filter by ticker") {
        // Example: Get specific ticker's IPO (if any)
        // Note: This might return 0 rows if ticker didn't IPO in this range
        auto df = ipo_cli.getIPOs("2020-01-01", "2024-12-31", "SNOW", 10);
        if (df.has_value()) {
            INFO(df->head().repr());
            if (df->num_rows() > 0) {
                std::vector<std::string> expected_cols = {
                    "ticker", "issuer_name", "listing_date", "announced_date", "ipo_status",
                    "exchange", "us_code", "isin", "final_price", "highest_price",
                    "lowest_price", "total_offer_size", "shares_outstanding",
                    "min_shares_offered", "max_shares_offered"
                };
                for (const auto& col : expected_cols) {
                    REQUIRE(df->contains(col));
                }
            }
        }
    }
}

TEST_CASE("integration: economy API calls", "[polygon][rest][integration]") {
    auto api_key = getenv_or("POLYGON_API_KEY");
    if (api_key.empty()) {
        SKIP("POLYGON_API_KEY not set; skipping integration test");
    }

    Options opt;
    opt.api_key = api_key;
    opt.request_timeout_sec = 5.0;

    EconomyClient economy_cli(opt);

    SECTION("Treasury Yields") {
        auto df = economy_cli.getTreasuryYields("2024-01-01", "2024-01-31", 10);
        if (!df.has_value()) {
            FAIL(df.error().message);
        }
        INFO(df->head().repr());
        REQUIRE(df.has_value());
        REQUIRE(df->num_rows() > 0);
        // Check all treasury yield columns
        std::vector<std::string> expected_cols = {
            "yield_1_month", "yield_3_month", "yield_6_month",
            "yield_1_year", "yield_2_year", "yield_3_year",
            "yield_5_year", "yield_7_year", "yield_10_year",
            "yield_20_year", "yield_30_year"
        };
        for (const auto& col : expected_cols) {
            REQUIRE(df->contains(col));
        }
    }

    SECTION("Inflation") {
        auto df = economy_cli.getInflation("2023-01-01", "2024-01-31", 10);
        if (!df.has_value()) {
            FAIL(df.error().message);
        }
        INFO(df->head().repr());
        REQUIRE(df.has_value());
        REQUIRE(df->num_rows() > 0);
        // Check all inflation columns
        std::vector<std::string> expected_cols = {
            "cpi", "cpi_core", "cpi_year_over_year",
            "pce", "pce_core", "pce_spending"
        };
        for (const auto& col : expected_cols) {
            REQUIRE(df->contains(col));
        }
    }

    SECTION("Inflation Expectations") {
        auto df = economy_cli.getInflationExpectations("2023-01-01", "2024-01-31", 10);
        if (!df.has_value()) {
            FAIL(df.error().message);
        }
        INFO(df->head().repr());
        REQUIRE(df.has_value());
        REQUIRE(df->num_rows() > 0);
        // Check all inflation expectation columns
        std::vector<std::string> expected_cols = {
            "market_5_year", "market_10_year", "forward_years_5_to_10",
            "model_1_year", "model_5_year", "model_10_year", "model_30_year"
        };
        for (const auto& col : expected_cols) {
            REQUIRE(df->contains(col));
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
  // SPY is a stock ETF, should have all fields including vw and n
  REQUIRE(res->num_rows() == 1);
  std::vector<std::string> expected_cols = {"o", "h", "l", "c", "v", "vw", "n"};
  for (const auto& col : expected_cols) {
    REQUIRE(res->contains(col));
  }
  REQUIRE(res->operator[]("c").iloc(0).as_double() == Catch::Approx(321.0));
}

TEST_CASE("EconomyClient: getTreasuryYields builds correct query", "[polygon][rest][economy]") {
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
            R"({"results":[{"date":"2024-01-15","yield_10_year":4.25}],"status":"OK","request_id":"req"})");
      };

  EconomyClient cli(opt);
  auto res = cli.getTreasuryYields("2024-01-01", "2024-01-31", 100);
  REQUIRE(res.has_value());
  REQUIRE(capturedPath == "/fed/v1/treasury-yields");

  auto has = [&](std::string k, std::string v) {
    return std::find(capturedQuery.begin(), capturedQuery.end(),
                     std::make_pair(k, v)) != capturedQuery.end();
  };
  REQUIRE(has("date.gte", "2024-01-01"));
  REQUIRE(has("date.lte", "2024-01-31"));
  REQUIRE(has("sort", "date"));
  REQUIRE(has("order", "desc"));
  REQUIRE(has("limit", "100"));
}

TEST_CASE("EconomyClient: getInflation builds correct query", "[polygon][rest][economy]") {
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
            R"({"results":[{"date":"2024-01-01","cpi":310.5,"cpi_core":320.1}],"status":"OK","request_id":"req"})");
      };

  EconomyClient cli(opt);
  auto res = cli.getInflation("2024-01-01", "2024-12-31");
  REQUIRE(res.has_value());
  REQUIRE(capturedPath == "/fed/v1/inflation");

  auto has = [&](std::string k, std::string v) {
    return std::find(capturedQuery.begin(), capturedQuery.end(),
                     std::make_pair(k, v)) != capturedQuery.end();
  };
  REQUIRE(has("date.gte", "2024-01-01"));
  REQUIRE(has("date.lte", "2024-12-31"));
  REQUIRE(has("sort", "date"));
  REQUIRE(has("order", "desc"));
}

TEST_CASE("EconomyClient: getInflationExpectations builds correct query", "[polygon][rest][economy]") {
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
            R"({"results":[{"date":"2024-01-01","market_5_year":2.5,"model_10_year":2.3}],"status":"OK","request_id":"req"})");
      };

  EconomyClient cli(opt);
  auto res = cli.getInflationExpectations("2023-01-01", "2024-01-01", 50);
  REQUIRE(res.has_value());
  REQUIRE(capturedPath == "/fed/v1/inflation-expectations");

  auto has = [&](std::string k, std::string v) {
    return std::find(capturedQuery.begin(), capturedQuery.end(),
                     std::make_pair(k, v)) != capturedQuery.end();
  };
  REQUIRE(has("date.gte", "2023-01-01"));
  REQUIRE(has("date.lte", "2024-01-01"));
  REQUIRE(has("sort", "date"));
  REQUIRE(has("order", "desc"));
  REQUIRE(has("limit", "50"));
}

TEST_CASE("NewsClient: getNews integration test", "[polygon][rest][news][integration]") {
    Options opt;
    opt.api_key = getenv_or("POLYGON_API_KEY", "");
    if (opt.api_key.empty()) {
        SKIP("POLYGON_API_KEY not set");
    }

    NewsClient news_cli(opt);

    SECTION("Get news for ticker") {
        auto df = news_cli.getNews("AAPL", "2024-01-01", "2024-01-31", 10);
        if (!df.has_value()) {
            FAIL(df.error().message);
        }
        INFO(df->head().repr());
        REQUIRE(df.has_value());
        REQUIRE(df->num_rows() > 0);

        // Exhaustive column check
        std::vector<std::string> expected_cols = {
            "id", "tickers", "title", "author", "publisher",
            "article_url", "description"
        };
        for (const auto& col : expected_cols) {
            REQUIRE(df->contains(col));
        }
    }
}

// ============================================================================
// ASYNC COROUTINE TESTS
// ============================================================================

TEST_CASE("async: getAggregatesAsync returns coroutine", "[polygon][rest][async]") {
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

  // Test async method - use sync_wait to block on coroutine
  auto res = drogon::sync_wait(cli.getAggregatesAsync("SPY", "2020-01-01", "2020-01-02", true));

  REQUIRE(res.has_value());
  REQUIRE(res->num_rows() == 1);
  std::vector<std::string> expected_cols = {"o", "h", "l", "c", "v", "vw", "n"};
  for (const auto& col : expected_cols) {
    REQUIRE(res->contains(col));
  }
  REQUIRE(res->operator[]("c").iloc(0).as_double() == Catch::Approx(321.0));
}

TEST_CASE("async: batch multiple tickers concurrently with joinAll", "[polygon][rest][async][batch]") {
  Options opt;
  opt.api_key = "test";

  int call_count = 0;
  opt.http_get_override =
      [&call_count](const std::string &path,
                    const std::vector<std::pair<std::string, std::string>> &) {
          SPDLOG_WARN("IN http_get_override: {}", path);

        call_count++;
        std::string ticker;
        if (path.find("/AAPL/") != std::string::npos) {
          ticker = "AAPL";
        } else if (path.find("/MSFT/") != std::string::npos) {
          ticker = "MSFT";
        } else if (path.find("/GOOGL/") != std::string::npos) {
          ticker = "GOOGL";
        }

        std::string body = R"({"ticker":")" + ticker + R"(","adjusted":true,"status":"OK","request_id":"req",
          "results":[{"v":1000,"vw":320.1,"o":320.0,"c":321.0,"h":322.0,"l":319.5,"t":1577836800000,"n":123}]
        })";
        return std::expected<std::string, HttpError>(body);
      };

  AggsClient cli(opt);

  // Prepare batch of async tasks
  std::vector<drogon::Task<Expected<epoch_frame::DataFrame>>> tasks;
  tasks.push_back(cli.getAggregatesAsync("AAPL", "2020-01-01", "2020-01-02", true));
  tasks.push_back(cli.getAggregatesAsync("MSFT", "2020-01-01", "2020-01-02", true));
  tasks.push_back(cli.getAggregatesAsync("GOOGL", "2020-01-01", "2020-01-02", true));

  // Execute all concurrently using batch utility
  auto results = data_sdk::common::syncJoinAll(std::move(tasks));

  REQUIRE(call_count == 3);
  REQUIRE(results.size() == 3);

  // Verify results are in order
  REQUIRE(results[0].has_value());
  REQUIRE(results[1].has_value());
  REQUIRE(results[2].has_value());

  REQUIRE(results[0]->num_rows() == 1);
  REQUIRE(results[1]->num_rows() == 1);
  REQUIRE(results[2]->num_rows() == 1);
}

TEST_CASE("async: error from mock is propagated", "[polygon][rest][async]") {
  Options opt;
  opt.api_key = "test";

  opt.http_get_override =
      [](const std::string &path,
         const std::vector<std::pair<std::string, std::string>> &) -> std::expected<std::string, HttpError> {
        // Return 404 error
        return std::unexpected(HttpError{404, "Not found", "test-details"});
      };

  AggsClient cli(opt);

  // Test single async call with error
  auto result = drogon::sync_wait(cli.getAggregatesAsync("AAPL", "2020-01-01", "2020-01-02", true));

  REQUIRE(!result.has_value());
  REQUIRE(result.error().http_status == 404);
  REQUIRE(result.error().message == "Not found");
}

TEST_CASE("async: batch with error handling", "[polygon][rest][async][batch]") {
  // Simplified version without SECTIONs to avoid timing issues
  Options opt;
  opt.api_key = "test";

  std::vector<std::string> paths_called;
  opt.http_get_override =
        [&paths_called](const std::string &path,
           const std::vector<std::pair<std::string, std::string>> &) -> std::expected<std::string, HttpError> {
          SPDLOG_WARN("IN http_get_override (BATCH SECTION): {}", path);
          paths_called.push_back(path);
          // Check ticker from path - format is /v2/aggs/ticker/TICKER/range/...
          if (path.find("/ticker/AAPL/") != std::string::npos) {
            return R"({"ticker":"AAPL","adjusted":true,"status":"OK","request_id":"req",
              "results":[{"v":1000,"vw":320.1,"o":320.0,"c":321.0,"h":322.0,"l":319.5,"t":1577836800000,"n":123}]
            })";
          }
          // INVALID fails with 404
          if (path.find("/ticker/INVALID/") != std::string::npos) {
            return std::unexpected(HttpError{404, "Not found", ""});
          }
          // MSFT succeeds
          if (path.find("/ticker/MSFT/") != std::string::npos) {
            return R"({"ticker":"MSFT","adjusted":true,"status":"OK","request_id":"req",
              "results":[{"v":1000,"vw":320.1,"o":320.0,"c":321.0,"h":322.0,"l":319.5,"t":1577836800000,"n":123}]
            })";
          }
          // Fallback error
          return std::unexpected(HttpError{400, "Bad request", ""});
      };

  AggsClient cli(opt);

  std::vector<drogon::Task<Expected<epoch_frame::DataFrame>>> tasks;
  tasks.push_back(cli.getAggregatesAsync("AAPL", "2020-01-01", "2020-01-02", true));
  tasks.push_back(cli.getAggregatesAsync("INVALID", "2020-01-01", "2020-01-02", true));
  tasks.push_back(cli.getAggregatesAsync("MSFT", "2020-01-01", "2020-01-02", true));

  auto results = data_sdk::common::syncJoinAll(std::move(tasks));

  // Validate we got 3 calls and 3 results (concurrent execution)
  REQUIRE(paths_called.size() == 3);
  REQUIRE(results.size() == 3);

  // Debug: Print what was called in order
  INFO("Path[0]: " << paths_called[0]);
  INFO("Path[1]: " << paths_called[1]);
  INFO("Path[2]: " << paths_called[2]);

  // Validate sequential order: paths should match task order
  // Task[0] = AAPL, Task[1] = INVALID, Task[2] = MSFT
  REQUIRE(paths_called[0].find("/ticker/AAPL/") != std::string::npos);
  REQUIRE(paths_called[1].find("/ticker/INVALID/") != std::string::npos);
  REQUIRE(paths_called[2].find("/ticker/MSFT/") != std::string::npos);

  // Validate results are in the same order as tasks
  // Result[0] should be AAPL (success)
  REQUIRE(results[0].has_value());
  REQUIRE(results[0]->num_rows() == 1);

  // Result[1] should be INVALID (404 error)
  INFO("Result[1] has_value: " << results[1].has_value());
  if (results[1].has_value()) {
    INFO("Result[1] UNEXPECTEDLY succeeded with " << results[1]->num_rows() << " rows");
  } else {
    INFO("Result[1] error: " << results[1].error().http_status << " - " << results[1].error().message);
  }
  REQUIRE(!results[1].has_value());
  REQUIRE(results[1].error().http_status == 404);

  // Result[2] should be MSFT (success)
  REQUIRE(results[2].has_value());
  REQUIRE(results[2]->num_rows() == 1);
}

// ============================================================================
// Async tests for remaining Polygon clients
// ============================================================================

TEST_CASE("async: getQuotesAsync returns coroutine", "[polygon][rest][async][quotes]") {
  Options opt;
  opt.api_key = "test";
  opt.http_get_override =
      [](const std::string &path,
         const std::vector<std::pair<std::string, std::string>> &) {
        REQUIRE(path.find("/v3/quotes/AAPL") != std::string::npos);
        return std::expected<std::string, HttpError>(
          R"({"results":[],"status":"OK","request_id":"req"})"
        );
      };

  QuotesClient cli(opt);
  auto res = drogon::sync_wait(cli.getQuotesAsync("AAPL", "2021-01-01", "2021-01-02", 10));

  REQUIRE(res.has_value());
  REQUIRE(res->num_rows() == 0);  // Empty results from mock
}

TEST_CASE("async: getTradesAsync returns coroutine", "[polygon][rest][async][trades]") {
  Options opt;
  opt.api_key = "test";
  opt.http_get_override =
      [](const std::string &path,
         const std::vector<std::pair<std::string, std::string>> &) {
        REQUIRE(path.find("/v3/trades/MSFT") != std::string::npos);
        return std::expected<std::string, HttpError>(
          R"({"results":[],"status":"OK","request_id":"req"})"
        );
      };

  TradesClient cli(opt);
  auto res = drogon::sync_wait(cli.getTradesAsync("MSFT", "2021-01-01", "2021-01-02", 5));

  REQUIRE(res.has_value());
  REQUIRE(res->num_rows() == 0);  // Empty results from mock
}

TEST_CASE("async: batch quotes and trades concurrently", "[polygon][rest][async][batch]") {
  Options opt;
  opt.api_key = "test";

  int call_count = 0;
  opt.http_get_override =
      [&call_count](const std::string &path,
                    const std::vector<std::pair<std::string, std::string>> &) -> std::expected<std::string, HttpError> {
        call_count++;
        if (path.find("/v3/quotes/") != std::string::npos) {
          return R"({"results":[{"ask_price":150.5,"bid_price":150.3,"participant_timestamp":1609459200000000000}],"status":"OK","request_id":"req"})";
        } else if (path.find("/v3/trades/") != std::string::npos) {
          return R"({"results":[{"price":250.5,"size":100,"participant_timestamp":1609459200000000000}],"status":"OK","request_id":"req"})";
        }
        return std::unexpected(HttpError{400, "Bad request", ""});
      };

  QuotesClient quotes_cli(opt);
  TradesClient trades_cli(opt);

  std::vector<drogon::Task<Expected<epoch_frame::DataFrame>>> tasks;
  tasks.push_back(quotes_cli.getQuotesAsync("AAPL", "2021-01-01", "2021-01-02", 10));
  tasks.push_back(trades_cli.getTradesAsync("MSFT", "2021-01-01", "2021-01-02", 5));

  auto results = data_sdk::common::syncJoinAll(std::move(tasks));

  REQUIRE(call_count == 2);
  REQUIRE(results.size() == 2);
  REQUIRE(results[0].has_value());
  REQUIRE(results[1].has_value());
}

TEST_CASE("async: getNewsAsync returns coroutine", "[polygon][rest][async][news]") {
  Options opt;
  opt.api_key = "test";
  opt.http_get_override =
      [](const std::string &path,
         const std::vector<std::pair<std::string, std::string>> &) {
        REQUIRE(path.find("/v2/reference/news") != std::string::npos);
        return std::expected<std::string, HttpError>(
          R"({"results":[],"status":"OK","request_id":"req","count":0})"
        );
      };

  NewsClient cli(opt);
  auto res = drogon::sync_wait(cli.getNewsAsync("AAPL", "2021-01-01", "2021-01-02", 10));

  REQUIRE(res.has_value());
  REQUIRE(res->num_rows() == 0);  // Empty results from mock
}

TEST_CASE("async: getDividendsAsync returns coroutine", "[polygon][rest][async][dividends]") {
  Options opt;
  opt.api_key = "test";
  opt.http_get_override =
      [](const std::string &path,
         const std::vector<std::pair<std::string, std::string>> &) {
        REQUIRE(path.find("/v3/reference/dividends") != std::string::npos);
        return std::expected<std::string, HttpError>(
          R"({"results":[],"status":"OK","request_id":"req"})"
        );
      };

  DividendsClient cli(opt);
  // Use DividendsOptions struct for cleaner API
  DividendsOptions opts;
  opts.ticker = "AAPL";
  opts.ex_dividend_date_gte = "2021-01-01";
  opts.ex_dividend_date_lte = "2021-12-31";
  opts.limit = 50;
  auto res = drogon::sync_wait(cli.getDividendsAsync(opts));

  REQUIRE(res.has_value());
  REQUIRE(res->num_rows() == 0);  // Empty results from mock
}

TEST_CASE("async: getSplitsAsync returns coroutine", "[polygon][rest][async][splits]") {
  Options opt;
  opt.api_key = "test";
  opt.http_get_override =
      [](const std::string &path,
         const std::vector<std::pair<std::string, std::string>> &) {
        REQUIRE(path.find("/v3/reference/splits") != std::string::npos);
        return std::expected<std::string, HttpError>(
          R"({"results":[],"status":"OK","request_id":"req"})"
        );
      };

  SplitsClient cli(opt);
  // Use SplitsOptions struct for cleaner API
  SplitsOptions opts;
  opts.ticker = "AAPL";
  opts.execution_date_gte = "2020-01-01";
  opts.execution_date_lte = "2020-12-31";
  opts.limit = 50;
  auto res = drogon::sync_wait(cli.getSplitsAsync(opts));

  REQUIRE(res.has_value());
  REQUIRE(res->num_rows() == 0);  // Empty results from mock
}
