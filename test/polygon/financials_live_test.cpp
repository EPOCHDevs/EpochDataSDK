#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <epoch_data_sdk/polygon/options.hpp>
#include <epoch_data_sdk/polygon/polygon_client.hpp>
#include <spdlog/spdlog.h>

using namespace data_sdk::polygon;
using namespace Catch::Matchers;

static bool has_env(const char *key) { return std::getenv(key) != nullptr; }

TEST_CASE("Polygon Financial Statements Live Tests",
          "[polygon][financials][live][requires_network]") {
  if (!has_env("POLYGON_API_KEY")) {
    SKIP("POLYGON_API_KEY not set; skipping live financials test");
  }

  Options opts;
  opts.api_key = std::getenv("POLYGON_API_KEY");
  PolygonClient client(opts);

  SECTION("Balance Sheets - AAPL") {
    auto df = client.getBalanceSheets(std::nullopt, std::nullopt, "AAPL",
                                      std::nullopt, std::nullopt, std::nullopt,
                                      5);
    REQUIRE(df.has_value());
    SPDLOG_INFO("Balance Sheets response: {} rows, {} cols", df->num_rows(),
                df->num_cols());
    CHECK(df->num_rows() > 0);
    CHECK(df->contains("ticker"));
    CHECK(df->contains("cik"));
    CHECK(df->contains("fiscal_year"));
    CHECK(df->contains("fiscal_quarter"));
    CHECK(df->contains("timeframe"));
    CHECK(df->contains("cash"));
    CHECK(df->contains("receivables"));
    CHECK(df->contains("inventories"));
    CHECK(df->contains("lt_debt"));
    CHECK(df->contains("retained_earnings"));
    auto cols = df->column_names();
    SPDLOG_INFO("Balance Sheets columns count: {}", cols.size());
  }

  SECTION("Cash Flow Statements - AAPL quarterly") {
    auto df = client.getCashFlowStatements(std::nullopt, std::nullopt, "AAPL",
                                           std::nullopt, std::nullopt,
                                           "quarterly", 5);
    REQUIRE(df.has_value());
    SPDLOG_INFO("Cash Flow Statements response: {} rows, {} cols",
                df->num_rows(), df->num_cols());
    CHECK(df->num_rows() > 0);
    CHECK(df->contains("ticker"));
    CHECK(df->contains("timeframe"));
    CHECK(df->contains("cfo"));
    CHECK(df->contains("ncf_operating"));
    CHECK(df->contains("ncf_investing"));
    CHECK(df->contains("ncf_financing"));
    CHECK(df->contains("capex"));
    CHECK(df->contains("dividends"));
    auto cols = df->column_names();
    SPDLOG_INFO("Cash Flow Statements columns count: {}", cols.size());
  }

  SECTION("Income Statements - AAPL annual") {
    auto df = client.getIncomeStatements(std::nullopt, std::nullopt, "AAPL",
                                         std::nullopt, std::nullopt, "annual",
                                         5);
    REQUIRE(df.has_value());
    SPDLOG_INFO("Income Statements response: {} rows, {} cols", df->num_rows(),
                df->num_cols());
    CHECK(df->num_rows() > 0);
    CHECK(df->contains("ticker"));
    CHECK(df->contains("timeframe"));
    CHECK(df->contains("revenue"));
    CHECK(df->contains("cogs"));
    CHECK(df->contains("gross_profit"));
    CHECK(df->contains("operating_income"));
    CHECK(df->contains("net_income"));
    CHECK(df->contains("basic_eps"));
    CHECK(df->contains("diluted_eps"));
    CHECK(df->contains("rd"));
    CHECK(df->contains("sga"));
    auto cols = df->column_names();
    SPDLOG_INFO("Income Statements columns count: {}", cols.size());
  }

  SECTION("Financial Ratios - AAPL") {
    auto df = client.getFinancialRatios("AAPL", std::nullopt, std::nullopt,
                                        std::nullopt, std::nullopt, std::nullopt,
                                        std::nullopt, std::nullopt, std::nullopt,
                                        5);
    REQUIRE(df.has_value());
    SPDLOG_INFO("Financial Ratios response: {} rows, {} cols", df->num_rows(),
                df->num_cols());
    CHECK(df->num_rows() > 0);
    CHECK(df->contains("ticker"));
    CHECK(df->contains("pe"));
    CHECK(df->contains("pb"));
    CHECK(df->contains("ps"));
    CHECK(df->contains("market_cap"));
    CHECK(df->contains("eps"));
    CHECK(df->contains("div_yield"));
    CHECK(df->contains("debt_to_equity"));
    CHECK(df->contains("current"));
    CHECK(df->contains("quick"));
    CHECK(df->contains("roa"));
    CHECK(df->contains("roe"));
    auto cols = df->column_names();
    SPDLOG_INFO("Financial Ratios columns count: {}", cols.size());
  }

  SECTION("Balance Sheets - Multiple tickers") {
    auto df = client.getBalanceSheets(std::nullopt, std::nullopt,
                                      "AAPL,MSFT,GOOGL", std::nullopt,
                                      std::nullopt, std::nullopt, 10);
    REQUIRE(df.has_value());
    SPDLOG_INFO("Multi-ticker Balance Sheets: {} rows", df->num_rows());
    CHECK(df->num_rows() > 0);
  }

  SECTION("Income Statements - Fiscal year filter") {
    auto df = client.getIncomeStatements(std::nullopt, std::nullopt, "AAPL",
                                         2023, std::nullopt, std::nullopt, 10);
    REQUIRE(df.has_value());
    SPDLOG_INFO("FY 2023 Income Statements: {} rows", df->num_rows());
    CHECK(df->num_rows() > 0);
  }

  SECTION("Cash Flow - Fiscal quarter filter") {
    auto df = client.getCashFlowStatements(std::nullopt, std::nullopt, "AAPL",
                                           2024, 1, std::nullopt, 5);
    REQUIRE(df.has_value());
    SPDLOG_INFO("Q1 2024 Cash Flow: {} rows", df->num_rows());
  }
}

TEST_CASE("Polygon Financial Error Handling",
          "[polygon][financials][live][error]") {
  if (!has_env("POLYGON_API_KEY")) {
    SKIP("POLYGON_API_KEY not set; skipping error handling test");
  }

  Options opts;
  opts.api_key = std::getenv("POLYGON_API_KEY");
  PolygonClient client(opts);

  SECTION("Balance Sheets - Invalid ticker") {
    auto df = client.getBalanceSheets(std::nullopt, std::nullopt,
                                      "INVALIDTICKER999", std::nullopt,
                                      std::nullopt, std::nullopt, 5);
    if (df.has_value()) {
      CHECK(df->empty());
    }
  }

  SECTION("Invalid API key") {
    Options bad_opts;
    bad_opts.api_key = "invalid_key_12345";
    PolygonClient bad_client(bad_opts);
    auto df = bad_client.getBalanceSheets(std::nullopt, std::nullopt, "AAPL",
                                          std::nullopt, std::nullopt,
                                          std::nullopt, 5);
    REQUIRE_FALSE(df.has_value());
    CHECK(df.error().http_status >= 400);
  }
}