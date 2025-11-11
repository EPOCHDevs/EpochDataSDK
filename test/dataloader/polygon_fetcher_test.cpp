#include "epoch_frame/datetime.h"
#include <arrow/type.h>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include "dataloader/polygon_fetcher.h"
#include <epoch_frame/dataframe.h>
#include <epoch_data_sdk/common/constants.hpp>
#include <epoch_data_sdk/model/asset/asset_constants.hpp>
#include "../src/polygon/options.hpp"
#include "../src/polygon/quotes_client.hpp"
#include "../src/polygon/trades_client.hpp"

using namespace data_sdk::dataloader;
using namespace epoch_frame;
using namespace Catch::Matchers;

class PolygonFetcherTestFixture {
public:
  PolygonFetcherTestFixture() {
    // Create fetcher with test options (could mock HTTP if needed)
    data_sdk::polygon::Options opts;
    opts.api_key = "test_api_key";
    opts.base_url = "https://api.polygon.io";
    opts.request_timeout_sec = 5.0;

    // For testing, we might want to override the HTTP client
    // to avoid actual network calls
    opts.http_get_override =
        [this](const std::string &path,
               const std::vector<std::pair<std::string, std::string>> &query)
        -> std::expected<std::string, data_sdk::polygon::HttpError> {
      return mockHttpResponse(path, query);
    };

    fetcher = std::make_unique<PolygonDataFetcher>(opts);
  }

  std::expected<std::string, data_sdk::polygon::HttpError>
  mockHttpResponse(const std::string &path,
                   const std::vector<std::pair<std::string, std::string>> &) {
    // Return mock JSON response based on path
    if (path.find("/v2/aggs/ticker") != std::string::npos) {
      // Distinguish day vs minute by path
      const bool isMinute = path.find("/minute/") != std::string::npos;
      if (isMinute) {
        // Provide ET-local millisecond timestamps within RTH: 09:31 and 09:32
        // ET 2024-01-01 09:31:00 ET and 09:32:00 ET in milliseconds since epoch
        // interpreted as ET We approximate by using the same ms values; the
        // client converts ET->UTC internally
        return std::string(R"({
          "status": "OK",
          "results": [
            {
              "t": 1704118260000,
              "o": 100.0,
              "h": 101.0,
              "l": 99.5,
              "c": 100.8,
              "v": 5000,
              "vw": 100.6,
              "n": 200
            },
            {
              "t": 1704118320000,
              "o": 100.8,
              "h": 101.2,
              "l": 100.6,
              "c": 101.0,
              "v": 6000,
              "vw": 100.9,
              "n": 240
            }
          ],
          "resultsCount": 2
        })");
      }
      // Daily bars: timestamps are milliseconds at midnight UTC
      return std::string(R"({
        "status": "OK",
        "results": [
          {
            "t": 1704067200000,
            "o": 100.0,
            "h": 105.0,
            "l": 95.0,
            "c": 102.0,
            "v": 1000000,
            "vw": 101.0,
            "n": 5000
          },
          {
            "t": 1704153600000,
            "o": 102.0,
            "h": 107.0,
            "l": 98.0,
            "c": 105.0,
            "v": 1100000,
            "vw": 103.0,
            "n": 5500
          }
        ],
        "resultsCount": 2
      })");
    }
    data_sdk::polygon::HttpError e;
    e.http_status = 404;
    e.message = "Not found";
    return std::unexpected(e);
  }

  std::unique_ptr<PolygonDataFetcher> fetcher;
};

static std::string get_index_timezone(const epoch_frame::DataFrame &df) {
  auto dtype = df.index()->dtype();
  auto ts = std::dynamic_pointer_cast<arrow::TimestampType>(dtype);
  REQUIRE(ts != nullptr);
  return ts->timezone();
}

static arrow::TimeUnit::type get_index_unit(const epoch_frame::DataFrame &df) {
  auto dtype = df.index()->dtype();
  auto ts = std::dynamic_pointer_cast<arrow::TimestampType>(dtype);
  REQUIRE(ts != nullptr);
  return ts->unit();
}

static std::vector<int64_t> get_index_values(const epoch_frame::DataFrame &df) {
  std::vector<int64_t> values;
  auto arr = df.index()->array().to_timestamp_view();
  values.reserve(arr->length());
  for (int64_t i = 0; i < arr->length(); ++i) {
    if (arr->IsNull(i)) {
      values.push_back(0);
    } else {
      values.push_back(arr->Value(i));
    }
  }
  return values;
}

TEST_CASE("PolygonDataFetcher::Fetch", "[polygon_fetcher]") {
  PolygonFetcherTestFixture fixture;

  SECTION("fetches daily bars successfully") {
    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;
    auto fromDate = DateTime::from_date_str("2024-01-01").date();
    auto toDate = DateTime::from_date_str("2024-01-31").date();

    auto result = fixture.fetcher->Fetch(asset, DataCategory::DailyBars,
                                         fromDate, toDate);

    REQUIRE(result.has_value());
    auto df = *result;
    CHECK(df.num_rows() == 2);
    CHECK(df.num_cols() >= 5); // At least OHLCV columns
    const auto &cols = data_sdk::ColumnConstants::instance();
    CHECK(df.contains(cols.OPEN()));
    CHECK(df.contains(cols.HIGH()));
    CHECK(df.contains(cols.LOW()));
    CHECK(df.contains(cols.CLOSE()));
    CHECK(df.contains(cols.VOLUME()));
    // Aggregates include vwap and trade count as well for stocks
    CHECK(df.contains("vw"));
    CHECK(df.contains("n"));
    CHECK(get_index_timezone(df) == "UTC");
    CHECK(get_index_unit(df) == arrow::TimeUnit::NANO);
    auto idx = get_index_values(df);
    REQUIRE(idx.size() == 2);
    CHECK(idx[0] == 1704067200LL * 1'000'000'000LL);
    CHECK(idx[1] == 1704153600LL * 1'000'000'000LL);
  }

  SECTION("fetches minute bars successfully") {
    auto asset = data_sdk::asset::AssetConstants::instance().GOOG;
    auto fromDate = DateTime::from_date_str("2024-01-01").date();
    auto toDate = DateTime::from_date_str("2024-01-01").date();

    auto result = fixture.fetcher->Fetch(asset, DataCategory::MinuteBars,
                                         fromDate, toDate);

    REQUIRE(result.has_value());
    auto df = *result;
    CHECK(df.num_rows() > 0);
    CHECK(df.contains("vw")); // Volume weighted average
    CHECK(df.contains("n"));  // Number of transactions
    CHECK(get_index_timezone(df) == "UTC");
    CHECK(get_index_unit(df) == arrow::TimeUnit::NANO);
  }

  SECTION("works with FX asset constant") {
    auto asset =
        data_sdk::asset::AssetConstants::instance().EUR_USD;
    auto fromDate = DateTime::from_date_str("2024-01-01").date();
    auto toDate = DateTime::from_date_str("2024-01-31").date();

    auto result = fixture.fetcher->Fetch(asset, DataCategory::DailyBars,
                                         fromDate, toDate);

    REQUIRE(result.has_value());
    CHECK(get_index_timezone(*result) == "UTC");
    CHECK(get_index_unit(*result) == arrow::TimeUnit::NANO);
  }

  // Error handling covered in client tests; keep fetcher tests focused on
  // parsing

  SECTION("handles empty response") {
    data_sdk::polygon::Options opts;
    opts.http_get_override =
        [](const std::string &,
           const std::vector<std::pair<std::string, std::string>> &) {
          return std::string(R"({
        "status": "OK",
        "results": [],
        "resultsCount": 0
      })");
        };

    PolygonDataFetcher emptyFetcher(opts);
    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;
    auto fromDate = DateTime::from_date_str("2024-01-01").date();
    auto toDate = DateTime::from_date_str("2024-01-31").date();

    auto result =
        emptyFetcher.Fetch(asset, DataCategory::DailyBars, fromDate, toDate);

    REQUIRE(result.has_value());
    CHECK(result->empty());
  }

  SECTION("parses aggregates when optional metadata fields are missing") {
    data_sdk::polygon::Options opts;
    opts.http_get_override =
        [](const std::string &path,
           const std::vector<std::pair<std::string, std::string>> &)
        -> std::expected<std::string, data_sdk::polygon::HttpError> {
      if (path.find("/v2/aggs/ticker/") != std::string::npos &&
          path.find("/day/") != std::string::npos) {
        return std::string(R"({
              "ticker":"AAPL",
              "results":[{"t":1704067200000,"o":100.0,"h":105.0,
                            "l":95.0,"c":102.0,"v":1000000,
                            "vw":101.0,"n":5000}],
              "resultsCount":1
            })");
      }
      data_sdk::polygon::HttpError e;
      e.http_status = 404;
      e.message = "Not found";
      return std::unexpected(e);
    };

    PolygonDataFetcher fetcher(opts);
    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;
    auto fromDate = DateTime::from_date_str("2024-01-01").date();
    auto toDate = DateTime::from_date_str("2024-01-31").date();

    auto result =
        fetcher.Fetch(asset, DataCategory::DailyBars, fromDate, toDate);
    REQUIRE(result.has_value());
    CHECK(result->num_rows() == 1);
    CHECK(result->contains("vw"));
    CHECK(result->contains("n"));
  }

  SECTION("minute bars: invalid JSON yields parse error") {
    data_sdk::polygon::Options opts;
    opts.http_get_override =
        [](const std::string &path,
           const std::vector<std::pair<std::string, std::string>> &)
        -> std::expected<std::string, data_sdk::polygon::HttpError> {
      // Simulate minute endpoint but with invalid JSON body
      if (path.find("/v2/aggs/ticker/") != std::string::npos &&
          path.find("/minute/") != std::string::npos) {
        return std::string("not a json");
      }
      data_sdk::polygon::HttpError e;
      e.http_status = 404;
      e.message = "Not found";
      return std::unexpected(e);
    };

    PolygonDataFetcher badFetcher(opts);
    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;
    auto date = DateTime::from_date_str("2024-01-01").date();

    auto result = badFetcher.Fetch(asset, DataCategory::MinuteBars, date, date);
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error() == "Failed to parse JSON response");
  }
}

TEST_CASE("PolygonDataFetcher with different asset types",
          "[polygon_fetcher]") {
  PolygonFetcherTestFixture fixture;

  SECTION("handles forex assets") {
    auto fxAsset =
        data_sdk::asset::AssetConstants::instance().EUR_USD;
    auto fromDate = DateTime::from_date_str("2024-01-01").date();
    auto toDate = DateTime::from_date_str("2024-01-31").date();

    auto result = fixture.fetcher->Fetch(fxAsset, DataCategory::MinuteBars,
                                         fromDate, toDate);

    REQUIRE(result.has_value());
    CHECK(result->num_rows() > 0);
    CHECK(get_index_timezone(*result) == "UTC");
    CHECK(get_index_unit(*result) == arrow::TimeUnit::NANO);
  }

  SECTION("handles crypto assets") {
    auto cryptoAsset =
        data_sdk::asset::AssetConstants::instance().BTC_USD;
    auto fromDate = DateTime::from_date_str("2024-01-01").date();
    auto toDate = DateTime::from_date_str("2024-01-31").date();

    auto result = fixture.fetcher->Fetch(cryptoAsset, DataCategory::DailyBars,
                                         fromDate, toDate);

    REQUIRE(result.has_value());
    CHECK(get_index_timezone(*result) == "UTC");
    CHECK(get_index_unit(*result) == arrow::TimeUnit::NANO);
  }

  SECTION("minute bars: HTTP error propagates") {
    data_sdk::polygon::Options opts;
    opts.http_get_override =
        [](const std::string &path,
           const std::vector<std::pair<std::string, std::string>> &)
        -> std::expected<std::string, data_sdk::polygon::HttpError> {
      if (path.find("/v2/aggs/ticker/") != std::string::npos &&
          path.find("/minute/") != std::string::npos) {
        data_sdk::polygon::HttpError e;
        e.http_status = 401;
        e.message = "Unauthorized";
        return std::unexpected(e);
      }
      data_sdk::polygon::HttpError e;
      e.http_status = 404;
      e.message = "Not found";
      return std::unexpected(e);
    };

    PolygonDataFetcher errFetcher(opts);
    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;
    auto date = DateTime::from_date_str("2024-01-01").date();

    auto result = errFetcher.Fetch(asset, DataCategory::MinuteBars, date, date);
    REQUIRE_FALSE(result.has_value());
    // Error bubble comes from Polygon client; message should be present
    CHECK(result.error().find("Unauthorized") != std::string::npos);
  }
}

TEST_CASE("PolygonDataFetcher date range handling", "[polygon_fetcher]") {
  PolygonFetcherTestFixture fixture;

  SECTION("handles single day request") {
    auto asset = data_sdk::asset::AssetConstants::instance().SPY;
    auto date = DateTime::from_date_str("2024-01-15").date();

    auto result =
        fixture.fetcher->Fetch(asset, DataCategory::DailyBars, date, date);

    REQUIRE(result.has_value());
    CHECK(get_index_timezone(*result) == "UTC");
    CHECK(get_index_unit(*result) == arrow::TimeUnit::NANO);
  }

  SECTION("handles long date range") {
    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;
    auto fromDate = DateTime::from_date_str("2023-01-01").date();
    auto toDate = DateTime::from_date_str("2024-12-31").date();

    auto result = fixture.fetcher->Fetch(asset, DataCategory::DailyBars,
                                         fromDate, toDate);

    REQUIRE(result.has_value());
    CHECK(get_index_timezone(*result) == "UTC");
    CHECK(get_index_unit(*result) == arrow::TimeUnit::NANO);
  }

  SECTION("handles weekend dates") {
    auto asset = data_sdk::asset::AssetConstants::instance().IBM;
    auto fromDate = DateTime::from_date_str("2024-01-06").date(); // Saturday
    auto toDate = DateTime::from_date_str("2024-01-07").date();   // Sunday

    auto result = fixture.fetcher->Fetch(asset, DataCategory::DailyBars,
                                         fromDate, toDate);

    // Should still succeed, even if no data for weekends
    REQUIRE(result.has_value());
    CHECK(get_index_timezone(*result) == "UTC");
    CHECK(get_index_unit(*result) == arrow::TimeUnit::NANO);
  }
}

TEST_CASE("Polygon quotes and trades produce UTC indexes",
          "[polygon_fetcher][utc]") {
  data_sdk::polygon::Options opts;
  opts.api_key = "test";

  // Mock both quotes and trades endpoints
  opts.http_get_override =
      [](const std::string &path,
         const std::vector<std::pair<std::string, std::string>> &)
      -> std::expected<std::string, data_sdk::polygon::HttpError> {
    if (path.find("/v3/quotes/") != std::string::npos) {
      return std::string(R"({
        "status": "OK",
        "request_id": "req",
        "results": [
          {"participant_timestamp": 1704067200000000000,
           "sip_timestamp": 1704067200001000000,
           "ask_price": 101.2, "bid_price": 101.1,
           "ask_size": 100, "bid_size": 200,
           "ask_exchange": 11, "bid_exchange": 12,
           "sequence_number": 1, "tape": 1},
          {"participant_timestamp": 1704067260000000000,
           "sip_timestamp": 1704067260001000000,
           "ask_price": 101.3, "bid_price": 101.2,
           "ask_size": 120, "bid_size": 220,
           "ask_exchange": 11, "bid_exchange": 12,
           "sequence_number": 2, "tape": 1}
        ]
      })");
    }
    if (path.find("/v3/trades/") != std::string::npos) {
      return std::string(R"({
        "status": "OK",
        "request_id": "req",
        "results": [
          {"participant_timestamp": 1704067200000000000,
           "sip_timestamp": 1704067200001000000,
           "price": 100.5, "size": 1.25,
           "exchange": 7, "sequence_number": 10, "tape": 1},
          {"participant_timestamp": 1704067260000000000,
           "sip_timestamp": 1704067260001000000,
           "price": 100.7, "size": 0.75,
           "exchange": 7, "sequence_number": 11, "tape": 1}
        ]
      })");
    }
    data_sdk::polygon::HttpError e;
    e.http_status = 404;
    e.message = "Not found";
    return std::unexpected(e);
  };

  data_sdk::polygon::QuotesClient quotes_cli(opts);
  data_sdk::polygon::TradesClient trades_cli(opts);

  SECTION("quotes: Stocks, Indices, FX, Crypto all UTC") {
    for (const std::string &ticker : {"AAPL", "^SPX", "C:EURUSD", "X:BTCUSD"}) {
      auto df = quotes_cli.getQuotes(ticker, "2024-01-01", "2024-01-01");
      REQUIRE(df.has_value());
      CHECK(df->num_rows() == 2);
      CHECK(get_index_timezone(*df) == "UTC");
      CHECK(get_index_unit(*df) == arrow::TimeUnit::NANO);
      auto idx = get_index_values(*df);
      REQUIRE(idx.size() == 2);
      CHECK(idx[0] == 1704067200000000000LL);
      CHECK(idx[1] == 1704067260000000000LL);
      CHECK(df->contains("ap"));
      CHECK(df->contains("bp"));
      CHECK(df->contains("asz"));
      CHECK(df->contains("bsz"));
      CHECK(df->contains("ax"));
      CHECK(df->contains("bx"));
      CHECK(df->contains("seq"));
      CHECK(df->contains("sip"));
      CHECK(df->contains("tape"));
    }
  }

  SECTION("trades: Stocks, Indices, FX, Crypto all UTC") {
    for (const std::string &ticker : {"AAPL", "^SPX", "C:EURUSD", "X:BTCUSD"}) {
      auto df = trades_cli.getTrades(ticker, "2024-01-01", "2024-01-01");
      REQUIRE(df.has_value());
      CHECK(df->num_rows() == 2);
      CHECK(get_index_timezone(*df) == "UTC");
      CHECK(get_index_unit(*df) == arrow::TimeUnit::NANO);
      auto idx = get_index_values(*df);
      REQUIRE(idx.size() == 2);
      CHECK(idx[0] == 1704067200000000000LL);
      CHECK(idx[1] == 1704067260000000000LL);
      CHECK(df->contains("p"));
      CHECK(df->contains("s"));
      CHECK(df->contains("x"));
      CHECK(df->contains("seq"));
      CHECK(df->contains("sip"));
      CHECK(df->contains("tape"));
    }
  }
}

TEST_CASE("PolygonDataFetcher pagination", "[polygon_fetcher]") {
  SECTION("handles pagination with next_url") {
    data_sdk::polygon::Options opts;
    int call_count = 0;
    opts.http_get_override =
        [&call_count](
            const std::string &path,
            const std::vector<std::pair<std::string, std::string>> &query)
        -> std::expected<std::string, data_sdk::polygon::HttpError> {
      call_count++;
      if (path.find("/v2/aggs/ticker/") != std::string::npos &&
          path.find("/day/") != std::string::npos) {
        if (call_count == 1) {
          // First page with next_url
          return std::string(R"({
                "ticker":"AAPL",
                "status":"OK",
                "request_id":"test",
                "count":2,
                "next_url":"https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2024-01-01/2024-01-31?cursor=page2",
                "results":[{"t":1704067200000,"o":100.0,"h":105.0,
                              "l":95.0,"c":102.0,"v":1000000,
                              "vw":101.0,"n":5000}],
                "resultsCount":1
              })");
        } else if (call_count == 2) {
          // Second page without next_url
          return std::string(R"({
                "ticker":"AAPL",
                "status":"OK",
                "request_id":"test2",
                "count":1,
                "results":[{"t":1704153600000,"o":102.0,"h":107.0,
                              "l":98.0,"c":105.0,"v":1100000,
                              "vw":103.0,"n":5500}],
                "resultsCount":1
              })");
        }
      }
      data_sdk::polygon::HttpError e;
      e.http_status = 404;
      e.message = "Not found";
      return std::unexpected(e);
    };

    PolygonDataFetcher fetcher(opts);
    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;
    auto fromDate = DateTime::from_date_str("2024-01-01").date();
    auto toDate = DateTime::from_date_str("2024-01-31").date();

    auto result =
        fetcher.Fetch(asset, DataCategory::DailyBars, fromDate, toDate);
    REQUIRE(result.has_value());
    CHECK(result->num_rows() == 2); // Should merge both pages
    CHECK(call_count == 2);         // Should have made 2 HTTP calls
    CHECK(result->contains("vw"));
    CHECK(result->contains("n"));
  }
}
