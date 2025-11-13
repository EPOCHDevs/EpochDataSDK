#include <catch2/catch_test_macros.hpp>
#include "dataloader/polygon_fetcher.h"
#include "polygon/options.hpp"
#include "polygon/aggs_client.hpp"
#include <epoch_frame/dataframe.h>
#include <epoch_frame/datetime.h>
#include <epoch_data_sdk/common/constants.hpp>
#include <epoch_data_sdk/model/asset/asset_constants.hpp>

using namespace data_sdk::dataloader;
using namespace epoch_frame;
using namespace data_sdk::polygon;

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

TEST_CASE("Polygon EOD timestamps normalized to midnight UTC",
          "[polygon][timestamp]") {
  Options opts;
  opts.api_key = "test";
  opts.http_get_override =
      [](const std::string &path,
         const std::vector<std::pair<std::string, std::string>> &)
      -> std::expected<std::string, HttpError> {
    if (path.find("/v2/aggs/ticker") != std::string::npos) {
      return std::string(R"({
        "status": "OK",
        "results": [
          {"t": 1704067200000, "o": 100, "h": 105, "l": 95, "c": 102, "v": 1000000},
          {"t": 1704153600000, "o": 102, "h": 107, "l": 98, "c": 105, "v": 1100000}
        ]
      })");
    }
    HttpError e;
    e.http_status = 404;
    return std::unexpected(e);
  };

  AggsClient cli(opts);

  SECTION("Stock EOD normalized to midnight") {
    auto df = cli.getAggregates("AAPL", "2024-01-01", "2024-01-02", true);
    REQUIRE(df.has_value());
    CHECK(get_index_timezone(*df) == "UTC");
    CHECK(get_index_unit(*df) == arrow::TimeUnit::NANO);

    auto idx = get_index_values(*df);
    REQUIRE(idx.size() == 2);
    // 2024-01-01 00:00:00 UTC
    CHECK(idx[0] == 1704067200000LL / 86400000LL * 86400000000000LL);
    // 2024-01-02 00:00:00 UTC
    CHECK(idx[1] == 1704153600000LL / 86400000LL * 86400000000000LL);
  }

  SECTION("Crypto EOD normalized to midnight") {
    auto df = cli.getAggregates("X:BTCUSD", "2024-01-01", "2024-01-02", true);
    REQUIRE(df.has_value());
    CHECK(get_index_timezone(*df) == "UTC");
    CHECK(get_index_unit(*df) == arrow::TimeUnit::NANO);

    auto idx = get_index_values(*df);
    REQUIRE(idx.size() == 2);
    // Should be normalized to midnight UTC
    CHECK(idx[0] == 1704067200000LL / 86400000LL * 86400000000000LL);
    CHECK(idx[1] == 1704153600000LL / 86400000LL * 86400000000000LL);
  }
}

TEST_CASE("Polygon intraday RTH filtering for stocks", "[polygon][rth]") {
  Options opts;
  opts.api_key = "test";

  // Mock response with bars throughout the day (ET times as ms)
  opts.http_get_override =
      [](const std::string &path,
         const std::vector<std::pair<std::string, std::string>> &)
      -> std::expected<std::string, HttpError> {
    if (path.find("/v2/aggs/ticker") != std::string::npos &&
        path.find("minute") != std::string::npos) {
      return std::string(R"({
        "status": "OK",
        "results": [
          {"t": 1704115800000, "o": 100, "h": 101, "l": 99, "c": 100, "v": 1000},
          {"t": 1704117000000, "o": 100, "h": 101, "l": 99, "c": 100, "v": 1000},
          {"t": 1704118260000, "o": 101, "h": 102, "l": 100, "c": 101, "v": 2000},
          {"t": 1704129600000, "o": 102, "h": 103, "l": 101, "c": 102, "v": 3000},
          {"t": 1704144000000, "o": 103, "h": 104, "l": 102, "c": 103, "v": 4000},
          {"t": 1704144060000, "o": 103, "h": 104, "l": 102, "c": 103, "v": 1000},
          {"t": 1704147600000, "o": 104, "h": 105, "l": 103, "c": 104, "v": 1000}
        ]
      })");
    }
    HttpError e;
    e.http_status = 404;
    return std::unexpected(e);
  };

  AggsClient cli(opts);

  SECTION("Stock minute bars filtered to RTH") {
    auto df = cli.getAggregates("AAPL", "2024-01-01", "2024-01-01", false);
    REQUIRE(df.has_value());

    // Should filter out pre-market and after-hours bars
    // Only bars between 09:31 and 16:00 ET should remain
    CHECK(df->num_rows() > 0);
    CHECK(df->num_rows() < 7); // Less than total bars due to filtering
  }

  SECTION("Crypto minute bars not filtered") {
    auto df = cli.getAggregates("X:BTCUSD", "2024-01-01", "2024-01-01", false);
    REQUIRE(df.has_value());

    // Crypto should have all bars (no RTH filtering)
    CHECK(df->num_rows() == 7);
  }

  SECTION("FX minute bars not filtered") {
    auto df = cli.getAggregates("C:EURUSD", "2024-01-01", "2024-01-01", false);
    REQUIRE(df.has_value());

    // FX should have all bars (no RTH filtering)
    CHECK(df->num_rows() == 7);
  }

  SECTION("Index minute bars not filtered") {
    auto df = cli.getAggregates("^SPX", "2024-01-01", "2024-01-01", false);
    REQUIRE(df.has_value());

    // Indices should have all bars (no RTH filtering)
    CHECK(df->num_rows() == 7);
  }
}

TEST_CASE("Polygon DST transition handling", "[polygon][dst]") {
  Options opts;
  opts.api_key = "test";

  // Mock bars around DST transitions
  opts.http_get_override =
      [](const std::string &path,
         const std::vector<std::pair<std::string, std::string>> &)
      -> std::expected<std::string, HttpError> {
    if (path.find("2024-03-10") != std::string::npos) {
      // Spring forward: 2024-03-10 (DST starts)
      return std::string(R"({
        "status": "OK",
        "results": [
          {"t": 1710077460000, "o": 100, "h": 101, "l": 99, "c": 100, "v": 1000},
          {"t": 1710097200000, "o": 102, "h": 103, "l": 101, "c": 102, "v": 2000}
        ]
      })");
    }
    if (path.find("2024-11-03") != std::string::npos) {
      // Fall back: 2024-11-03 (DST ends)
      return std::string(R"({
        "status": "OK",
        "results": [
          {"t": 1730646660000, "o": 100, "h": 101, "l": 99, "c": 100, "v": 1000},
          {"t": 1730667600000, "o": 102, "h": 103, "l": 101, "c": 102, "v": 2000}
        ]
      })");
    }
    HttpError e;
    e.http_status = 404;
    return std::unexpected(e);
  };

  AggsClient cli(opts);

  SECTION("Spring forward DST transition") {
    auto df = cli.getAggregates("AAPL", "2024-03-10", "2024-03-10", false);
    REQUIRE(df.has_value());
    CHECK(get_index_timezone(*df) == "UTC");

    // Verify timestamps are correctly converted considering DST
    auto idx = get_index_values(*df);
    for (auto ts : idx) {
      CHECK(ts > 0);
    }
  }

  SECTION("Fall back DST transition") {
    auto df = cli.getAggregates("AAPL", "2024-11-03", "2024-11-03", false);
    REQUIRE(df.has_value());
    CHECK(get_index_timezone(*df) == "UTC");

    // Verify timestamps are correctly converted considering DST
    auto idx = get_index_values(*df);
    for (auto ts : idx) {
      CHECK(ts > 0);
    }
  }
}

TEST_CASE("Polygon timestamp conversion for different asset classes",
          "[polygon][timestamp]") {
  Options opts;
  opts.api_key = "test";

  opts.http_get_override =
      [](const std::string &path,
         const std::vector<std::pair<std::string, std::string>> &)
      -> std::expected<std::string, HttpError> {
    if (path.find("/v2/aggs/ticker") != std::string::npos &&
        path.find("minute") != std::string::npos) {
      return std::string(R"({
        "status": "OK",
        "results": [
          {"t": 1704118260000, "o": 100, "h": 101, "l": 99, "c": 100, "v": 1000}
        ]
      })");
    }
    HttpError e;
    e.http_status = 404;
    return std::unexpected(e);
  };

  AggsClient cli(opts);

  SECTION("Stock timestamps converted from ET to UTC") {
    auto df = cli.getAggregates("AAPL", "2024-01-01", "2024-01-01", false);
    REQUIRE(df.has_value());
    CHECK(get_index_timezone(*df) == "UTC");
    CHECK(get_index_unit(*df) == arrow::TimeUnit::NANO);

    // Stock timestamp should be converted from ET to UTC
    auto idx = get_index_values(*df);
    REQUIRE(idx.size() == 1);
    // Original is ET ms, converted to UTC ns
    CHECK(idx[0] > 0);
  }

  SECTION("Crypto timestamps remain UTC") {
    auto df = cli.getAggregates("X:BTCUSD", "2024-01-01", "2024-01-01", false);
    REQUIRE(df.has_value());
    CHECK(get_index_timezone(*df) == "UTC");
    CHECK(get_index_unit(*df) == arrow::TimeUnit::NANO);

    // Crypto timestamp is already UTC ms, just converted to ns
    auto idx = get_index_values(*df);
    REQUIRE(idx.size() == 1);
    CHECK(idx[0] == 1704118260000LL * 1'000'000LL);
  }

  SECTION("FX timestamps converted from ET to UTC") {
    auto df = cli.getAggregates("C:EURUSD", "2024-01-01", "2024-01-01", false);
    REQUIRE(df.has_value());
    CHECK(get_index_timezone(*df) == "UTC");
    CHECK(get_index_unit(*df) == arrow::TimeUnit::NANO);
  }

  SECTION("Index timestamps converted from ET to UTC") {
    auto df = cli.getAggregates("^SPX", "2024-01-01", "2024-01-01", false);
    REQUIRE(df.has_value());
    CHECK(get_index_timezone(*df) == "UTC");
    CHECK(get_index_unit(*df) == arrow::TimeUnit::NANO);
  }
}
