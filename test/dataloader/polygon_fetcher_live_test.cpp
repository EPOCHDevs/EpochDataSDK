#include <arrow/type.h>
#include <catch2/catch_test_macros.hpp>
#include "dataloader/polygon_fetcher.h"
#include <epoch_frame/datetime.h>
#include <epoch_data_sdk/common/constants.hpp>
#include <epoch_data_sdk/model/asset/asset_constants.hpp>

using namespace data_sdk::dataloader;
using namespace epoch_frame;

static bool has_env(const char *key) { return std::getenv(key) != nullptr; }

static std::string tz_of_index(const epoch_frame::DataFrame &df) {
  auto dtype = df.index()->dtype();
  auto ts = std::dynamic_pointer_cast<arrow::TimestampType>(dtype);
  REQUIRE(ts != nullptr);
  return ts->timezone();
}

TEST_CASE("Polygon live minute and daily fetch (requires POLYGON_API_KEY)",
          "[polygon][polygon_live][requires_network]") {
  if (!has_env("POLYGON_API_KEY")) {
    SKIP("POLYGON_API_KEY not set; skipping live Polygon test");
  }

  PolygonDataFetcher fetcher;
  const auto &assets =
      data_sdk::asset::AssetConstants::instance();

  auto fromDate = DateTime::from_date_str("2024-01-02").date();
  auto toDate = DateTime::from_date_str("2024-01-02").date();

  SECTION("daily bars AAPL: columns and index") {
    auto res =
        fetcher.Fetch(assets.AAPL, DataCategory::DailyBars, fromDate, toDate);
    REQUIRE(res.has_value());
    const auto &cols = data_sdk::ColumnConstants::instance();
    CHECK(res->contains(cols.OPEN()));
    CHECK(res->contains(cols.HIGH()));
    CHECK(res->contains(cols.LOW()));
    CHECK(res->contains(cols.CLOSE()));
    CHECK(res->contains(cols.VOLUME()));
    CHECK(tz_of_index(*res) == "UTC");
    // Daily bars should include vw and n for stocks
    CHECK(res->contains("vw"));
    CHECK(res->contains("n"));
  }

  SECTION("minute bars AAPL: columns and index") {
    auto res =
        fetcher.Fetch(assets.AAPL, DataCategory::MinuteBars, fromDate, toDate);
    REQUIRE(res.has_value());
    const auto &cols = data_sdk::ColumnConstants::instance();
    CHECK(res->contains(cols.OPEN()));
    CHECK(res->contains(cols.HIGH()));
    CHECK(res->contains(cols.LOW()));
    CHECK(res->contains(cols.CLOSE()));
    CHECK(res->contains(cols.VOLUME()));
    CHECK(res->contains("vw"));
    CHECK(res->contains("n"));
    CHECK(tz_of_index(*res) == "UTC");
  }

  SECTION("minute bars FX (EUR_USD): columns and index") {
    auto res = fetcher.Fetch(assets.EUR_USD, DataCategory::MinuteBars, fromDate,
                             toDate);
    REQUIRE(res.has_value());
    const auto &cols = data_sdk::ColumnConstants::instance();
    CHECK(res->contains(cols.OPEN()));
    CHECK(res->contains(cols.HIGH()));
    CHECK(res->contains(cols.LOW()));
    CHECK(res->contains(cols.CLOSE()));
    CHECK(res->contains(cols.VOLUME()));
    CHECK(tz_of_index(*res) == "UTC");
    CHECK(res->contains("vw"));
    CHECK(res->contains("n"));
  }

  SECTION("minute bars Crypto (BTC_USD): columns and index") {
    auto res = fetcher.Fetch(assets.BTC_USD, DataCategory::MinuteBars, fromDate,
                             toDate);
    REQUIRE(res.has_value());
    const auto &cols = data_sdk::ColumnConstants::instance();
    CHECK(res->contains(cols.OPEN()));
    CHECK(res->contains(cols.HIGH()));
    CHECK(res->contains(cols.LOW()));
    CHECK(res->contains(cols.CLOSE()));
    CHECK(res->contains(cols.VOLUME()));
    CHECK(tz_of_index(*res) == "UTC");
    CHECK(res->contains("vw"));
    CHECK(res->contains("n"));
  }

  SECTION("minute bars Index (SPX): columns and index") {
    auto res =
        fetcher.Fetch(assets.SPX, DataCategory::MinuteBars, fromDate, toDate);
    REQUIRE(res.has_value());
    const auto &cols = data_sdk::ColumnConstants::instance();
    CHECK(res->contains(cols.OPEN()));
    CHECK(res->contains(cols.HIGH()));
    CHECK(res->contains(cols.LOW()));
    CHECK(res->contains(cols.CLOSE()));
    CHECK(res->contains(cols.VOLUME()));
    CHECK(tz_of_index(*res) == "UTC");
  }
}
