#include <catch2/catch_test_macros.hpp>
#include "dataloader/archive_fetcher.h"
#include <epoch_frame/dataframe.h>
#include <epoch_data_sdk/common/constants.hpp>
#include <epoch_data_sdk/model/asset/asset_constants.hpp>

using namespace data_sdk::dataloader;
using namespace epoch_frame;

static bool s3_integration_enabled() {
  if (const char *it = std::getenv("EPOCH_DB_S3_IT");
      it && std::string(it) == "1") {
    return true;
  }
  return false;
}

TEST_CASE("ArchiveDataFetcher S3 integration - Stocks DailyBars",
          "[archive_fetcher][s3]") {
  if (!s3_integration_enabled()) {
    SUCCEED("Skipped S3 integration test. Set EPOCH_DB_S3_IT=1 to enable.");
    return;
  }

  ArchiveDataFetcher fetcher(std::string(data_sdk::EPOCH_DB_S3));
  auto asset = data_sdk::asset::AssetConstants::instance().AAPL;
  auto fromDate = DateTime::from_date_str("2019-01-01").date();
  auto toDate = DateTime::from_date_str("2019-02-01").date();

  auto result = fetcher.Fetch(asset, DataCategory::DailyBars, fromDate, toDate);
  REQUIRE(result.has_value());
  // Allow empty frames if date ranges have no data
  // But typically should have some rows
}

TEST_CASE("ArchiveDataFetcher S3 integration - FX MinuteBars",
          "[archive_fetcher][s3]") {
  if (!s3_integration_enabled()) {
    SUCCEED("Skipped S3 integration test. Set EPOCH_DB_S3_IT=1 to enable.");
    return;
  }

  ArchiveDataFetcher fetcher(std::string(data_sdk::EPOCH_DB_S3));
  auto asset =
      data_sdk::asset::AssetConstants::instance().EUR_USD;
  auto fromDate = DateTime::from_date_str("2019-01-02").date();
  auto toDate = DateTime::from_date_str("2019-01-02").date();

  auto result =
      fetcher.Fetch(asset, DataCategory::MinuteBars, fromDate, toDate);
  REQUIRE(result.has_value());
}

TEST_CASE("ArchiveDataFetcher S3 integration - Crypto DailyBars",
          "[archive_fetcher][s3]") {
  if (!s3_integration_enabled()) {
    SUCCEED("Skipped S3 integration test. Set EPOCH_DB_S3_IT=1 to enable.");
    return;
  }

  ArchiveDataFetcher fetcher(std::string(data_sdk::EPOCH_DB_S3));
  auto asset =
      data_sdk::asset::AssetConstants::instance().BTC_USD;
  auto fromDate = DateTime::from_date_str("2019-01-01").date();
  auto toDate = DateTime::from_date_str("2019-02-01").date();

  auto result = fetcher.Fetch(asset, DataCategory::DailyBars, fromDate, toDate);
  REQUIRE(result.has_value());
}
