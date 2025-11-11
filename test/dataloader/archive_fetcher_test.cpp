#include "common/asserts.h"
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include "dataloader/archive_fetcher.h"
#include <epoch_frame/dataframe.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/serialization.h>
#include <filesystem>
#include <fstream>
#include <epoch_data_sdk/common/constants.hpp>
#include <epoch_data_sdk/model/asset/asset_constants.hpp>

using namespace data_sdk::dataloader;
using namespace epoch_frame;
using namespace Catch::Matchers;

class ArchiveFetcherTestFixture {
public:
  ArchiveFetcherTestFixture() {
    // Create temp directory structure for archive
    tempArchive = std::filesystem::temp_directory_path() / "archive_test";
    std::filesystem::create_directories(tempArchive);

    // Set archive path
    archivePath = tempArchive.string();
    fetcher = std::make_unique<ArchiveDataFetcher>(archivePath);
  }

  ~ArchiveFetcherTestFixture() { std::filesystem::remove_all(tempArchive); }

  DataFrame makeDailyDf(const std::string &startDate, int numDays) {
    std::vector<DateTime> timestamps;
    std::vector<double> opens, highs, lows, closes, volumes;
    auto baseDate = DateTime::from_date_str(startDate).date();
    for (int i = 0; i < numDays; ++i) {
      timestamps.push_back(DateTime(baseDate) + chrono_days(i));
      opens.push_back(100.0 + i);
      highs.push_back(105.0 + i);
      lows.push_back(95.0 + i);
      closes.push_back(102.0 + i);
      volumes.push_back(1000000.0 + i * 10000);
    }
    return make_dataframe<double>(
        epoch_frame::factory::index::make_datetime_index(timestamps),
        {opens, highs, lows, closes, volumes}, {"o", "h", "l", "c", "v"});
  }

  DataFrame makeMinuteDfForDay(const std::string &date, int numMinutes) {
    std::vector<DateTime> timestamps;
    std::vector<double> opens, highs, lows, closes, volumes;
    auto baseDate = DateTime::from_date_str(date).date();
    for (int i = 0; i < numMinutes; ++i) {
      timestamps.push_back(DateTime(baseDate) +
                           chrono_minutes(9 * 60 + 30 + i));
      opens.push_back(100.0 + i);
      highs.push_back(105.0 + i);
      lows.push_back(95.0 + i);
      closes.push_back(102.0 + i);
      volumes.push_back(1000000.0 + i * 1000);
    }
    return make_dataframe<double>(
        epoch_frame::factory::index::make_datetime_index(timestamps),
        {opens, highs, lows, closes, volumes}, {"o", "h", "l", "c", "v"});
  }

  void writeParquetForAsset(const data_sdk::asset::Asset &asset,
                            DataCategory category, const DataFrame &df) {
    auto filePath = tempArchive / DataCategoryWrapper::ToString(category) /
                    AssetClassWrapper::ToLongFormString(asset.GetAssetClass()) /
                    (asset.GetSymbolStr() + ".parquet.gzip");
    std::filesystem::create_directories(filePath.parent_path());
    auto status = epoch_frame::write_parquet(
        df, filePath.string(), {.include_index = true, .index_label = "t"});
    if (!status.ok()) {
      throw std::runtime_error(status.message());
    }
  }

  // Removed createTestAsset; use EpochStratifyXAssetConstants instead

  std::filesystem::path tempArchive;
  std::string archivePath;
  std::unique_ptr<ArchiveDataFetcher> fetcher;
};

TEST_CASE("ArchiveDataFetcher::Fetch", "[archive_fetcher]") {
  ArchiveFetcherTestFixture fixture;

  SECTION("fetches daily bars from archive") {
    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;
    auto df = fixture.makeDailyDf("2024-01-01", 10);
    fixture.writeParquetForAsset(asset, DataCategory::DailyBars, df);

    auto fromDate = DateTime::from_date_str("2024-01-01").date();
    auto toDate = DateTime::from_date_str("2024-01-15").date();

    auto result = fixture.fetcher->Fetch(asset, DataCategory::DailyBars,
                                         fromDate, toDate);

    REQUIRE(result.has_value());
    df = *result;
    CHECK(df.num_rows() == 10); // 5 + 5 from two files
    CHECK(df.contains("o"));
    CHECK(df.contains("h"));
    CHECK(df.contains("l"));
    CHECK(df.contains("c"));
    CHECK(df.contains("v"));
  }

  SECTION("fetches minute bars from archive") {
    auto asset = data_sdk::asset::AssetConstants::instance().GOOG;
    auto df = fixture.makeMinuteDfForDay("2024-01-02", 390);
    fixture.writeParquetForAsset(asset, DataCategory::MinuteBars, df);

    auto fromDate = DateTime::from_date_str("2024-01-02").date();
    auto toDate = DateTime::from_date_str("2024-01-03").date();

    auto result = fixture.fetcher->Fetch(asset, DataCategory::MinuteBars,
                                         fromDate, toDate);

    REQUIRE(result.has_value());
    df = *result;
    CHECK(df.num_rows() == 390); // Full trading day
  }

  SECTION("filters data by date range") {
    auto asset = data_sdk::asset::AssetConstants::instance().MSFT;
    auto df = fixture.makeDailyDf("2024-01-01", 30);
    fixture.writeParquetForAsset(asset, DataCategory::DailyBars, df);

    auto fromDate = DateTime::from_date_str("2024-01-10").date();
    auto toDate = DateTime::from_date_str("2024-01-25").date();

    auto result = fixture.fetcher->Fetch(asset, DataCategory::DailyBars,
                                         fromDate, toDate);

    REQUIRE(result.has_value());
    auto filtered = *result;
    CHECK(filtered.num_rows() > 0);
    CHECK(filtered.num_rows() < 30);
  }

  SECTION("returns error when no files exist") {
    auto asset = data_sdk::asset::AssetConstants::instance().IBM;

    auto fromDate = DateTime::from_date_str("2024-01-01").date();
    auto toDate = DateTime::from_date_str("2024-01-31").date();

    auto result = fixture.fetcher->Fetch(asset, DataCategory::DailyBars,
                                         fromDate, toDate);

    REQUIRE(!result.has_value());
  }

  SECTION("handles missing directory gracefully") {
    auto asset = data_sdk::asset::AssetConstants::instance().SPY;

    auto fromDate = DateTime::from_date_str("2024-01-01").date();
    auto toDate = DateTime::from_date_str("2024-01-31").date();

    auto result = fixture.fetcher->Fetch(asset, DataCategory::MinuteBars,
                                         fromDate, toDate);

    REQUIRE(!result.has_value());
  }

  SECTION("returns sorted data in date order") {
    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;
    auto df = fixture.makeDailyDf("2024-01-01", 15);
    fixture.writeParquetForAsset(asset, DataCategory::DailyBars, df);

    auto fromDate = DateTime::from_date_str("2024-01-01").date();
    auto toDate = DateTime::from_date_str("2024-01-31").date();

    auto result = fixture.fetcher->Fetch(asset, DataCategory::DailyBars,
                                         fromDate, toDate);

    REQUIRE(result.has_value());
    auto out = *result;
    CHECK(out.num_rows() == 15);

    // Check that data is in chronological order
    auto firstDate = out.index()->at(0);
    auto lastDate = out.index()->at(-1);
    CHECK(firstDate <= lastDate);
  }
}

TEST_CASE("ArchiveDataFetcher with different asset types",
          "[archive_fetcher]") {
  ArchiveFetcherTestFixture fixture;
  auto assets = data_sdk::asset::AssetConstants::instance();
  SECTION("handles forex assets") {
    auto eur_usd = assets.EUR_USD;
    auto df = fixture.makeMinuteDfForDay("2024-01-02", 1440);
    fixture.writeParquetForAsset(eur_usd, DataCategory::MinuteBars, df);

    auto fromDate = DateTime::from_date_str("2024-01-02").date();
    auto toDate = DateTime::from_date_str("2024-01-04").date();

    auto result = fixture.fetcher->Fetch(eur_usd, DataCategory::MinuteBars,
                                         fromDate, toDate);

    REQUIRE(result.has_value());
    REQUIRE(result->num_rows() == 1440); // 24 hours of minute data
  }

  SECTION("handles futures assets") {
    auto es = assets.ES;
    auto df = fixture.makeDailyDf("2024-01-01", 20);
    fixture.writeParquetForAsset(es, DataCategory::DailyBars, df);

    auto fromDate = DateTime::from_date_str("2024-01-01").date();
    auto toDate = DateTime::from_date_str("2024-01-31").date();

    auto result =
        fixture.fetcher->Fetch(es, DataCategory::DailyBars, fromDate, toDate);

    REQUIRE(result.has_value());
    CHECK(result->num_rows() == 20);
  }

  SECTION("handles crypto assets") {
    auto btc = assets.BTC_USD;
    auto df = fixture.makeDailyDf("2024-01-01", 31);
    fixture.writeParquetForAsset(btc, DataCategory::DailyBars, df);

    auto fromDate = DateTime::from_date_str("2024-01-01").date();
    auto toDate = DateTime::from_date_str("2024-01-31").date();

    auto result =
        fixture.fetcher->Fetch(btc, DataCategory::DailyBars, fromDate, toDate);

    REQUIRE(result.has_value());
    CHECK(result->num_rows() == 31);
  }
}

TEST_CASE("ArchiveDataFetcher error handling", "[archive_fetcher]") {
  ArchiveFetcherTestFixture fixture;

  SECTION("handles corrupted parquet file") {
    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;

    // Create a corrupted file at expected location
    auto filePath =
        fixture.tempArchive / "daily_bars" /
        epoch_core::AssetClassWrapper::ToLongFormString(asset.GetAssetClass()) /
        (asset.GetSymbolStr() + ".parquet.gzip");
    std::filesystem::create_directories(filePath.parent_path());
    std::ofstream file(filePath);
    file << "This is not a valid parquet file";
    file.close();

    auto fromDate = DateTime::from_date_str("2024-01-01").date();
    auto toDate = DateTime::from_date_str("2024-01-01").date();

    auto result = fixture.fetcher->Fetch(asset, DataCategory::DailyBars,
                                         fromDate, toDate);

    CHECK(!result.has_value());
  }

  SECTION("handles invalid archive path") {
    ArchiveDataFetcher invalidFetcher("/non/existent/path");
    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;

    auto fromDate = DateTime::from_date_str("2024-01-01").date();
    auto toDate = DateTime::from_date_str("2024-01-31").date();

    auto result =
        invalidFetcher.Fetch(asset, DataCategory::DailyBars, fromDate, toDate);

    REQUIRE(!result.has_value());
  }
}

TEST_CASE("ArchiveDataFetcher with custom archive path", "[archive_fetcher]") {
  ArchiveFetcherTestFixture fixture;

  SECTION("uses custom archive path from constructor") {
    auto customPath = std::filesystem::temp_directory_path() / "custom_archive";
    auto asset = data_sdk::asset::AssetConstants::instance().AAPL;
    std::filesystem::create_directories(
        customPath / "DailyBars" /
        epoch_core::AssetClassWrapper::ToLongFormString(asset.GetAssetClass()));

    ArchiveDataFetcher customFetcher(customPath.string());

    // Create test file in custom location
    auto filePath =
        customPath / "DailyBars" /
        epoch_core::AssetClassWrapper::ToLongFormString(asset.GetAssetClass()) /
        (asset.GetSymbolStr() + ".parquet.gzip");

    // Create simple test data
    DataFrame df = make_dataframe<double>(
        epoch_frame::factory::index::make_datetime_index(
            {DateTime::from_date_str("2024-01-01"),
             DateTime::from_date_str("2024-01-02"),
             DateTime::from_date_str("2024-01-03")}),
        {std::vector<double>{100.0, 101.0, 102.0}}, {"c"});
    auto status = epoch_frame::write_parquet(
        df, filePath.string(), {.include_index = true, .index_label = "t"});
    if (!status.ok()) {
      FAIL(status.message());
    }

    auto fromDate = DateTime::from_date_str("2024-01-01").date();
    auto toDate = DateTime::from_date_str("2024-01-31").date();

    auto result =
        customFetcher.Fetch(asset, DataCategory::DailyBars, fromDate, toDate);
    if (!result) {
      FAIL(result.error());
    }
    REQUIRE(result.has_value());
    CHECK(result->num_rows() == 3);

    // Cleanup
    std::filesystem::remove_all(customPath);
  }
}
