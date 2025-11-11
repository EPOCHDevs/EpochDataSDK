#include "epoch_frame/datetime.h"
#include "epoch_frame/factory/dataframe_factory.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/factory/scalar_factory.h"
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include "dataloader/cache/cache_utils.h"
#include <epoch_frame/dataframe.h>
#include <epoch_frame/series.h>
#include <filesystem>
#include <fstream>
#include <epoch_data_sdk/common/constants.hpp>
#include <epoch_data_sdk/model/asset/asset_constants.hpp>

using namespace data_sdk::dataloader;
using namespace epoch_frame;
using namespace Catch::Matchers;

class CacheTestFixture {
public:
  CacheTestFixture() {
    // Create temp directory for tests
    tempDir = std::filesystem::temp_directory_path() / "epoch_cache_test";
    std::filesystem::create_directories(tempDir);
  }

  ~CacheTestFixture() {
    // Clean up temp directory
    std::filesystem::remove_all(tempDir);
  }

  DataFrame createTestDataFrame(const std::string &startDate = "2024-01-01",
                                int numDays = 10) {
    std::vector<DateTime> dates;
    std::vector<double> opens, highs, lows, closes, volumes;

    auto start = DateTime::from_date_str(startDate).date();
    for (int i = 0; i < numDays; ++i) {
      dates.push_back(DateTime(start) + chrono_days(i));
      opens.push_back(100.0 + i);
      highs.push_back(105.0 + i);
      lows.push_back(95.0 + i);
      closes.push_back(102.0 + i);
      volumes.push_back(1000000.0 + i * 10000);
    }

    return epoch_frame::make_dataframe<double>(
        epoch_frame::factory::index::make_datetime_index(dates),
        {opens, highs, lows, closes, volumes}, {"o", "h", "l", "c", "v"});
  }

  std::filesystem::path tempDir;
};

TEST_CASE("cache::MakeCachePath", "[cache_utils]") {
  CacheTestFixture fixture;
  const auto &assets =
      data_sdk::asset::AssetConstants::instance();

  SECTION("generates correct path for minute bars") {
    auto asset = assets.AAPL;
    auto path =
        cache::MakeCachePath(fixture.tempDir, asset, DataCategory::MinuteBars);

    CHECK(path.parent_path() == fixture.tempDir / "MinuteBars" / "Stocks");
    CHECK_THAT(path.filename().string(), ContainsSubstring("AAPL"));
    CHECK_THAT(path.extension().string(), Equals(".arrow"));
  }

  SECTION("generates correct path for daily bars") {
    auto asset = assets.GOOG;
    auto path =
        cache::MakeCachePath(fixture.tempDir, asset, DataCategory::DailyBars);

    CHECK(path.parent_path() == fixture.tempDir / "DailyBars" / "Stocks");
    CHECK_THAT(path.filename().string(), ContainsSubstring("GOOG"));
    CHECK_THAT(path.extension().string(), Equals(".arrow"));
  }

  SECTION("creates directory if it doesn't exist") {
    auto asset = assets.MSFT;
    auto path = cache::MakeCachePath(fixture.tempDir / "new_subdir", asset,
                                     DataCategory::MinuteBars);

    CHECK(std::filesystem::exists(path.parent_path()));
  }
}

TEST_CASE("cache::Write and cache::TryLoad", "[cache_utils]") {
  CacheTestFixture fixture;
  const auto &assets =
      data_sdk::asset::AssetConstants::instance();

  SECTION("write and read back DataFrame") {
    auto asset = assets.SPY;
    auto df = fixture.createTestDataFrame();
    auto cachePath =
        cache::MakeCachePath(fixture.tempDir, asset, DataCategory::DailyBars);

    // Write to cache
    cache::Write(fixture.tempDir, cachePath, true, df);
    CHECK(std::filesystem::exists(cachePath));

    // Read back
    std::filesystem::path outPath;
    auto loaded = cache::TryLoad(fixture.tempDir, asset,
                                 DataCategory::DailyBars, 3600, outPath);

    REQUIRE(loaded.has_value());
    CHECK(loaded->num_rows() == df.num_rows());
    CHECK(loaded->num_cols() == df.num_cols());
    CHECK(outPath == cachePath);
  }

  SECTION("returns empty optional when file doesn't exist") {
    auto asset = assets.TSLA;
    std::filesystem::path outPath;
    auto loaded = cache::TryLoad(fixture.tempDir, asset,
                                 DataCategory::MinuteBars, 3600, outPath);

    CHECK(!loaded.has_value());
  }

  SECTION("returns empty optional when cache is expired") {
    auto asset = assets.AMZN;
    auto df = fixture.createTestDataFrame();
    auto cachePath =
        cache::MakeCachePath(fixture.tempDir, asset, DataCategory::DailyBars);

    // Write to cache
    cache::Write(fixture.tempDir, cachePath, true, df);

    // Modify file time to make it old
    auto oldTime =
        std::filesystem::file_time_type::clock::now() - std::chrono::hours(2);
    std::filesystem::last_write_time(cachePath, oldTime);

    // Try to load with 1 hour TTL (should be expired)
    std::filesystem::path outPath;
    auto loaded = cache::TryLoad(fixture.tempDir, asset,
                                 DataCategory::DailyBars, 3600, outPath);

    CHECK(!loaded.has_value());
  }

  SECTION("respects TTL for fresh cache") {
    auto asset = assets.IBM;
    auto df = fixture.createTestDataFrame();
    auto cachePath =
        cache::MakeCachePath(fixture.tempDir, asset, DataCategory::MinuteBars);

    // Write to cache
    cache::Write(fixture.tempDir, cachePath, true, df);

    // Load with long TTL (should succeed)
    std::filesystem::path outPath;
    auto loaded = cache::TryLoad(fixture.tempDir, asset,
                                 DataCategory::MinuteBars, 7200, outPath);

    REQUIRE(loaded.has_value());
    CHECK(loaded->num_rows() == df.num_rows());
  }

  SECTION("write with enableCache=false doesn't create file") {
    auto asset = assets.AA;
    auto df = fixture.createTestDataFrame();
    auto cachePath =
        cache::MakeCachePath(fixture.tempDir, asset, DataCategory::DailyBars);

    // Write with caching disabled
    cache::Write(fixture.tempDir, cachePath, false, df);

    CHECK(!std::filesystem::exists(cachePath));
  }

  SECTION("handles corrupted cache file gracefully") {
    auto asset = assets.AAPL;
    auto cachePath =
        cache::MakeCachePath(fixture.tempDir, asset, DataCategory::DailyBars);

    // Create a corrupted file
    std::filesystem::create_directories(cachePath.parent_path());
    std::ofstream file(cachePath);
    file << "This is not valid Arrow format data";
    file.close();

    // Try to load corrupted cache
    std::filesystem::path outPath;
    auto loaded = cache::TryLoad(fixture.tempDir, asset,
                                 DataCategory::DailyBars, 3600, outPath);

    CHECK(!loaded.has_value());
  }
}

TEST_CASE("cache::Write overwrites existing file", "[cache_utils]") {
  CacheTestFixture fixture;
  const auto &assets =
      data_sdk::asset::AssetConstants::instance();

  auto asset = assets.MSFT;
  auto df1 = fixture.createTestDataFrame("2024-01-01", 5);
  auto df2 = fixture.createTestDataFrame("2024-02-01", 10);
  auto cachePath =
      cache::MakeCachePath(fixture.tempDir, asset, DataCategory::DailyBars);

  // Write first dataframe
  cache::Write(fixture.tempDir, cachePath, true, df1);

  // Write second dataframe (should overwrite)
  cache::Write(fixture.tempDir, cachePath, true, df2);

  // Load and verify it's the second dataframe
  std::filesystem::path outPath;
  auto loaded = cache::TryLoad(fixture.tempDir, asset, DataCategory::DailyBars,
                               3600, outPath);

  REQUIRE(loaded.has_value());
  CHECK(loaded->num_rows() == 10);
  // Check the date to ensure it's the second dataframe
  auto firstDate = loaded->index()->at(0).to_datetime().date();
  CHECK(firstDate.repr() == "2024-02-01");
}

TEST_CASE("cache handles different asset types", "[cache_utils]") {
  CacheTestFixture fixture;
  const auto &assets =
      data_sdk::asset::AssetConstants::instance();

  SECTION("handles forex assets") {
    auto fxAsset = assets.EUR_USD;

    auto df = fixture.createTestDataFrame();
    auto cachePath = cache::MakeCachePath(fixture.tempDir, fxAsset,
                                          DataCategory::MinuteBars);

    cache::Write(fixture.tempDir, cachePath, true, df);

    std::filesystem::path outPath;
    auto loaded = cache::TryLoad(fixture.tempDir, fxAsset,
                                 DataCategory::MinuteBars, 3600, outPath);

    REQUIRE(loaded.has_value());
    CHECK(loaded->num_rows() == df.num_rows());
  }

  SECTION("handles futures assets") {
    auto futAsset = assets.ES;

    auto df = fixture.createTestDataFrame();
    auto cachePath = cache::MakeCachePath(fixture.tempDir, futAsset,
                                          DataCategory::DailyBars);

    cache::Write(fixture.tempDir, cachePath, true, df);

    std::filesystem::path outPath;
    auto loaded = cache::TryLoad(fixture.tempDir, futAsset,
                                 DataCategory::DailyBars, 3600, outPath);

    REQUIRE(loaded.has_value());
    CHECK(loaded->num_rows() == df.num_rows());
  }
}
