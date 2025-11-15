#include <catch2/catch_test_macros.hpp>
#include "../../src/dataloader/simple_merger.hpp"
#include "epoch_data_sdk/dataloader/metadata_registry.hpp"
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/array_factory.h>
#include <epoch_frame/datetime.h>
#include <spdlog/spdlog.h>
#include <chrono>

using namespace data_sdk;
using namespace data_sdk::dataloader;
using namespace epoch_frame;
using namespace epoch_frame::factory::index;
using namespace epoch_frame::factory::array;
using epoch_core::DataCategory;
using arrow::ChunkedArrayPtr;

TEST_CASE("SimpleMerger - Single Category Pass-Through", "[simple_merger]") {
  SimpleMerger merger;

  // Create a simple DailyBars DataFrame
  std::vector<DateTime> dates = {
      DateTime::from_date_str("2024-01-01"),
      DateTime::from_date_str("2024-01-02"),
      DateTime::from_date_str("2024-01-03"),
  };

  std::vector<double> opens = {100.0, 101.0, 102.0};
  std::vector<double> closes = {101.5, 102.5, 103.5};

  auto index = make_datetime_index(dates);
  auto expected = make_dataframe<double>(index, {opens, closes}, {"o", "c"});

  std::unordered_map<DataCategory, DataFrame> category_data = {
      {DataCategory::DailyBars, expected}
  };

  auto result = merger.Merge(category_data);

  REQUIRE(result.has_value());
  REQUIRE(result->equals(expected));
}

TEST_CASE("SimpleMerger - Empty Data Error", "[simple_merger]") {
  SimpleMerger merger;
  std::unordered_map<DataCategory, DataFrame> category_data;

  auto result = merger.Merge(category_data);

  REQUIRE_FALSE(result.has_value());
  REQUIRE(result.error() == "Cannot merge empty category data");
}

TEST_CASE("SimpleMerger - All Normalized (DailyBars + Dividends + Splits)", "[simple_merger]") {
  SimpleMerger merger;

  // Create shared dates for all categories
  std::vector<DateTime> dates = {
      DateTime::from_date_str("2024-01-01"),
      DateTime::from_date_str("2024-01-02"),
      DateTime::from_date_str("2024-01-03"),
  };

  // Create DailyBars DataFrame
  std::vector<double> opens = {100.0, 101.0, 102.0};
  std::vector<double> closes = {101.5, 102.5, 103.5};
  auto daily_index = make_datetime_index(dates);
  auto daily_df = make_dataframe<double>(daily_index, {opens, closes}, {"o", "c"});

  // Create Dividends DataFrame (prefixed columns) - same dates
  std::vector<double> div_amounts = {0.0, 2.50, 0.0};  // Only 01-02 has dividend
  auto div_index = make_datetime_index(dates);
  auto div_df = make_dataframe<double>(div_index, {div_amounts}, {"D:cash_amount"});

  // Create Splits DataFrame (prefixed columns) - same dates
  std::vector<double> split_ratios = {0.0, 0.0, 2.0};  // Only 01-03 has split
  auto split_index = make_datetime_index(dates);
  auto split_df = make_dataframe<double>(split_index, {split_ratios}, {"S:split_ratio"});

  // Merge all three categories
  std::unordered_map<DataCategory, DataFrame> category_data = {
      {DataCategory::DailyBars, daily_df},
      {DataCategory::Dividends, div_df},
      {DataCategory::Splits, split_df},
  };

  auto result = merger.Merge(category_data);

  REQUIRE(result.has_value());

  // Build expected merged DataFrame
  auto expected_index = make_datetime_index(dates);
  auto expected = make_dataframe<double>(
      expected_index,
      {opens, closes, div_amounts, split_ratios},
      {"o", "c", "D:cash_amount", "S:split_ratio"}
  );

  REQUIRE(result->equals(expected));
}

TEST_CASE("SimpleMerger - Mixed (MinuteBars + Dividends with Forward-Fill)", "[simple_merger]") {
  SimpleMerger merger;

  // MinuteBars is non-normalized (intraday timestamps)
  // Dividends is normalized (midnight UTC)
  // This tests mixed merge with forward-fill

  // Create MinuteBars DataFrame with intraday timestamps
  auto base_date = DateTime::from_date_str("2024-01-02");
  std::vector<DateTime> minute_times = {
      base_date + std::chrono::hours(9) + std::chrono::minutes(31),
      base_date + std::chrono::hours(9) + std::chrono::minutes(32),
      base_date + std::chrono::hours(9) + std::chrono::minutes(33),
  };
  std::vector<double> opens = {100.0, 100.5, 101.0};
  std::vector<double> closes = {100.5, 101.0, 101.5};

  auto minute_index = make_datetime_index(minute_times);
  auto minute_df = make_dataframe<double>(minute_index, {opens, closes}, {"o", "c"});

  // Create Dividends DataFrame (midnight UTC for 2024-01-02)
  std::vector<DateTime> div_dates = {
      DateTime::from_date_str("2024-01-02"),  // midnight
  };
  std::vector<double> div_amounts = {2.50};

  auto div_index = make_datetime_index(div_dates);
  auto div_df = make_dataframe<double>(div_index, {div_amounts}, {"D:cash_amount"});

  // Merge MinuteBars (non-normalized) + Dividends (normalized)
  std::unordered_map<DataCategory, DataFrame> category_data = {
      {DataCategory::MinuteBars, minute_df},
      {DataCategory::Dividends, div_df},
  };

  auto result = merger.Merge(category_data);

  REQUIRE(result.has_value());

  // Build expected: outer join creates 4 rows (midnight + 3 intraday)
  // Forward-fill propagates dividend value from midnight to all intraday bars
  std::vector<DateTime> expected_times = {
      base_date,  // midnight (dividend only)
      base_date + std::chrono::hours(9) + std::chrono::minutes(31),
      base_date + std::chrono::hours(9) + std::chrono::minutes(32),
      base_date + std::chrono::hours(9) + std::chrono::minutes(33),
  };

  auto expected_index = make_datetime_index(expected_times);
  auto expected = make_dataframe<double>(
      expected_index,
      {
          {std::nan(""), 100.0, 100.5, 101.0},     // o (NaN at midnight)
          {std::nan(""), 100.5, 101.0, 101.5},     // c (NaN at midnight)
          {2.50, 2.50, 2.50, 2.50}                 // D:cash_amount (forward-filled)
      },
      {"o", "c", "D:cash_amount"}
  );

  REQUIRE(result->equals(expected));
}

TEST_CASE("SimpleMerger - All Normalized Categories from Metadata", "[simple_merger]") {
  SimpleMerger merger;

  // Use metadata to verify we're testing with real schemas
  auto daily_metadata = MetadataRegistry::GetMetadataForCategory(DataCategory::DailyBars);
  auto div_metadata = MetadataRegistry::GetMetadataForCategory(DataCategory::Dividends);
  auto split_metadata = MetadataRegistry::GetMetadataForCategory(DataCategory::Splits);

  // Verify all are normalized
  REQUIRE(daily_metadata.index_normalized == true);
  REQUIRE(div_metadata.index_normalized == true);
  REQUIRE(split_metadata.index_normalized == true);

  // Verify prefixes match expected values
  REQUIRE(daily_metadata.category_prefix == "");
  REQUIRE(div_metadata.category_prefix == "D:");
  REQUIRE(split_metadata.category_prefix == "S:");

  // Verify data types
  REQUIRE(daily_metadata.data_type == "aggregates");
  REQUIRE(div_metadata.data_type == "dividends");
  REQUIRE(split_metadata.data_type == "splits");
}

TEST_CASE("SimpleMerger - Mixed Normalized Status Check", "[simple_merger]") {
  // Verify MinuteBars is non-normalized (intraday timestamps)
  auto minute_metadata = MetadataRegistry::GetMetadataForCategory(DataCategory::MinuteBars);
  REQUIRE(minute_metadata.index_normalized == false);

  // Verify DailyBars is normalized (midnight UTC)
  auto daily_metadata = MetadataRegistry::GetMetadataForCategory(DataCategory::DailyBars);
  REQUIRE(daily_metadata.index_normalized == true);

  // Verify News is non-normalized (timestamped events)
  auto news_metadata = MetadataRegistry::GetMetadataForCategory(DataCategory::News);
  REQUIRE(news_metadata.index_normalized == false);

  // Verify Dividends is normalized (date-based events)
  auto div_metadata = MetadataRegistry::GetMetadataForCategory(DataCategory::Dividends);
  REQUIRE(div_metadata.index_normalized == true);
}

TEST_CASE("SimpleMerger - Verify IsSameNormalizationPolicy", "[simple_merger]") {
  SimpleMerger merger;

  // Create test data with different column names for each category
  std::vector<DateTime> dates = {DateTime::from_date_str("2024-01-01")};
  auto index = make_datetime_index(dates);

  auto df1 = make_dataframe<double>(index, {std::vector<double>{100.0}}, {"price"});
  auto df2 = make_dataframe<double>(index, {std::vector<double>{2.5}}, {"D:amount"});
  auto df3 = make_dataframe<double>(index, {std::vector<double>{2.0}}, {"S:ratio"});

  // Test: All normalized categories (DailyBars, Dividends, Splits)
  std::unordered_map<DataCategory, DataFrame> all_normalized = {
      {DataCategory::DailyBars, df1},
      {DataCategory::Dividends, df2},
      {DataCategory::Splits, df3},
  };

  auto result = merger.Merge(all_normalized);
  REQUIRE(result.has_value());

  auto expected = make_dataframe<double>(
      index,
      {std::vector<double>{100.0}, std::vector<double>{2.5}, std::vector<double>{2.0}},
      {"price", "D:amount", "S:ratio"}
  );
  REQUIRE(result->equals(expected));

  // Test: Single category (should always work)
  std::unordered_map<DataCategory, DataFrame> single_cat = {
      {DataCategory::News, df1},
  };

  result = merger.Merge(single_cat);
  REQUIRE(result.has_value());
  REQUIRE(result->equals(df1));
}
