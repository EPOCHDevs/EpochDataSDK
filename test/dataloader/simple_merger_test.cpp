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
      DateTime::from_date_str("2024-01-01", "UTC"),
      DateTime::from_date_str("2024-01-02", "UTC"),
      DateTime::from_date_str("2024-01-03", "UTC"),
  };

  std::vector<double> opens = {100.0, 101.0, 102.0};
  std::vector<double> closes = {101.5, 102.5, 103.5};

  auto index = make_datetime_index(dates, "", "UTC");
  auto expected = make_dataframe<double>(index, {opens, closes}, {"o", "c"});

  std::unordered_map<std::string, DataFrame> category_data = {
      {DataCategoryWrapper::ToString(DataCategory::DailyBars), expected}
  };

  auto result = merger.Merge(category_data);

  REQUIRE(result.has_value());
  REQUIRE(result->equals(expected));
}

TEST_CASE("SimpleMerger - Empty Data Error", "[simple_merger]") {
  SimpleMerger merger;
  std::unordered_map<std::string, DataFrame> category_data;

  auto result = merger.Merge(category_data);

  REQUIRE_FALSE(result.has_value());
  REQUIRE(result.error() == "Cannot merge empty data map");
}

TEST_CASE("SimpleMerger - All Normalized (DailyBars + Dividends + Splits)", "[simple_merger]") {
  SimpleMerger merger;

  // Create shared dates for all categories
  std::vector<DateTime> dates = {
      DateTime::from_date_str("2024-01-01", "UTC"),
      DateTime::from_date_str("2024-01-02", "UTC"),
      DateTime::from_date_str("2024-01-03", "UTC"),
  };

  // Create DailyBars DataFrame
  std::vector<double> opens = {100.0, 101.0, 102.0};
  std::vector<double> closes = {101.5, 102.5, 103.5};
  auto daily_index = make_datetime_index(dates, "", "UTC");
  auto daily_df = make_dataframe<double>(daily_index, {opens, closes}, {"o", "c"});

  // Create Dividends DataFrame (prefixed columns) - same dates
  std::vector<double> div_amounts = {0.0, 2.50, 0.0};  // Only 01-02 has dividend
  auto div_index = make_datetime_index(dates, "", "UTC");
  auto div_df = make_dataframe<double>(div_index, {div_amounts}, {"D:cash_amount"});

  // Create Splits DataFrame (prefixed columns) - same dates
  std::vector<double> split_ratios = {0.0, 0.0, 2.0};  // Only 01-03 has split
  auto split_index = make_datetime_index(dates, "", "UTC");
  auto split_df = make_dataframe<double>(split_index, {split_ratios}, {"S:split_ratio"});

  // Merge all three categories
  std::unordered_map<std::string, DataFrame> category_data = {
      {DataCategoryWrapper::ToString(DataCategory::DailyBars), daily_df},
      {DataCategoryWrapper::ToString(DataCategory::Dividends), div_df},
      {DataCategoryWrapper::ToString(DataCategory::Splits), split_df},
  };

  auto result = merger.Merge(category_data);

  REQUIRE(result.has_value());

  // Build expected merged DataFrame
  auto expected_index = make_datetime_index(dates, "", "UTC");
  auto expected = make_dataframe<double>(
      expected_index,
      {opens, closes, div_amounts, split_ratios},
      {"o", "c", "D:cash_amount", "S:split_ratio"}
  );

  REQUIRE(result->equals(expected));
}

TEST_CASE("SimpleMerger - Mixed (MinuteBars + Dividends with First-Timestamp Alignment)", "[simple_merger]") {
  SimpleMerger merger;

  // MinuteBars is non-normalized (intraday timestamps)
  // Dividends is normalized (midnight UTC)
  // This tests mixed merge with first-timestamp alignment (no forward-fill)

  // Create MinuteBars DataFrame with intraday timestamps
  auto base_date = DateTime::from_date_str("2024-01-02", "UTC");
  std::vector<DateTime> minute_times = {
      base_date + std::chrono::hours(9) + std::chrono::minutes(31),
      base_date + std::chrono::hours(9) + std::chrono::minutes(32),
      base_date + std::chrono::hours(9) + std::chrono::minutes(33),
  };
  std::vector<double> opens = {100.0, 100.5, 101.0};
  std::vector<double> closes = {100.5, 101.0, 101.5};

  auto minute_index = make_datetime_index(minute_times, "", "UTC");
  auto minute_df = make_dataframe<double>(minute_index, {opens, closes}, {"o", "c"});

  // Create Dividends DataFrame (midnight UTC for 2024-01-02)
  std::vector<DateTime> div_dates = {
      DateTime::from_date_str("2024-01-02", "UTC"),  // midnight
  };
  std::vector<double> div_amounts = {2.50};

  auto div_index = make_datetime_index(div_dates, "", "UTC");
  auto div_df = make_dataframe<double>(div_index, {div_amounts}, {"D:cash_amount"});

  // Merge MinuteBars (non-normalized) + Dividends (normalized)
  std::unordered_map<std::string, DataFrame> category_data = {
      {DataCategoryWrapper::ToString(DataCategory::MinuteBars), minute_df},
      {DataCategoryWrapper::ToString(DataCategory::Dividends), div_df},
  };

  auto result = merger.Merge(category_data);

  REQUIRE(result.has_value());

  // Build expected: 3 rows (only intraday bars, no midnight row)
  // Dividend value appears ONLY at first intraday timestamp (09:31), NaN elsewhere
  std::vector<DateTime> expected_times = {
      base_date + std::chrono::hours(9) + std::chrono::minutes(31),  // dividend aligned here
      base_date + std::chrono::hours(9) + std::chrono::minutes(32),
      base_date + std::chrono::hours(9) + std::chrono::minutes(33),
  };

  auto expected_index = make_datetime_index(expected_times, "", "UTC");
  auto expected = make_dataframe<double>(
      expected_index,
      {
          {100.0, 100.5, 101.0},                        // o
          {100.5, 101.0, 101.5},                        // c
          {2.50, std::nan(""), std::nan("")}            // D:cash_amount (only at first timestamp)
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
  REQUIRE(news_metadata.index_normalized == true);

  // Verify Dividends is normalized (date-based events)
  auto div_metadata = MetadataRegistry::GetMetadataForCategory(DataCategory::Dividends);
  REQUIRE(div_metadata.index_normalized == true);
}

TEST_CASE("SimpleMerger - Verify IsSameNormalizationPolicy", "[simple_merger]") {
  SimpleMerger merger;

  // Create test data with different column names for each category
  std::vector<DateTime> dates = {DateTime::from_date_str("2024-01-01", "UTC")};
  auto index = make_datetime_index(dates, "", "UTC");

  auto df1 = make_dataframe<double>(index, {std::vector<double>{100.0}}, {"price"});
  auto df2 = make_dataframe<double>(index, {std::vector<double>{2.5}}, {"D:amount"});
  auto df3 = make_dataframe<double>(index, {std::vector<double>{2.0}}, {"S:ratio"});

  // Test: All normalized categories (DailyBars, Dividends, Splits)
  std::unordered_map<std::string, DataFrame> all_normalized = {
      {DataCategoryWrapper::ToString(DataCategory::DailyBars), df1},
      {DataCategoryWrapper::ToString(DataCategory::Dividends), df2},
      {DataCategoryWrapper::ToString(DataCategory::Splits), df3},
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
  std::unordered_map<std::string, DataFrame> single_cat = {
      {DataCategoryWrapper::ToString(DataCategory::News), df1},
  };

  result = merger.Merge(single_cat);
  REQUIRE(result.has_value());
  REQUIRE(result->equals(df1));
}

TEST_CASE("SimpleMerger - Mixed Multi-Day First-Timestamp Alignment", "[simple_merger]") {
  SimpleMerger merger;

  // Test alignment across multiple days to ensure each day's normalized data
  // is aligned to the first intraday timestamp of that day

  // Create MinuteBars for multiple days
  auto day1 = DateTime::from_date_str("2024-01-02", "UTC");
  auto day2 = DateTime::from_date_str("2024-01-03", "UTC");

  std::vector<DateTime> minute_times = {
      // Day 1: First bar at 09:31
      day1 + std::chrono::hours(9) + std::chrono::minutes(31),
      day1 + std::chrono::hours(9) + std::chrono::minutes(32),
      day1 + std::chrono::hours(10) + std::chrono::minutes(0),
      // Day 2: First bar at 09:30 (different from day 1)
      day2 + std::chrono::hours(9) + std::chrono::minutes(30),
      day2 + std::chrono::hours(9) + std::chrono::minutes(35),
      day2 + std::chrono::hours(10) + std::chrono::minutes(0),
  };
  std::vector<double> prices = {100.0, 100.5, 101.0, 102.0, 102.5, 103.0};

  auto minute_index = make_datetime_index(minute_times, "", "UTC");
  auto minute_df = make_dataframe<double>(minute_index, {prices}, {"close"});

  // Create DailyBars (normalized) for same days
  std::vector<DateTime> daily_dates = {
      DateTime::from_date_str("2024-01-02", "UTC"),
      DateTime::from_date_str("2024-01-03", "UTC"),
  };
  std::vector<double> daily_volumes = {1000000.0, 1100000.0};

  auto daily_index = make_datetime_index(daily_dates, "", "UTC");
  auto daily_df = make_dataframe<double>(daily_index, {daily_volumes}, {"volume"});

  // Merge
  std::unordered_map<std::string, DataFrame> category_data = {
      {DataCategoryWrapper::ToString(DataCategory::MinuteBars), minute_df},
      {DataCategoryWrapper::ToString(DataCategory::DailyBars), daily_df},
  };

  auto result = merger.Merge(category_data);
  REQUIRE(result.has_value());

  // Expected: Daily volume appears only at first minute bar of each day
  auto expected_index = make_datetime_index(minute_times, "", "UTC");
  auto expected = make_dataframe<double>(
      expected_index,
      {
          {100.0, 100.5, 101.0, 102.0, 102.5, 103.0},                                        // close
          {1000000.0, std::nan(""), std::nan(""), 1100000.0, std::nan(""), std::nan("")}     // volume (only at first bars)
      },
      {"close", "volume"}
  );

  REQUIRE(result->equals(expected));
}

TEST_CASE("SimpleMerger - Mixed with Non-Overlapping Dates", "[simple_merger]") {
  SimpleMerger merger;

  // Test when normalized data has dates without intraday data
  // Those dates should be filtered out

  // Create MinuteBars for 2024-01-02 only
  auto day1 = DateTime::from_date_str("2024-01-02", "UTC");
  std::vector<DateTime> minute_times = {
      day1 + std::chrono::hours(9) + std::chrono::minutes(31),
      day1 + std::chrono::hours(9) + std::chrono::minutes(32),
  };
  std::vector<double> prices = {100.0, 100.5};

  auto minute_index = make_datetime_index(minute_times, "", "UTC");
  auto minute_df = make_dataframe<double>(minute_index, {prices}, {"close"});

  // Create DailyBars for 2024-01-01, 2024-01-02, and 2024-01-03
  // Only 2024-01-02 overlaps with intraday data
  std::vector<DateTime> daily_dates = {
      DateTime::from_date_str("2024-01-01", "UTC"),  // No intraday data
      DateTime::from_date_str("2024-01-02", "UTC"),  // Has intraday data
      DateTime::from_date_str("2024-01-03", "UTC"),  // No intraday data
  };
  std::vector<double> daily_volumes = {900000.0, 1000000.0, 1100000.0};

  auto daily_index = make_datetime_index(daily_dates, "", "UTC");
  auto daily_df = make_dataframe<double>(daily_index, {daily_volumes}, {"volume"});

  // Merge
  std::unordered_map<std::string, DataFrame> category_data = {
      {DataCategoryWrapper::ToString(DataCategory::MinuteBars), minute_df},
      {DataCategoryWrapper::ToString(DataCategory::DailyBars), daily_df},
  };

  auto result = merger.Merge(category_data);
  REQUIRE(result.has_value());

  // Expected: Only 2 rows (from intraday), volume only at first bar of 2024-01-02
  // 2024-01-01 and 2024-01-03 daily data are filtered out
  auto expected_index = make_datetime_index(minute_times, "", "UTC");
  auto expected = make_dataframe<double>(
      expected_index,
      {
          {100.0, 100.5},                        // close
          {1000000.0, std::nan("")}              // volume (only at first bar)
      },
      {"close", "volume"}
  );

  REQUIRE(result->equals(expected));
}

TEST_CASE("SimpleMerger - Mixed with No Overlapping Dates", "[simple_merger]") {
  SimpleMerger merger;

  // Test when normalized data has NO dates with intraday data
  // Should return just the intraday data

  // Create MinuteBars for 2024-01-02
  auto day1 = DateTime::from_date_str("2024-01-02", "UTC");
  std::vector<DateTime> minute_times = {
      day1 + std::chrono::hours(9) + std::chrono::minutes(31),
      day1 + std::chrono::hours(9) + std::chrono::minutes(32),
  };
  std::vector<double> prices = {100.0, 100.5};

  auto minute_index = make_datetime_index(minute_times, "", "UTC");
  auto minute_df = make_dataframe<double>(minute_index, {prices}, {"close"});

  // Create DailyBars for completely different dates
  std::vector<DateTime> daily_dates = {
      DateTime::from_date_str("2024-01-10", "UTC"),
      DateTime::from_date_str("2024-01-11", "UTC"),
  };
  std::vector<double> daily_volumes = {1000000.0, 1100000.0};

  auto daily_index = make_datetime_index(daily_dates, "", "UTC");
  auto daily_df = make_dataframe<double>(daily_index, {daily_volumes}, {"volume"});

  // Merge
  std::unordered_map<std::string, DataFrame> category_data = {
      {DataCategoryWrapper::ToString(DataCategory::MinuteBars), minute_df},
      {DataCategoryWrapper::ToString(DataCategory::DailyBars), daily_df},
  };

  auto result = merger.Merge(category_data);
  REQUIRE(result.has_value());

  // Expected: Just the intraday data (no daily columns since no overlap)
  REQUIRE(result->equals(minute_df));
}
