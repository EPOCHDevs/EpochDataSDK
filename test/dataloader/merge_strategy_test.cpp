#include <catch2/catch_test_macros.hpp>
#include "dataloader/cache/merge_strategy.h"
#include <epoch_frame/dataframe.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <chrono>

using namespace data_sdk::dataloader;
using namespace data_sdk::dataloader::cache;
using namespace epoch_frame;

// Helper to create test datetime
DateTime createDateTime(const std::string& date_str, int hour = 0, int minute = 0) {
  auto dt = DateTime::from_date_str(date_str);
  if (hour > 0 || minute > 0) {
    return dt + std::chrono::hours(hour) + std::chrono::minutes(minute);
  }
  return dt;
}

// Helper to create test data with intraday timestamps
DataFrame createIntradayData(const std::string& base_date,
                             const std::vector<int>& hours,
                             const std::vector<std::string>& values) {
  std::vector<DateTime> timestamps;
  for (int hour : hours) {
    timestamps.push_back(createDateTime(base_date, hour, 30));
  }

  return epoch_frame::make_dataframe<std::string>(
      epoch_frame::factory::index::make_datetime_index(timestamps),
      {values}, {std::string("value")});
}

// Helper to create multi-day data
DataFrame createMultiDayData(const std::vector<std::string>& dates,
                             const std::vector<int>& hours,
                             const std::vector<std::string>& values) {
  std::vector<DateTime> timestamps;
  for (size_t i = 0; i < dates.size(); ++i) {
    timestamps.push_back(createDateTime(dates[i], hours[i], 30));
  }

  return epoch_frame::make_dataframe<std::string>(
      epoch_frame::factory::index::make_datetime_index(timestamps),
      {values}, {std::string("headline")});
}

TEST_CASE("extractDate - Convert timestamp to midnight UTC", "[merge_strategy]") {
  using namespace std::chrono;

  SECTION("extracts date from morning timestamp") {
    // 2024-01-15 10:30:00 UTC
    auto dt = createDateTime("2024-01-15", 10, 30);
    auto ts = dt.timestamp().value;
    auto date_ts = extractDate(ts);

    // Should be 2024-01-15 00:00:00 UTC
    auto result_dt = DateTime::fromtimestamp(date_ts, "UTC");

    // Verify it's midnight by checking timestamp modulo (day in nanoseconds)
    auto day_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::hours(24)).count();
    REQUIRE(date_ts % day_ns == 0);
    REQUIRE(result_dt.date().repr() == "2024-01-15");
  }

  SECTION("extracts date from evening timestamp") {
    // 2024-01-15 23:45:00 UTC
    auto dt = createDateTime("2024-01-15", 23, 45);
    auto ts = dt.timestamp().value;
    auto date_ts = extractDate(ts);

    // Should still be 2024-01-15 00:00:00 UTC
    auto result_dt = DateTime::fromtimestamp(date_ts, "UTC");
    REQUIRE(result_dt.date().repr() == "2024-01-15");
  }

  SECTION("midnight timestamp remains unchanged") {
    // 2024-01-15 00:00:00 UTC
    auto dt = createDateTime("2024-01-15", 0, 0);
    auto ts = dt.timestamp().value;
    auto date_ts = extractDate(ts);

    REQUIRE(ts == date_ts);
  }
}

TEST_CASE("normalizeForIntradayMerge - Group by date and take first",
          "[merge_strategy]") {

  SECTION("single event on single day") {
    auto df = createIntradayData("2024-01-15", {10}, {"News A"});
    auto result = normalizeForIntradayMerge(df, "news");

    REQUIRE(result.num_rows() == 1);
    REQUIRE(result.contains("value"));
    REQUIRE(result.contains("news_timestamp"));
  }

  SECTION("multiple events on single day - takes first") {
    auto df = createMultiDayData(
        {"2024-01-15", "2024-01-15", "2024-01-15"},
        {10, 14, 16},
        {"News A", "News B", "News C"});

    auto result = normalizeForIntradayMerge(df, "news");

    // Should group to 1 row (first event of the day)
    REQUIRE(result.num_rows() == 1);

    // Verify column exists
    REQUIRE(result.contains("headline"));
    REQUIRE(result.contains("news_timestamp"));
  }

  SECTION("events across multiple days") {
    auto df = createMultiDayData(
        {"2024-01-15", "2024-01-15", "2024-01-16", "2024-01-16"},
        {10, 14, 9, 15},
        {"Day1_A", "Day1_B", "Day2_A", "Day2_B"});

    auto result = normalizeForIntradayMerge(df, "news");

    // Should have 2 rows (one per day, first event each)
    REQUIRE(result.num_rows() == 2);
    REQUIRE(result.contains("headline"));
    REQUIRE(result.contains("news_timestamp"));
  }

  SECTION("empty dataframe returns empty") {
    DataFrame empty = epoch_frame::make_dataframe<std::string>(
        epoch_frame::factory::index::make_datetime_index(std::vector<int64_t>{}),
        {std::vector<std::string>{}}, {std::string("value")});

    auto result = normalizeForIntradayMerge(empty, "news");
    REQUIRE(result.empty());
  }

  SECTION("preserves original timestamp in dedicated column") {
    auto df = createIntradayData("2024-01-15", {10}, {"News A"});
    auto result = normalizeForIntradayMerge(df, "news");

    // Check news_timestamp column exists
    REQUIRE(result.contains("news_timestamp"));
    REQUIRE(result.num_rows() == 1);

    // Verify index is normalized to midnight
    auto idx_ts = result.index()->at(0).timestamp().value;
    auto day_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::hours(24)).count();
    REQUIRE(idx_ts % day_ns == 0);  // Index should be midnight
  }
}

TEST_CASE("normalizeForDailyMerge - Preserve all events with date index",
          "[merge_strategy]") {

  SECTION("single event on single day") {
    auto df = createIntradayData("2024-01-15", {10}, {"News A"});
    auto result = normalizeForDailyMerge(df, "news");

    REQUIRE(result.num_rows() == 1);
    REQUIRE(result.contains("value"));
    REQUIRE(result.contains("news_timestamp"));
  }

  SECTION("multiple events on single day - preserves ALL") {
    auto df = createMultiDayData(
        {"2024-01-15", "2024-01-15", "2024-01-15"},
        {10, 14, 16},
        {"News A", "News B", "News C"});

    auto result = normalizeForDailyMerge(df, "news");

    // Should preserve all 3 events
    REQUIRE(result.num_rows() == 3);
    REQUIRE(result.contains("headline"));
    REQUIRE(result.contains("news_timestamp"));
  }

  SECTION("events across multiple days - preserves all") {
    auto df = createMultiDayData(
        {"2024-01-15", "2024-01-15", "2024-01-16", "2024-01-16", "2024-01-16"},
        {10, 14, 9, 12, 15},
        {"D1_A", "D1_B", "D2_A", "D2_B", "D2_C"});

    auto result = normalizeForDailyMerge(df, "news");

    // Should have all 5 rows
    REQUIRE(result.num_rows() == 5);
    REQUIRE(result.contains("headline"));
    REQUIRE(result.contains("news_timestamp"));
  }

  SECTION("empty dataframe returns empty") {
    DataFrame empty = epoch_frame::make_dataframe<std::string>(
        epoch_frame::factory::index::make_datetime_index(std::vector<int64_t>{}),
        {std::vector<std::string>{}}, {std::string("value")});

    auto result = normalizeForDailyMerge(empty, "news");
    REQUIRE(result.empty());
  }

  SECTION("preserves original timestamps in dedicated column") {
    auto df = createMultiDayData(
        {"2024-01-15", "2024-01-15"},
        {10, 14},
        {"News A", "News B"});

    auto result = normalizeForDailyMerge(df, "news");

    REQUIRE(result.contains("news_timestamp"));
    REQUIRE(result.num_rows() == 2);
  }

  SECTION("index is normalized to dates (midnight UTC)") {
    auto df = createMultiDayData(
        {"2024-01-15", "2024-01-15"},
        {10, 14},
        {"News A", "News B"});

    auto result = normalizeForDailyMerge(df, "news");

    // Check all index timestamps are at midnight
    auto day_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::hours(24)).count();

    for (int64_t i = 0; i < static_cast<int64_t>(result.num_rows()); ++i) {
      auto ts = result.index()->at(i).timestamp().value;
      REQUIRE(ts % day_ns == 0);  // Should be midnight
    }
  }
}

TEST_CASE("normalizeForIntradayMerge vs normalizeForDailyMerge - Behavior comparison",
          "[merge_strategy]") {

  SECTION("same input produces different row counts") {
    // 3 events on same day
    auto df = createMultiDayData(
        {"2024-01-15", "2024-01-15", "2024-01-15"},
        {10, 14, 16},
        {"A", "B", "C"});

    auto intraday_result = normalizeForIntradayMerge(df, "news");
    auto daily_result = normalizeForDailyMerge(df, "news");

    // Intraday: groups to 1 row (first)
    REQUIRE(intraday_result.num_rows() == 1);

    // Daily: preserves all 3 rows
    REQUIRE(daily_result.num_rows() == 3);
  }

  SECTION("both preserve original timestamps") {
    auto df = createIntradayData("2024-01-15", {10}, {"News"});

    auto intraday_result = normalizeForIntradayMerge(df, "news");
    auto daily_result = normalizeForDailyMerge(df, "news");

    REQUIRE(intraday_result.contains("news_timestamp"));
    REQUIRE(daily_result.contains("news_timestamp"));
  }

  SECTION("both normalize index to midnight") {
    auto df = createIntradayData("2024-01-15", {15}, {"News"});

    auto intraday_result = normalizeForIntradayMerge(df, "news");
    auto daily_result = normalizeForDailyMerge(df, "news");

    // Both should have midnight index
    auto day_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::hours(24)).count();

    auto check_midnight = [day_ns](const DataFrame& result) {
      auto ts = result.index()->at(0).timestamp().value;
      return (ts % day_ns) == 0;
    };

    REQUIRE(check_midnight(intraday_result));
    REQUIRE(check_midnight(daily_result));
  }
}
