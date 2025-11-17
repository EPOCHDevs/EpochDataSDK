#include <catch2/catch_test_macros.hpp>
#include "dataloader/cache/dedup_utils.h"
#include <epoch_data_sdk/model/asset/asset_constants.hpp>
#include <epoch_data_sdk/common/enums.hpp>
#include <epoch_frame/dataframe.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <spdlog/spdlog.h>

using namespace data_sdk;
using namespace data_sdk::dataloader::cache;
using namespace epoch_frame;

TEST_CASE("deduplicateByTimestamp - empty DataFrame", "[dedup_utils]") {
  auto asset = asset::AssetConstants::instance().AAPL;
  DataFrame empty_df;

  auto result = deduplicateByTimestamp(empty_df, asset, DataCategory::Dividends);

  REQUIRE(result.has_value());
  REQUIRE(result->empty());
}

TEST_CASE("deduplicateByTimestamp - unique timestamps (no duplicates)", "[dedup_utils]") {
  auto asset = asset::AssetConstants::instance().AAPL;

  // Create simple test data with 3 unique dates
  auto df = make_dataframe<double>(
    factory::index::make_datetime_index({
      DateTime(Date{chrono_year{2024}, chrono_month{1}, chrono_day{15}}),
      DateTime(Date{chrono_year{2024}, chrono_month{2}, chrono_day{15}}),
      DateTime(Date{chrono_year{2024}, chrono_month{3}, chrono_day{15}})
    }),
    {{0.25, 0.30, 0.28}},
    {"cash_amount"}
  );

  REQUIRE(df.num_rows() == 3);

  auto result = deduplicateByTimestamp(df, asset, DataCategory::Dividends);

  REQUIRE(result.has_value());
  REQUIRE(result->num_rows() == 3);  // All rows preserved
}

TEST_CASE("deduplicateByTimestamp - simple duplicates (2x same timestamp)", "[dedup_utils]") {
  auto asset = asset::AssetConstants::instance().AAPL;

  // Create test data with duplicate timestamp
  auto df = make_dataframe<double>(
    factory::index::make_datetime_index({
      DateTime(Date{chrono_year{2024}, chrono_month{8}, chrono_day{11}}),  // Duplicate 1
      DateTime(Date{chrono_year{2024}, chrono_month{8}, chrono_day{11}}),  // Duplicate 2
      DateTime(Date{chrono_year{2024}, chrono_month{9}, chrono_day{11}})   // Unique
    }),
    {{0.25, 0.25, 0.30}},
    {"cash_amount"}
  );

  REQUIRE(df.num_rows() == 3);

  auto result = deduplicateByTimestamp(df, asset, DataCategory::Dividends);

  REQUIRE(result.has_value());
  REQUIRE(result->num_rows() == 2);  // 3 -> 2 rows (1 duplicate removed)
}

TEST_CASE("deduplicateByTimestamp - multiple duplicates (NVDA case)", "[dedup_utils]") {
  auto asset = asset::AssetConstants::instance().AAPL;

  // NVDA has 3 duplicates: each date appears 2 times
  auto df = make_dataframe<double>(
    factory::index::make_datetime_index({
      DateTime(Date{chrono_year{2024}, chrono_month{6}, chrono_day{10}}),  // Dup 1/2
      DateTime(Date{chrono_year{2024}, chrono_month{6}, chrono_day{10}}),  // Dup 2/2
      DateTime(Date{chrono_year{2024}, chrono_month{9}, chrono_day{11}}),  // Dup 1/2
      DateTime(Date{chrono_year{2024}, chrono_month{9}, chrono_day{11}}),  // Dup 2/2
      DateTime(Date{chrono_year{2024}, chrono_month{12}, chrono_day{4}}),  // Dup 1/2
      DateTime(Date{chrono_year{2024}, chrono_month{12}, chrono_day{4}}),  // Dup 2/2
      DateTime(Date{chrono_year{2024}, chrono_month{12}, chrono_day{30}})  // Unique
    }),
    {{0.01, 0.01, 0.01, 0.01, 0.01, 0.01, 0.01}},
    {"cash_amount"}
  );

  REQUIRE(df.num_rows() == 7);

  auto result = deduplicateByTimestamp(df, asset, DataCategory::Dividends);

  REQUIRE(result.has_value());
  REQUIRE(result->num_rows() == 4);  // 7 -> 4 rows (3 duplicates removed)
}

TEST_CASE("deduplicateByTimestamp - worst case 4x duplicates (WMT case)", "[dedup_utils]") {
  auto asset = asset::AssetConstants::instance().AAPL;

  // WMT worst case: 2024-02-19 appears 4 times
  auto df = make_dataframe<double>(
    factory::index::make_datetime_index({
      DateTime(Date{chrono_year{2024}, chrono_month{2}, chrono_day{19}}),  // Dup 1/4
      DateTime(Date{chrono_year{2024}, chrono_month{2}, chrono_day{19}}),  // Dup 2/4
      DateTime(Date{chrono_year{2024}, chrono_month{2}, chrono_day{19}}),  // Dup 3/4
      DateTime(Date{chrono_year{2024}, chrono_month{2}, chrono_day{19}}),  // Dup 4/4
      DateTime(Date{chrono_year{2024}, chrono_month{3}, chrono_day{15}})   // Unique
    }),
    {{0.57, 0.57, 0.57, 0.57, 0.58}},
    {"cash_amount"}
  );

  REQUIRE(df.num_rows() == 5);

  auto result = deduplicateByTimestamp(df, asset, DataCategory::Dividends);

  REQUIRE(result.has_value());
  REQUIRE(result->num_rows() == 2);  // 5 -> 2 rows (3 duplicates removed)
}

TEST_CASE("deduplicateByTimestamp - works for all categories", "[dedup_utils]") {
  auto asset = asset::AssetConstants::instance().AAPL;

  // Create simple duplicate data
  auto df = make_dataframe<double>(
    factory::index::make_datetime_index({
      DateTime(Date{chrono_year{2024}, chrono_month{1}, chrono_day{15}}),
      DateTime(Date{chrono_year{2024}, chrono_month{1}, chrono_day{15}})
    }),
    {{100.0, 105.0}},
    {"value"}
  );

  // Test each category
  std::vector<DataCategory> categories = {
    DataCategory::Dividends,
    DataCategory::Splits,
    DataCategory::ShortInterest,
    DataCategory::ShortVolume,
    DataCategory::DailyBars,
    DataCategory::MinuteBars
  };

  for (auto category : categories) {
    auto result = deduplicateByTimestamp(df, asset, category);
    REQUIRE(result.has_value());
    REQUIRE(result->num_rows() == 1);  // Should deduplicate for all categories
  }
}

TEST_CASE("deduplicateByTimestamp - preserves column schema", "[dedup_utils]") {
  auto asset = asset::AssetConstants::instance().AAPL;

  auto df = make_dataframe(
    factory::index::make_datetime_index({
      DateTime(Date{chrono_year{2024}, chrono_month{1}, chrono_day{15}}),
      DateTime(Date{chrono_year{2024}, chrono_month{1}, chrono_day{15}})
    }),
    {
      factory::array::make_array(std::vector<double>{1.0, 2.0}),
      factory::array::make_array(std::vector<int64_t>{100, 200}),
      factory::array::make_array(std::vector<std::string>{"A", "B"})
    },
    {"float_column", "int_column", "string_column"}
  );

  auto original_schema = df.table()->schema();

  auto result = deduplicateByTimestamp(df, asset, DataCategory::Dividends);

  REQUIRE(result.has_value());

  // Verify schema is preserved (same number of columns, same names)
  auto result_schema = result->table()->schema();
  REQUIRE(result_schema->num_fields() == original_schema->num_fields());

  for (int i = 0; i < original_schema->num_fields(); ++i) {
    REQUIRE(result_schema->field(i)->name() == original_schema->field(i)->name());
    REQUIRE(result_schema->field(i)->type()->id() == original_schema->field(i)->type()->id());
  }
}

TEST_CASE("deduplicateByTimestamp - preserves index type", "[dedup_utils]") {
  auto asset = asset::AssetConstants::instance().AAPL;

  auto df = make_dataframe<double>(
    factory::index::make_datetime_index({
      DateTime(Date{chrono_year{2024}, chrono_month{1}, chrono_day{15}}),
      DateTime(Date{chrono_year{2024}, chrono_month{1}, chrono_day{15}})
    }),
    {{1.0, 2.0}},
    {"value"}
  );

  auto result = deduplicateByTimestamp(df, asset, DataCategory::Dividends);

  REQUIRE(result.has_value());

  // Verify index is still datetime (timestamp with UTC timezone)
  REQUIRE(result->index()->inferred_type() == "timestamp[ns, tz=UTC]");
}
