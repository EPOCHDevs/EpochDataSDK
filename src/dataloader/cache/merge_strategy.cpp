#include "merge_strategy.h"
#include <epoch_frame/factory/series_factory.h>
#include <epoch_frame/index.h>
#include <spdlog/spdlog.h>

namespace data_sdk::dataloader::cache {

epoch_frame::DataFrame normalizeForIntradayMerge(
    const epoch_frame::DataFrame& df,
    const std::string& category_prefix) {

  using namespace epoch_frame;

  if (df.empty()) {
    return df;
  }

  SPDLOG_DEBUG("normalizeForIntradayMerge: Processing {} rows for category '{}'",
               df.num_rows(), category_prefix);

  // 1. Extract timestamps from index
  std::string ts_col_name = category_prefix + "_timestamp";
  const auto timestampArray = df.index()->array().to_timestamp_view();

  std::vector<int64_t> original_timestamps;
  original_timestamps.reserve(df.num_rows());
  for (size_t i = 0; i < df.num_rows(); ++i) {
    original_timestamps.push_back(timestampArray->Value(static_cast<int64_t>(i)));
  }

  // 2. Create timestamp series and assign to dataframe
  auto ts_series = epoch_frame::make_series(df.index(), original_timestamps, ts_col_name);
  auto df_with_ts = df.assign(ts_col_name, ts_series);

  // 3. Normalize index to dates (midnight UTC)
  auto normalized_index = df_with_ts.index()->normalize();

  // 4. Create DataFrame with normalized index
  auto df_normalized = df_with_ts.from_base(normalized_index, df_with_ts.table());

  // 5. Group by normalized date index and take first event of each day
  // Note: group_by_agg with index array doesn't preserve the grouping key in result,
  // so we pass the normalized index as-is and it becomes the result's index
  auto result = df_normalized.group_by_agg(normalized_index->as_chunked_array()).first();

  SPDLOG_DEBUG("normalizeForIntradayMerge: Reduced to {} rows (grouped by date)",
               result.num_rows());

  return result;
}

epoch_frame::DataFrame normalizeForDailyMerge(
    const epoch_frame::DataFrame& df,
    const std::string& category_prefix) {

  using namespace epoch_frame;

  if (df.empty()) {
    return df;
  }

  SPDLOG_DEBUG("normalizeForDailyMerge: Processing {} rows for category '{}'",
               df.num_rows(), category_prefix);

  // 1. Extract timestamps from index
  std::string ts_col_name = category_prefix + "_timestamp";
  const auto timestampArray = df.index()->array().to_timestamp_view();

  std::vector<int64_t> original_timestamps;
  original_timestamps.reserve(df.num_rows());
  for (size_t i = 0; i < df.num_rows(); ++i) {
    original_timestamps.push_back(timestampArray->Value(static_cast<int64_t>(i)));
  }

  // 2. Create timestamp series and assign to dataframe
  auto ts_series = epoch_frame::make_series(df.index(), original_timestamps, ts_col_name);
  auto df_with_ts = df.assign(ts_col_name, ts_series);

  // 3. Normalize index to dates (midnight UTC)
  // For daily merge, we preserve ALL events - no grouping
  auto normalized_index = df_with_ts.index()->normalize();

  // 4. Create new DataFrame with normalized date index
  auto result = df_with_ts.from_base(normalized_index, df_with_ts.table());

  SPDLOG_DEBUG("normalizeForDailyMerge: Preserving all {} rows with date index",
               result.num_rows());

  return result;
}

} // namespace data_sdk::dataloader::cache