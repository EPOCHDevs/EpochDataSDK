#pragma once

#include <epoch_data_sdk/common/enums.hpp>
#include <epoch_data_sdk/model/asset/asset.hpp>
#include <epoch_frame/dataframe.h>
#include <epoch_frame/datetime.h>
#include <chrono>

namespace data_sdk::dataloader::cache {

/**
 * Extract date component (midnight UTC) from nanosecond timestamp
 * Used for date-based grouping and merging
 */
inline int64_t extractDate(int64_t timestamp_ns) {
  using namespace std::chrono;
  auto tp = sys_time<nanoseconds>{nanoseconds{timestamp_ns}};
  auto day = floor<days>(tp);
  return duration_cast<nanoseconds>(day.time_since_epoch()).count();
}

/**
 * Normalize auxiliary data for intraday merge (with MinuteBars)
 * Strategy:
 * - Groups by date (extractDate)
 * - Takes first event of each day
 * - Preserves original timestamp in {category}_timestamp column
 * - Returns DataFrame indexed by date (midnight UTC)
 *
 * Example:
 *   Input: News at 10:23, 14:30, 15:45 on 2024-01-15
 *   Output: One row at 2024-01-15 00:00:00 with first news + news_timestamp=10:23
 */
epoch_frame::DataFrame normalizeForIntradayMerge(
    const epoch_frame::DataFrame& df,
    const std::string& category_prefix);

/**
 * Normalize auxiliary data for daily merge (with DailyBars)
 * Strategy:
 * - Preserves ALL events (no grouping)
 * - Adds {category}_timestamp column with original timestamp
 * - Normalizes index to date (midnight UTC)
 * - Multiple events per day result in multiple rows
 *
 * Example:
 *   Input: News at 10:23, 14:30, 15:45 on 2024-01-15
 *   Output: Three rows at 2024-01-15 00:00:00, each with different news + news_timestamp
 */
epoch_frame::DataFrame normalizeForDailyMerge(
    const epoch_frame::DataFrame& df,
    const std::string& category_prefix);

} // namespace data_sdk::dataloader::cache
