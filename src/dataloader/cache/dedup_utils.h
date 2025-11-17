#pragma once

#include <epoch_data_sdk/common/enums.hpp>
#include <epoch_data_sdk/model/asset/asset.hpp>
#include <epoch_frame/dataframe.h>
#include <expected>
#include <string>

namespace data_sdk::dataloader::cache {

/**
 * Deduplicate DataFrame rows with duplicate timestamp indices
 *
 * Strategy:
 * - Fast path: If index is unique, return as-is (no-op)
 * - Slow path: Group by timestamp and aggregate duplicate rows based on column type:
 *   - Timestamp columns: MAX (keep latest timestamp)
 *   - Float/Double columns: MEAN (average values)
 *   - Int32/Int64 columns: SUM (sum values)
 *   - String columns: CONCAT with "\n" delimiter
 *   - Boolean columns: FIRST (keep first occurrence)
 *
 * @param df DataFrame to deduplicate
 * @param asset Asset being processed (for logging)
 * @param category DataCategory being processed (for logging)
 * @return Deduplicated DataFrame or error message
 *
 * Example:
 *   Input: 3 rows with timestamps [T1, T1, T2]
 *   Output: 2 rows with timestamps [T1, T2], where T1 row is aggregated
 *
 * Logging:
 *   Logs warning if duplicates detected with count and asset/category info
 */
std::expected<epoch_frame::DataFrame, std::string>
deduplicateByTimestamp(const epoch_frame::DataFrame& df,
                      const asset::Asset& asset,
                      DataCategory category);

} // namespace data_sdk::dataloader::cache
