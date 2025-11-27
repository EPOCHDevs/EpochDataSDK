#include "simple_merger.hpp"
#include "epoch_data_sdk/dataloader/metadata_registry.hpp"
#include <spdlog/spdlog.h>
#include <epoch_frame/common.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/serialization.h>
#include <arrow/compute/api.h>

namespace data_sdk::dataloader {

// Debug helper: write DataFrame to CSV for inspection
static void debug_write_csv(const epoch_frame::DataFrame& df, const std::string& filename) {
  std::string filepath = "/tmp/" + filename;
  auto status = epoch_frame::write_csv_file(df, filepath);
  if (!status.ok()) {
    SPDLOG_ERROR("Failed to write CSV to {}: {}", filepath, status.ToString());
  } else {
    SPDLOG_INFO("DEBUG: Wrote {} rows x {} cols to {}", df.num_rows(), df.num_cols(), filepath);
  }
}

std::expected<epoch_frame::DataFrame, std::string>
SimpleMerger::Merge(const std::unordered_map<std::string, epoch_frame::DataFrame>& data_map) {
  if (data_map.empty()) {
    return std::unexpected("Cannot merge empty data map");
  }

  // Single DataFrame - no merge needed
  if (data_map.size() == 1) {
    return data_map.begin()->second;
  }

  // Determine merge strategy based on index normalization
  if (IsSameNormalizationPolicy(data_map)) {
    // Check if all are normalized or all are non-normalized
    auto first_metadata = MetadataRegistry::GetMetadata(data_map.begin()->first);
    if (first_metadata.index_normalized) {
      SPDLOG_DEBUG("SimpleMerger: All categories normalized, using outer join on dates");
      return MergeNormalizedData(data_map);
    } else {
      SPDLOG_DEBUG("SimpleMerger: All categories non-normalized, using outer join on timestamps");
      return MergeNonNormalizedData(data_map);
    }
  } else {
    SPDLOG_DEBUG("SimpleMerger: Mixed normalized/non-normalized, using forward-fill strategy");
    return MergeMixedData(data_map);
  }
}

bool SimpleMerger::IsSameNormalizationPolicy(
    const std::unordered_map<std::string, epoch_frame::DataFrame>& category_data) const {
  if (category_data.empty()) {
    return true;
  }

  // Get the index_normalized value from the first category
  auto first_metadata = MetadataRegistry::GetMetadata(category_data.begin()->first);
  bool expected_normalized = first_metadata.index_normalized;

  // Check if all categories have the same index_normalized value
  for (const auto& [cat, df] : category_data) {
    auto metadata = MetadataRegistry::GetMetadata(cat);
    if (metadata.index_normalized != expected_normalized) {
      return false;  // Found a mismatch
    }
  }

  return true;  // All have the same normalization policy
}

std::expected<epoch_frame::DataFrame, std::string>
SimpleMerger::MergeNormalizedData(
    const std::unordered_map<std::string, epoch_frame::DataFrame>& normalized_data) const {

  if (normalized_data.empty()) {
    return std::unexpected("MergeNormalizedData: No data to merge");
  }

  // Build vector of DataFrames for concat
  std::vector<epoch_frame::FrameOrSeries> frames;
  frames.reserve(normalized_data.size());
  for (const auto& [cat, df] : normalized_data) {
    // Check for duplicate indices - following pandas behavior, error if duplicates exist
    // Financial data (IncomeStatements, BalanceSheets, CashFlowStatements) can have
    // duplicate timestamps due to amended filings/restatements
    if (df.index()->has_duplicates()) {
      SPDLOG_WARN("SimpleMerger: Category {} has duplicate index values, deduplicating (keeping last)",
                  cat);

      // Drop duplicate index values, keeping last occurrence (most recent filing)
      auto deduped_df = df.drop_duplicates(epoch_frame::DropDuplicatesKeepPolicy::Last);

      SPDLOG_DEBUG("SimpleMerger: Category {} deduplicated: {} rows -> {} rows",
                   cat, df.num_rows(), deduped_df.num_rows());

      frames.emplace_back(deduped_df);
    } else {
      frames.emplace_back(df);
    }
  }

  // Concat along column axis (side-by-side) with outer join on index
  epoch_frame::ConcatOptions options;
  options.frames = std::move(frames);
  options.joinType = epoch_frame::JoinType::Outer;  // Keep all dates from all categories
  options.axis = epoch_frame::AxisType::Column;     // Concatenate columns side-by-side
  options.sort = true;                              // Sort by index (dates)

  auto result = epoch_frame::concat(options);
  SPDLOG_DEBUG("SimpleMerger: Merged {} normalized categories into {} rows × {} columns",
               normalized_data.size(), result.num_rows(), result.num_cols());

  return result;
}

std::expected<epoch_frame::DataFrame, std::string>
SimpleMerger::MergeNonNormalizedData(
    const std::unordered_map<std::string, epoch_frame::DataFrame>& non_normalized_data) const {

  if (non_normalized_data.empty()) {
    return std::unexpected("MergeNonNormalizedData: No data to merge");
  }

  // Build vector of DataFrames for concat
  std::vector<epoch_frame::FrameOrSeries> frames;
  frames.reserve(non_normalized_data.size());
  for (const auto& [cat, df] : non_normalized_data) {
    frames.push_back(df);
  }

  // Concat along column axis (side-by-side) with outer join on index
  epoch_frame::ConcatOptions options;
  options.frames = std::move(frames);
  options.joinType = epoch_frame::JoinType::Outer;  // Keep all timestamps from all categories
  options.axis = epoch_frame::AxisType::Column;     // Concatenate columns side-by-side
  options.sort = true;                              // Sort by index (timestamps)

  auto result = epoch_frame::concat(options);
  SPDLOG_DEBUG("SimpleMerger: Merged {} non-normalized categories into {} rows × {} columns",
               non_normalized_data.size(), result.num_rows(), result.num_cols());

  return result;
}

std::expected<epoch_frame::DataFrame, std::string>
SimpleMerger::MergeMixedData(
    const std::unordered_map<std::string, epoch_frame::DataFrame>& category_data) const {

  // Separate normalized and non-normalized categories
  std::unordered_map<std::string, epoch_frame::DataFrame> normalized;
  std::unordered_map<std::string, epoch_frame::DataFrame> non_normalized;

  for (const auto& [cat, df] : category_data) {
    auto metadata = MetadataRegistry::GetMetadata(cat);
    if (metadata.index_normalized) {
      normalized[cat] = df;
    } else {
      non_normalized[cat] = df;
    }
  }

  SPDLOG_DEBUG("SimpleMerger::MergeMixedData - {} normalized, {} non-normalized categories",
               normalized.size(), non_normalized.size());

  // Step 1: Merge all non-normalized data first (this gives us the intraday timestamps)
  auto merged_intraday_result = MergeNonNormalizedData(non_normalized);
  if (!merged_intraday_result) {
    return std::unexpected("Failed to merge non-normalized data: " + merged_intraday_result.error());
  }
  auto merged_intraday = *merged_intraday_result;

  // Step 2: Merge all normalized data
  auto merged_normalized_result = MergeNormalizedData(normalized);
  if (!merged_normalized_result) {
    return std::unexpected("Failed to merge normalized data: " + merged_normalized_result.error());
  }
  auto merged_normalized = *merged_normalized_result;

  // Step 3: Build mapping from date -> first intraday timestamp of that day
  auto intraday_ts_array = merged_intraday.index()->array().to_timestamp_view();

  // Map: normalized date (midnight UTC nanoseconds) -> first intraday timestamp (nanoseconds)
  std::unordered_map<int64_t, int64_t> date_to_first_ts;

  constexpr int64_t NANOS_PER_DAY = 86400000000000LL;  // 24 * 60 * 60 * 1e9

  for (int64_t i = 0; i < intraday_ts_array->length(); ++i) {
    if (!intraday_ts_array->IsNull(i)) {
      int64_t ts_nanos = intraday_ts_array->Value(i);
      // Normalize to midnight UTC (date) by truncating to day boundary
      int64_t date_nanos = (ts_nanos / NANOS_PER_DAY) * NANOS_PER_DAY;

      // Keep only the first timestamp for each date
      if (date_to_first_ts.find(date_nanos) == date_to_first_ts.end()) {
        date_to_first_ts[date_nanos] = ts_nanos;
      }
    }
  }

  SPDLOG_DEBUG("SimpleMerger: Found {} unique days in intraday data", date_to_first_ts.size());

  // Step 4: Reindex normalized data to use first intraday timestamp per day
  // Build a map from old timestamp to new timestamp for all matching dates
  auto normalized_ts_array = merged_normalized.index()->array().to_timestamp_view();

  std::vector<int64_t> aligned_timestamps;
  std::vector<int64_t> rows_to_keep;

  for (int64_t i = 0; i < normalized_ts_array->length(); ++i) {
    if (!normalized_ts_array->IsNull(i)) {
      int64_t date_nanos = normalized_ts_array->Value(i);  // Already normalized to midnight

      // Check if this date has intraday data
      auto it = date_to_first_ts.find(date_nanos);
      if (it != date_to_first_ts.end()) {
        aligned_timestamps.push_back(it->second);  // Use first intraday timestamp
        rows_to_keep.push_back(i);
      }
      // Skip dates without intraday data
    }
  }

  if (rows_to_keep.empty()) {
    SPDLOG_WARN("SimpleMerger: No date overlap between normalized and intraday data");
    // Return just the intraday data if there's no overlap
    return merged_intraday;
  }

  SPDLOG_DEBUG("SimpleMerger: Aligning {} normalized dates to first intraday timestamps",
               rows_to_keep.size());

  // Step 5: Use Arrow's Take to select specific rows from the table
  arrow::Int64Builder indices_builder;
  auto append_status = indices_builder.AppendValues(rows_to_keep);
  if (!append_status.ok()) {
    return std::unexpected("Failed to append indices: " + append_status.ToString());
  }

  auto indices_result = indices_builder.Finish();
  if (!indices_result.ok()) {
    return std::unexpected("Failed to build indices array: " + indices_result.status().ToString());
  }

  // Take columns from the normalized table using the indices
  std::vector<std::shared_ptr<arrow::ChunkedArray>> new_columns;
  for (int col_idx = 0; col_idx < merged_normalized.table()->num_columns(); ++col_idx) {
    auto column = merged_normalized.table()->column(col_idx);

    // Take from each chunk
    std::vector<std::shared_ptr<arrow::Array>> taken_chunks;
    for (int chunk_idx = 0; chunk_idx < column->num_chunks(); ++chunk_idx) {
      auto take_result = arrow::compute::Take(column->chunk(chunk_idx), *indices_result);
      if (!take_result.ok()) {
        return std::unexpected("Failed to take rows: " + take_result.status().ToString());
      }
      taken_chunks.push_back(take_result.ValueOrDie().make_array());
    }

    new_columns.push_back(std::make_shared<arrow::ChunkedArray>(taken_chunks));
  }

  // Create new table with filtered rows
  auto new_table = arrow::Table::Make(merged_normalized.table()->schema(), new_columns);

  // Create new index with aligned timestamps (always UTC)
  auto new_index = epoch_frame::factory::index::make_datetime_index(aligned_timestamps, "", "UTC");
  auto reindexed_normalized = merged_normalized.from_base(new_index, new_table);

  // Step 6: Concat both DataFrames (no forward-fill needed!)
  std::vector<epoch_frame::FrameOrSeries> frames;
  frames.push_back(merged_intraday);
  frames.push_back(reindexed_normalized);

  epoch_frame::ConcatOptions options;
  options.frames = std::move(frames);
  options.joinType = epoch_frame::JoinType::Outer;
  options.axis = epoch_frame::AxisType::Column;
  options.sort = true;

  auto result = epoch_frame::concat(options);

  SPDLOG_DEBUG("SimpleMerger: Merged mixed data into {} rows × {} columns",
               result.num_rows(), result.num_cols());

  return result;
}

} // namespace data_sdk::dataloader
