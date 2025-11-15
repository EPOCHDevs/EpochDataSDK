#include "simple_merger.hpp"
#include "metadata_registry.hpp"
#include <spdlog/spdlog.h>
#include <epoch_frame/common.h>

namespace data_sdk::dataloader {

std::expected<epoch_frame::DataFrame, std::string>
SimpleMerger::Merge(const std::unordered_map<DataCategory, epoch_frame::DataFrame>& category_data) {
  if (category_data.empty()) {
    return std::unexpected("Cannot merge empty category data");
  }

  // Single category - no merge needed
  if (category_data.size() == 1) {
    return category_data.begin()->second;
  }

  // Determine merge strategy based on index normalization
  if (IsSameNormalizationPolicy(category_data)) {
    // Check if all are normalized or all are non-normalized
    auto first_metadata = MetadataRegistry::GetMetadataForCategory(category_data.begin()->first);
    if (first_metadata.index_normalized) {
      SPDLOG_DEBUG("SimpleMerger: All categories normalized, using outer join on dates");
      return MergeNormalizedData(category_data);
    } else {
      SPDLOG_DEBUG("SimpleMerger: All categories non-normalized, using outer join on timestamps");
      return MergeNonNormalizedData(category_data);
    }
  } else {
    SPDLOG_DEBUG("SimpleMerger: Mixed normalized/non-normalized, using forward-fill strategy");
    return MergeMixedData(category_data);
  }
}

bool SimpleMerger::IsSameNormalizationPolicy(
    const std::unordered_map<DataCategory, epoch_frame::DataFrame>& category_data) const {
  if (category_data.empty()) {
    return true;
  }

  // Get the index_normalized value from the first category
  auto first_metadata = MetadataRegistry::GetMetadataForCategory(category_data.begin()->first);
  bool expected_normalized = first_metadata.index_normalized;

  // Check if all categories have the same index_normalized value
  for (const auto& [cat, df] : category_data) {
    auto metadata = MetadataRegistry::GetMetadataForCategory(cat);
    if (metadata.index_normalized != expected_normalized) {
      return false;  // Found a mismatch
    }
  }

  return true;  // All have the same normalization policy
}

std::expected<epoch_frame::DataFrame, std::string>
SimpleMerger::MergeNormalizedData(
    const std::unordered_map<DataCategory, epoch_frame::DataFrame>& normalized_data) const {

  if (normalized_data.empty()) {
    return std::unexpected("MergeNormalizedData: No data to merge");
  }

  // Build vector of DataFrames for concat
  std::vector<epoch_frame::FrameOrSeries> frames;
  frames.reserve(normalized_data.size());
  for (const auto& [cat, df] : normalized_data) {
    frames.emplace_back(df);
  }

  // Concat along column axis (side-by-side) with outer join on index
  epoch_frame::ConcatOptions options;
  options.frames = std::move(frames);
  options.joinType = epoch_frame::JoinType::Outer;  // Keep all dates from all categories
  options.axis = epoch_frame::AxisType::Column;     // Concatenate columns side-by-side
  options.sort = true;                              // Sort by index (dates)

  auto result = epoch_frame::concat(options);
  SPDLOG_DEBUG("SimpleMerger: Merged {} normalized categories into {} rows × {} columns",
               normalized_data.size(), result.num_rows(), result.num_columns());

  return result;
}

std::expected<epoch_frame::DataFrame, std::string>
SimpleMerger::MergeNonNormalizedData(
    const std::unordered_map<DataCategory, epoch_frame::DataFrame>& non_normalized_data) const {

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
               non_normalized_data.size(), result.num_rows(), result.num_columns());

  return result;
}

std::expected<epoch_frame::DataFrame, std::string>
SimpleMerger::MergeMixedData(
    const std::unordered_map<DataCategory, epoch_frame::DataFrame>& category_data) const {

  // Separate normalized and non-normalized categories
  std::unordered_map<DataCategory, epoch_frame::DataFrame> normalized;
  std::unordered_map<DataCategory, epoch_frame::DataFrame> non_normalized;

  for (const auto& [cat, df] : category_data) {
    auto metadata = MetadataRegistry::GetMetadataForCategory(cat);
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

  // Step 3: Concat both DataFrames with outer join (keeps all timestamps)
  std::vector<epoch_frame::FrameOrSeries> frames = {merged_intraday, merged_normalized};
  epoch_frame::ConcatOptions options;
  options.frames = std::move(frames);
  options.joinType = epoch_frame::JoinType::Outer;
  options.axis = epoch_frame::AxisType::Column;
  options.sort = true;

  auto concat_result = epoch_frame::concat(options);

  // Step 4: Forward-fill to propagate normalized values to intraday timestamps
  auto result = concat_result.ffill();

  SPDLOG_DEBUG("SimpleMerger: Merged mixed data into {} rows × {} columns",
               result.num_rows(), result.num_cols());

  return result;
}

} // namespace data_sdk::dataloader
