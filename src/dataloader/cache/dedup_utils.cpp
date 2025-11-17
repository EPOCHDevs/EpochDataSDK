#include "dedup_utils.h"
#include <epoch_frame/index.h>
#include <spdlog/spdlog.h>
#include <format>

namespace data_sdk::dataloader::cache {

std::expected<epoch_frame::DataFrame, std::string>
deduplicateByTimestamp(const epoch_frame::DataFrame& df,
                      const asset::Asset& asset,
                      DataCategory category) {

  using namespace epoch_frame;

  // Fast path: If DataFrame is empty, return as-is
  if (df.empty()) {
    return df;
  }

  const auto original_rows = df.num_rows();

  SPDLOG_DEBUG("deduplicateByTimestamp: Processing {}/{} with {} rows",
               asset.GetID(), epoch_core::DataCategoryWrapper::ToString(category), original_rows);

  try {
    // Group by the index (timestamp) and keep first occurrence
    // This matches the pattern used in normalizeForIntradayMerge
    auto index_array = df.index()->as_chunked_array();
    auto result = df.group_by_agg(index_array).first();

    const auto deduped_rows = result.num_rows();
    const auto duplicates_removed = original_rows - deduped_rows;

    if (duplicates_removed > 0) {
      SPDLOG_WARN("deduplicateByTimestamp: Removed {} duplicate rows for {}/{} ({} -> {} rows)",
                  duplicates_removed, asset.GetID(), epoch_core::DataCategoryWrapper::ToString(category),
                  original_rows, deduped_rows);
    }

    return result;

  } catch (const std::exception& ex) {
    auto error_msg = std::format(
      "Failed to deduplicate {}/{}: {}",
      asset.GetID(), epoch_core::DataCategoryWrapper::ToString(category), ex.what());
    SPDLOG_ERROR("deduplicateByTimestamp: {}", error_msg);
    return std::unexpected(error_msg);
  }
}

} // namespace data_sdk::dataloader::cache
