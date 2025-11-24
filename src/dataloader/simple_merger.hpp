#pragma once

#include "epoch_data_sdk/dataloader/merger.hpp"
#include "epoch_data_sdk/dataloader/metadata_registry.hpp"

namespace data_sdk::dataloader {

/**
 * @brief Simple merger implementation with first-timestamp alignment
 *
 * Merge strategy:
 * 1. All normalized data: Concat with outer join on date index
 * 2. All non-normalized data: Concat with outer join on timestamp index
 * 3. Mixed (normalized + non-normalized):
 *    - Merge all non-normalized data together (intraday timestamps)
 *    - Merge all normalized data together (date-aligned)
 *    - Align normalized data to first intraday timestamp of each day
 *    - Concat both results along column axis (no forward-fill)
 *
 * Example (mixed case):
 *   MinuteBars: 09:31, 09:32, 09:33, ..., 16:00 on 2024-01-15
 *   Dividends: Single event on 2024-01-15 (ex_dividend_date at midnight UTC)
 *   Result: Dividend columns appear ONLY at 09:31 (first intraday timestamp),
 *           NaN for all other timestamps
 *
 * This provides cleaner visualizations and avoids misleading forward-fill artifacts.
 * For backtesting, use ffill() explicitly on the result if you need propagation.
 */
class SimpleMerger : public IDataMerger {
public:
  SimpleMerger() = default;
  ~SimpleMerger() override = default;

  std::expected<epoch_frame::DataFrame, std::string>
  Merge(const std::unordered_map<std::string, epoch_frame::DataFrame>& data_map) override;

private:
  // Check if all categories have the same normalization policy
  bool IsSameNormalizationPolicy(const std::unordered_map<std::string, epoch_frame::DataFrame>& data_map) const;

  // Merge all normalized data (concat with outer join on dates)
  std::expected<epoch_frame::DataFrame, std::string>
  MergeNormalizedData(const std::unordered_map<std::string, epoch_frame::DataFrame>& normalized_data) const;

  // Merge all non-normalized data (concat with outer join on timestamps)
  std::expected<epoch_frame::DataFrame, std::string>
  MergeNonNormalizedData(const std::unordered_map<std::string, epoch_frame::DataFrame>& non_normalized_data) const;

  // Merge mixed data (normalized + non-normalized with first-timestamp alignment)
  std::expected<epoch_frame::DataFrame, std::string>
  MergeMixedData(const std::unordered_map<std::string, epoch_frame::DataFrame>& data_map) const;
};

} // namespace data_sdk::dataloader
