#pragma once

#include "epoch_data_sdk/dataloader/merger.hpp"
#include "epoch_data_sdk/dataloader/metadata_registry.hpp"

namespace data_sdk::dataloader {

/**
 * @brief Simple merger implementation with forward-fill logic
 *
 * Merge strategy:
 * 1. All normalized data: Concat with outer join on date index
 * 2. All non-normalized data: Concat with outer join on timestamp index
 * 3. Mixed (normalized + non-normalized):
 *    - Merge all non-normalized data together (intraday timestamps)
 *    - Merge all normalized data together (date-aligned)
 *    - Reindex normalized to intraday timestamps and forward-fill
 *    - Concat both results along column axis
 *
 * Example (mixed case):
 *   MinuteBars: 09:31, 09:32, ..., 16:00 on 2024-01-15
 *   Dividends: Single event on 2024-01-15 (ex_dividend_date)
 *   Result: Dividend columns appear on ALL minute bars for that day (forward-filled)
 *
 * This ensures corporate actions (splits, dividends) are visible throughout
 * the trading day in backtesting systems.
 */
class SimpleMerger : public IDataMerger {
public:
  SimpleMerger() = default;
  ~SimpleMerger() override = default;

  std::expected<epoch_frame::DataFrame, std::string>
  Merge(const std::unordered_map<DataCategory, epoch_frame::DataFrame>& category_data) override;

private:
  // Check if all categories have the same normalization policy
  bool IsSameNormalizationPolicy(const std::unordered_map<DataCategory, epoch_frame::DataFrame>& category_data) const;

  // Merge all normalized data (concat with outer join on dates)
  std::expected<epoch_frame::DataFrame, std::string>
  MergeNormalizedData(const std::unordered_map<DataCategory, epoch_frame::DataFrame>& normalized_data) const;

  // Merge all non-normalized data (concat with outer join on timestamps)
  std::expected<epoch_frame::DataFrame, std::string>
  MergeNonNormalizedData(const std::unordered_map<DataCategory, epoch_frame::DataFrame>& non_normalized_data) const;

  // Merge mixed data (normalized + non-normalized with forward-fill)
  std::expected<epoch_frame::DataFrame, std::string>
  MergeMixedData(const std::unordered_map<DataCategory, epoch_frame::DataFrame>& category_data) const;
};

} // namespace data_sdk::dataloader
