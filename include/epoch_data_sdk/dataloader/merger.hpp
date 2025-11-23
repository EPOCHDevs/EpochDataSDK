#pragma once

#include <unordered_map>
#include <expected>
#include <string>

#include "epoch_data_sdk/common/enums.hpp"
#include <epoch_frame/dataframe.h>

namespace data_sdk::dataloader {

/**
 * @brief Interface for merging multiple DataCategory DataFrames into a single DataFrame
 *
 * Strategy pattern that allows pluggable merge implementations.
 * Users can provide custom merge logic by implementing this interface.
 *
 * Example use cases:
 * - SimpleMerger: Forward-fill normalized data to intraday timestamps
 * - CustomMerger: User-defined merge logic for specific strategies
 */
class IDataMerger {
public:
  virtual ~IDataMerger() = default;

  /**
   * Merge multiple DataFrames with string keys
   *
   * @param data_map Map of string keys to DataFrame (e.g., "DailyBars", "Indices", "EconomicIndicator")
   * @return Merged DataFrame on success, error string on failure
   *
   * Expected behavior:
   * - All input DataFrames share compatible date ranges
   * - Output DataFrame has combined columns from all keys
   * - Column names should be prefixed before adding to map to avoid conflicts
   * - Index is determined by merge strategy (date-aligned or timestamp-aligned)
   */
  virtual std::expected<epoch_frame::DataFrame, std::string>
  Merge(const std::unordered_map<std::string, epoch_frame::DataFrame>& data_map) = 0;
};

} // namespace data_sdk::dataloader
