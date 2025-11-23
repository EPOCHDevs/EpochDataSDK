#pragma once

#include "epoch_data_sdk/common/enums.hpp"
#include "epoch_data_sdk/common/metadata.hpp"

namespace data_sdk::dataloader {

/**
 * @brief Static metadata registry for data categories
 *
 * Provides centralized access to DataFrameMetadata for each DataCategory
 * by delegating to the corresponding client's getMetadata() method.
 */
class MetadataRegistry {
public:
  /**
   * Get metadata for a specific data category
   * @param category The data category to query
   * @return DataFrameMetadata with column descriptions, types, and index properties
   * @throws std::invalid_argument if category is not supported
   */
  static DataFrameMetadata GetMetadataForCategory(DataCategory category);

  /**
   * Get metadata for ALFRED point-in-time economic data
   * @return DataFrameMetadata with ALFRED schema (published_at, observation_date, value, revision)
   */
  static DataFrameMetadata GetAlfredMetadata();

  /**
   * Get metadata for cross-sectional economic indicators
   * @param category The cross-sectional data category
   * @return DataFrameMetadata with ALFRED schema (same as GetAlfredMetadata)
   */
  static DataFrameMetadata GetCrossSectionalMetadata(CrossSectionalDataCategory category);

  /**
   * Get metadata for market indices data
   * @param indexTicker The index ticker symbol (e.g., "SPX", "VIX")
   * @return DataFrameMetadata with OHLCV schema (same as AggsClient)
   */
  static DataFrameMetadata GetIndicesMetadata(const std::string& indexTicker);
};

} // namespace data_sdk::dataloader
