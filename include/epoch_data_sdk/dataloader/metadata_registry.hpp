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
   * @param is_eod True for daily bars (normalized index), false for minute bars (non-normalized)
   * @return DataFrameMetadata with OHLC schema (without v, vw, and n)
   */
  static DataFrameMetadata GetIndicesMetadata(bool is_eod);

  /**
   * Get metadata for a string key (supports DataCategory names and auxiliary keys)
   * @param key String key - can be DataCategory name (e.g., "DailyBars"),
   *            "EconomicIndicator" for economic data, or "Indices" for market indices
   * @return DataFrameMetadata for the specified key
   * @throws std::invalid_argument if key is not recognized
   */
  static DataFrameMetadata GetMetadata(const std::string& key);
};

} // namespace data_sdk::dataloader
