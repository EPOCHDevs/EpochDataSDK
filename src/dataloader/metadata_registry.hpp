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
};

} // namespace data_sdk::dataloader
