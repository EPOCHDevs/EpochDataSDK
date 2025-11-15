#pragma once

#include <optional>
#include <string>
#include <vector>

#include "epoch_data_sdk/model/asset/asset_specification.hpp"

namespace data_sdk {

/**
 * @brief Arrow data type enumeration for DataFrame columns
 *
 * Maps to Arrow data types used throughout the SDK.
 */
enum class ArrowType {
  STRING,           ///< arrow::string() - UTF-8 string
  INT32,            ///< arrow::int32() - 32-bit signed integer
  INT64,            ///< arrow::int64() - 64-bit signed integer
  FLOAT32,          ///< arrow::float32() - 32-bit floating point
  FLOAT64,          ///< arrow::float64() - 64-bit floating point
  TIMESTAMP_NS_UTC, ///< arrow::timestamp(TimeUnit::NANO, "UTC") - nanosecond timestamp in UTC
  BOOLEAN           ///< arrow::boolean() - boolean value
};

/**
 * @brief Convert ArrowType enum to string representation
 */
inline std::string arrowTypeToString(ArrowType type) {
  switch (type) {
    case ArrowType::STRING:
      return "string";
    case ArrowType::INT32:
      return "int32";
    case ArrowType::INT64:
      return "int64";
    case ArrowType::FLOAT32:
      return "float32";
    case ArrowType::FLOAT64:
      return "float64";
    case ArrowType::TIMESTAMP_NS_UTC:
      return "timestamp[ns, UTC]";
    case ArrowType::BOOLEAN:
      return "boolean";
    default:
      return "unknown";
  }
}

/**
 * @brief Metadata for a single DataFrame column
 */
struct ColumnMetadata {
  std::string id;           ///< Column identifier (technical name)
  std::string name;         ///< Human-readable column name
  std::string description;  ///< Detailed description of the column
  ArrowType type;           ///< Arrow data type
  bool nullable;            ///< Whether the column can contain null values
};

/**
 * @brief Complete metadata for a DataFrame returned by SDK methods
 *
 * Provides comprehensive information about the structure, types, and semantics
 * of DataFrames returned by data client methods.
 */
struct DataFrameMetadata {
  std::string data_type;                     ///< Type of data (e.g., "news", "balance_sheet", "trades")
  std::string description;                   ///< Detailed description of the data, use cases, and purpose
  std::optional<AssetClass> asset_class;     ///< Asset class (Stocks, FX, Crypto, Futures, Indices), or nullopt for N/A
  bool index_normalized;                     ///< Whether the index is normalized to dates (midnight UTC) vs has time-of-day
  std::string category_prefix;               ///< Prefix for column names when merging categories (e.g., "N:", "D:", "" for timeseries)
  std::vector<ColumnMetadata> columns;       ///< Metadata for each column in the DataFrame
};

} // namespace data_sdk
