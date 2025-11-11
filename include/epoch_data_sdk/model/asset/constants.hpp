#pragma once
#include <string_view>

namespace data_sdk::asset {

// Futures continuation prefix
constexpr std::string_view FUTURES_CONTINUATION_PREFIX = "~";

// S3 bucket configuration for asset data
struct AssetDataS3Config {
  static constexpr const char* BUCKET_NAME = "epoch-data-sdk-files";
  static constexpr const char* ASSET_SPECS_KEY = "asset_specifications.json";
  static constexpr const char* INDEX_CONSTITUENTS_KEY = "index_constituents.json";
};

// Default timestamp column name for parquet files (matches EpochScript convention)
constexpr std::string_view DEFAULT_TIMESTAMP_COLUMN = "t";

// Bar attribute column names (matching Polygon API column names)
constexpr std::string_view CLOSE_COLUMN = "c";
constexpr std::string_view OPEN_COLUMN = "o";
constexpr std::string_view HIGH_COLUMN = "h";
constexpr std::string_view LOW_COLUMN = "l";
constexpr std::string_view VOLUME_COLUMN = "v";

} // namespace data_sdk::asset
