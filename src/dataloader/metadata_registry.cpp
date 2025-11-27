#include "epoch_data_sdk/dataloader/metadata_registry.hpp"

// Polygon clients
#include "../polygon/aggs_client.hpp"
#include "../polygon/news_client.hpp"
#include "../polygon/dividends_client.hpp"
#include "../polygon/splits_client.hpp"
#include "../polygon/ticker_events_client.hpp"
#include "../polygon/financials_client.hpp"
#include "../polygon/ratios_client.hpp"
#include "../polygon/short_interest_client.hpp"
#include "../polygon/short_volume_client.hpp"

// FRED clients
#include "../fred/alfred_client.hpp"

#include <stdexcept>
#include <unordered_map>

namespace data_sdk::dataloader {

// Static registry mapping DataCategory to metadata
static const std::unordered_map<DataCategory, DataFrameMetadata>& GetRegistry() {
  static const std::unordered_map<DataCategory, DataFrameMetadata> registry = {
      // Time series data
      {DataCategory::MinuteBars, polygon::AggsClient::getMetadata()},  // Non-normalized (intraday timestamps)
      {DataCategory::DailyBars, []() {
        auto metadata = polygon::AggsClient::getMetadata();
        metadata.index_normalized = true;  // Daily bars normalized to midnight UTC
        return metadata;
      }()},

      // Corporate actions & events
      {DataCategory::News, polygon::NewsClient::getMetadata()},
      {DataCategory::Dividends, polygon::DividendsClient::getMetadata()},
      {DataCategory::Splits, polygon::SplitsClient::getMetadata()},
      {DataCategory::TickerEvents, polygon::TickerEventsClient::getMetadata()},

      // Fundamentals
      {DataCategory::BalanceSheets, polygon::FinancialsClient::getBalanceSheetsMetadata()},
      {DataCategory::CashFlowStatements, polygon::FinancialsClient::getCashFlowStatementsMetadata()},
      {DataCategory::IncomeStatements, polygon::FinancialsClient::getIncomeStatementsMetadata()},
      {DataCategory::Ratios, polygon::RatiosClient::getMetadata()},

      // Short interest & volume
      {DataCategory::ShortInterest, polygon::ShortInterestClient::getMetadata()},
      {DataCategory::ShortVolume, polygon::ShortVolumeClient::getMetadata()},
  };
  return registry;
}

DataFrameMetadata MetadataRegistry::GetMetadataForCategory(DataCategory category) {
  const auto& registry = GetRegistry();
  auto it = registry.find(category);

  if (it == registry.end()) {
    throw std::invalid_argument(
        "Unsupported DataCategory: " +
        DataCategoryWrapper::ToString(category));
  }

  return it->second;
}

DataFrameMetadata MetadataRegistry::GetAlfredMetadata() {
  return fred::AlfredClient::getMetadata();
}

DataFrameMetadata MetadataRegistry::GetIndicesMetadata(bool is_eod) {
  // Market indices use the same OHLCV schema as AggsClient
  auto metadata = is_eod
    ? []() {
        auto meta = polygon::AggsClient::getMetadata();
        meta.index_normalized = true;  // Daily bars normalized to midnight UTC
        return meta;
      }()
    : polygon::AggsClient::getMetadata();  // Non-normalized (intraday timestamps)

  // Remove v, vw and n columns (indices don't have volume data)
  metadata.columns.erase(
      std::remove_if(metadata.columns.begin(), metadata.columns.end(),
                     [](const auto& col) { return col.id == "v" || col.id == "vw" || col.id == "n"; }),
      metadata.columns.end());

  return metadata;
}

DataFrameMetadata MetadataRegistry::GetMetadata(const std::string& key) {
  // Handle ReferenceAgg prefixes (IDX:, STK:, FX:, CRYPTO:)
  // These all use the same OHLCV schema but with different column prefixes
  if (key.starts_with("IDX:") || key.starts_with("STK:") ||
      key.starts_with("FX:") || key.starts_with("CRYPTO:")) {
    // Parse timespan from key: "IDX:SPX:daily" or "IDX:SPX:minute"
    // Note: timespan suffix is optional, defaults to daily
    bool is_eod = !key.ends_with(":minute");
    return GetIndicesMetadata(is_eod);  // Same OHLCV schema for all reference aggs
  }

  if (key == "Indices") {
    // Generic indices - default to daily/normalized
    return GetIndicesMetadata(true);
  }

  // Try to parse as CrossSectionalDataCategory (economic indicators)
  try {
    (void)CrossSectionalDataCategoryWrapper::FromString(key);  // Validate it's a valid category
    // Cross-sectional economic indicators use the same schema as ALFRED
    auto metadata = fred::AlfredClient::getMetadata();
    metadata.description = "Cross-sectional economic indicator: " + key;
    metadata.data_type = "economic_indicator";
    return metadata;
  } catch (const std::exception&) {
    // Not a cross-sectional category, continue to DataCategory
  }

  // Try to parse as DataCategory
  try {
    auto category = DataCategoryWrapper::FromString(key);
    return GetMetadataForCategory(category);
  } catch (const std::exception&) {
    throw std::invalid_argument(
        "Unsupported metadata key: " + key +
        ". Expected DataCategory name, CrossSectionalDataCategory name, or ReferenceAgg prefix (IDX:, STK:, FX:, CRYPTO:)");
  }
}

} // namespace data_sdk::dataloader
