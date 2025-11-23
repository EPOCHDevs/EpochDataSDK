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

DataFrameMetadata MetadataRegistry::GetCrossSectionalMetadata(CrossSectionalDataCategory category) {
  // Cross-sectional economic indicators use the same schema as ALFRED
  // (published_at index, observation_date, value, revision columns)
  auto metadata = fred::AlfredClient::getMetadata();

  // Update description to be specific to the category
  metadata.description = "Cross-sectional economic indicator: " +
                        CrossSectionalDataCategoryWrapper::ToString(category);
  metadata.data_type = "economic_indicator";

  return metadata;
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
  // Handle specific market indices (IDX:SPX:daily, IDX:SPX:minute, or generic "Indices")
  if (key.starts_with("IDX:")) {
    // Parse timespan from key: "IDX:SPX:daily" or "IDX:SPX:minute"
    bool is_eod = key.ends_with(":daily");
    return GetIndicesMetadata(is_eod);
  }

  if (key == "Indices") {
    // Generic indices - default to daily/normalized
    return GetIndicesMetadata(true);
  }

  // Try to parse as CrossSectionalDataCategory (economic indicators)
  try {
    auto cross_cat = CrossSectionalDataCategoryWrapper::FromString(key);
    return GetCrossSectionalMetadata(cross_cat);
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
        ". Expected DataCategory name, CrossSectionalDataCategory name, or 'IDX:<ticker>'");
  }
}

} // namespace data_sdk::dataloader
