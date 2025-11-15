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


} // namespace data_sdk::dataloader
