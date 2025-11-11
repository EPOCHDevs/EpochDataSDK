#pragma once
#include <epoch_data_sdk/common/enums.hpp>
#include <epoch_frame/dataframe.h>
#include <epoch_frame/datetime.h>
#include <epoch_data_sdk/model/asset/asset.hpp>
#include <drogon/drogon.h>
#include <expected>
#include <unordered_map>

namespace data_sdk::dataloader {

// Interface for data fetching from various providers
struct IDataFetcher {
  using FetchResult = std::expected<epoch_frame::DataFrame, std::string>;
  using Parameters = std::unordered_map<std::string, std::string>;

  virtual ~IDataFetcher() = default;

  // Synchronous fetch
  // parameters: Optional key-value pairs for filtering/configuration
  //   Examples: {"type": "balance_sheet"}, {"transaction_code": "P", "min_value": "100000"}
  virtual FetchResult Fetch(const asset::Asset &asset,
                            DataCategory category,
                            const epoch_frame::Date &fromDate,
                            const epoch_frame::Date &toDate,
                            const Parameters& parameters = {}) const = 0;

  // Async fetch for concurrent operations
  virtual drogon::Task<FetchResult> FetchAsync(const asset::Asset &asset,
                                               DataCategory category,
                                               const epoch_frame::Date &fromDate,
                                               const epoch_frame::Date &toDate,
                                               Parameters parameters = {}) const = 0;
};

// Provider interface for routing fetchers based on asset/category
struct IFetcherProvider {
  virtual ~IFetcherProvider() = default;

  // Get the appropriate fetcher for this asset and category
  virtual IDataFetcher& Get(const asset::Asset& asset, DataCategory category) const = 0;
};

} // namespace data_sdk::dataloader
