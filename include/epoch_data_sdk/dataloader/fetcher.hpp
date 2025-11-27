#pragma once
#include <epoch_data_sdk/common/enums.hpp>
#include <epoch_data_sdk/dataloader/fetch_kwargs.hpp>
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

  virtual ~IDataFetcher() = default;

  // ============================================================
  // Asset-based fetching (time series, fundamentals, etc.)
  // ============================================================

  // Synchronous fetch with kwargs
  virtual FetchResult Fetch(const asset::Asset &asset,
                            DataCategory category,
                            const epoch_frame::Date &fromDate,
                            const epoch_frame::Date &toDate,
                            const FetchKwargs &kwargs = NoKwargs{}) const = 0;

  // Async fetch with kwargs
  virtual drogon::Task<FetchResult> FetchAsync(const asset::Asset &asset,
                                               DataCategory category,
                                               const epoch_frame::Date &fromDate,
                                               const epoch_frame::Date &toDate,
                                               const FetchKwargs &kwargs = NoKwargs{}) const = 0;

  // ============================================================
  // Asset-less fetching (economic indicators, indices, etc.)
  // ============================================================

  // Synchronous fetch without asset (for cross-sectional data)
  virtual FetchResult Fetch(DataCategory category,
                            const epoch_frame::Date &fromDate,
                            const epoch_frame::Date &toDate,
                            const FetchKwargs &kwargs) const {
    // Default implementation - subclasses override for categories that support asset-less fetch
    return std::unexpected("Asset-less fetch not supported for this category");
  }

  // Async fetch without asset (for cross-sectional data)
  virtual drogon::Task<FetchResult> FetchAsync(DataCategory category,
                                               const epoch_frame::Date &fromDate,
                                               const epoch_frame::Date &toDate,
                                               const FetchKwargs &kwargs) const {
    // Default implementation - subclasses override for categories that support asset-less fetch
    co_return std::unexpected("Asset-less fetch not supported for this category");
  }
};

// Provider interface for routing fetchers based on asset/category
struct IFetcherProvider {
  virtual ~IFetcherProvider() = default;

  // Get the appropriate fetcher for this asset and category
  virtual IDataFetcher& Get(const asset::Asset& asset, DataCategory category) const = 0;

  // Get fetcher for asset-less categories (economic indicators, indices)
  virtual IDataFetcher& Get(DataCategory category) const = 0;
};

} // namespace data_sdk::dataloader
