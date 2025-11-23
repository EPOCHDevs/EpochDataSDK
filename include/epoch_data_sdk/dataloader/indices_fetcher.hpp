#pragma once
#include <epoch_frame/dataframe.h>
#include <epoch_frame/datetime.h>
#include <drogon/drogon.h>
#include <expected>
#include <string>

namespace data_sdk::dataloader {

// Interface for indices data fetching
// Fetches data for market indices using ticker symbols (e.g., "SPX", "VIX", "NDX")
struct IIndicesFetcher {
  using FetchResult = std::expected<epoch_frame::DataFrame, std::string>;

  virtual ~IIndicesFetcher() = default;

  // Synchronous fetch
  // is_eod: true = daily bars, false = minute bars
  virtual FetchResult Fetch(const std::string& indexTicker,
                           const epoch_frame::Date &fromDate,
                           const epoch_frame::Date &toDate,
                           bool is_eod = true) const = 0;

  // Async fetch for concurrent operations
  // is_eod: true = daily bars, false = minute bars
  virtual drogon::Task<FetchResult> FetchAsync(const std::string& indexTicker,
                                              const epoch_frame::Date &fromDate,
                                              const epoch_frame::Date &toDate,
                                              bool is_eod = true) const = 0;
};

} // namespace data_sdk::dataloader
