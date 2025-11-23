#pragma once
#include <epoch_data_sdk/common/enums.hpp>
#include <epoch_frame/dataframe.h>
#include <epoch_frame/datetime.h>
#include <drogon/drogon.h>
#include <expected>

namespace data_sdk::dataloader {

// Interface for cross-sectional economic data fetching
// Fetches data without requiring an Asset - uses CrossSectionalDataCategory directly
struct ICrossSectionalFetcher {
  using FetchResult = std::expected<epoch_frame::DataFrame, std::string>;

  virtual ~ICrossSectionalFetcher() = default;

  // Synchronous fetch
  virtual FetchResult Fetch(CrossSectionalDataCategory category,
                           const epoch_frame::Date &fromDate,
                           const epoch_frame::Date &toDate) const = 0;

  // Async fetch for concurrent operations
  virtual drogon::Task<FetchResult> FetchAsync(CrossSectionalDataCategory category,
                                              const epoch_frame::Date &fromDate,
                                              const epoch_frame::Date &toDate) const = 0;
};

} // namespace data_sdk::dataloader
