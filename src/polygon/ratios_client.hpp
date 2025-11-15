#pragma once

#include <optional>
#include <string>

#include <drogon/drogon.h>
#include <epoch_frame/dataframe.h>

#include "epoch_data_sdk/common/metadata.hpp"
#include "error.hpp"
#include "options.hpp"

namespace data_sdk::polygon {

template <typename T> using Expected = std::expected<T, HttpError>;

// Options for financial ratios data requests
struct RatiosOptions {
  std::optional<std::string> ticker = std::nullopt;
  std::optional<int> limit = std::nullopt;
  std::optional<std::string> sort = std::nullopt;  // e.g., "ticker.asc", "market_cap.desc"
};

// RatiosClient - Handles financial ratios data for stocks
class RatiosClient {
public:
  explicit RatiosClient(Options options);
  ~RatiosClient();

  // Prevent copying
  RatiosClient(const RatiosClient&) = delete;
  RatiosClient& operator=(const RatiosClient&) = delete;

  // Allow moving
  RatiosClient(RatiosClient&&) = default;
  RatiosClient& operator=(RatiosClient&&) = default;

  // Get financial ratios data
  // ticker: Optional ticker symbol to filter results
  // limit: Maximum number of results (optional)
  // sort: Sort field and order (optional, e.g., "ticker.asc")
  Expected<epoch_frame::DataFrame>
  getRatios(std::optional<std::string> ticker = std::nullopt,
            std::optional<int> limit = std::nullopt,
            std::optional<std::string> sort = std::nullopt) const;

  // Struct-based overload
  Expected<epoch_frame::DataFrame>
  getRatios(const RatiosOptions &opts) const {
    return getRatios(opts.ticker, opts.limit, opts.sort);
  }

  // Async variant - pass-by-value to avoid coroutine lifetime issues
  drogon::Task<Expected<epoch_frame::DataFrame>>
  getRatiosAsync(std::optional<std::string> ticker = std::nullopt,
                 std::optional<int> limit = std::nullopt,
                 std::optional<std::string> sort = std::nullopt) const;

  // Struct-based async overload
  drogon::Task<Expected<epoch_frame::DataFrame>>
  getRatiosAsync(RatiosOptions opts) const {
    return getRatiosAsync(opts.ticker, opts.limit, opts.sort);
  }

  // Get metadata describing the DataFrame structure returned by getRatios()
  static data_sdk::DataFrameMetadata getMetadata();

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::polygon
