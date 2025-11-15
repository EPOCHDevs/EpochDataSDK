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

// Options for trades data requests
struct TradesOptions {
  std::string ticker;
  std::string from_date;  // "YYYY-MM-DD"
  std::string to_date;    // "YYYY-MM-DD"
  std::optional<int> limit = std::nullopt;
};

// TradesClient - Handles historical trade data
// Supports stocks, crypto historical trades
class TradesClient {
public:
  explicit TradesClient(Options options);
  ~TradesClient();

  // Prevent copying
  TradesClient(const TradesClient&) = delete;
  TradesClient& operator=(const TradesClient&) = delete;

  // Allow moving
  TradesClient(TradesClient&&) = default;
  TradesClient& operator=(TradesClient&&) = default;

  // Get historical trades for a ticker within a date range
  // from_date/to_date used as timestamp.gte/lte filters
  // Data is always returned in ascending chronological order (order=asc, sort=timestamp)
  Expected<epoch_frame::DataFrame>
  getTrades(const std::string &ticker, const std::string &from_date,
            const std::string &to_date, std::optional<int> limit = std::nullopt) const;

  // Struct-based overload
  Expected<epoch_frame::DataFrame>
  getTrades(const TradesOptions &opts) const {
    return getTrades(opts.ticker, opts.from_date, opts.to_date, opts.limit);
  }

  // Async variant - pass-by-value to avoid coroutine lifetime issues
  drogon::Task<Expected<epoch_frame::DataFrame>>
  getTradesAsync(std::string ticker, std::string from_date,
                 std::string to_date, std::optional<int> limit = std::nullopt) const;

  // Struct-based async overload
  drogon::Task<Expected<epoch_frame::DataFrame>>
  getTradesAsync(TradesOptions opts) const {
    return getTradesAsync(std::move(opts.ticker), std::move(opts.from_date),
                          std::move(opts.to_date), opts.limit);
  }

  // Get metadata describing the DataFrame structure returned by getTrades()
  static data_sdk::DataFrameMetadata getMetadata();

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::polygon
