#pragma once

#include <optional>
#include <string>

#include <epoch_frame/dataframe.h>

#include "base_client.hpp"

namespace data_sdk::polygon {

// Options for aggregate data requests
struct AggsOptions {
  std::string ticker;
  std::string from_date;  // "YYYY-MM-DD"
  std::string to_date;    // "YYYY-MM-DD"
  bool is_eod = true;     // true = daily bars, false = minute bars
  std::optional<bool> adjusted = true;  // Adjust for splits/dividends
};

// AggsClient - Handles aggregate/OHLCV bar data
// Supports stocks, forex (C:), crypto (X:) historical data
class AggsClient : private BaseClient {
public:
  explicit AggsClient(Options options) : BaseClient(std::move(options)) {}
  ~AggsClient() = default;

  // Prevent copying
  AggsClient(const AggsClient&) = delete;
  AggsClient& operator=(const AggsClient&) = delete;

  // Allow moving
  AggsClient(AggsClient&&) = default;
  AggsClient& operator=(AggsClient&&) = default;

  // Get aggregate bars for a ticker over a date range
  // Works for stocks, crypto (X: prefix), forex (C: prefix)
  // For stocks: minute bars are filtered to NYSE RTH (09:31-16:00 ET)
  // is_eod: true = daily bars, false = minute bars
  // Data is always returned in ascending chronological order (sort=asc)
  Expected<epoch_frame::DataFrame>
  getAggregates(const std::string &ticker, const std::string &from_date,
                const std::string &to_date, bool is_eod,
                std::optional<bool> adjusted = true) const;

  // Struct-based overload for cleaner API
  Expected<epoch_frame::DataFrame>
  getAggregates(const AggsOptions &opts) const {
    return getAggregates(opts.ticker, opts.from_date, opts.to_date, opts.is_eod, opts.adjusted);
  }

  // Async variant of getAggregates - returns a coroutine
  // Use co_await to execute, or pass to batch utilities in common/async_batch.hpp
  // Example: auto df = co_await client.getAggregatesAsync("AAPL", from, to, true);
  // NOTE: Parameters are taken by value to avoid coroutine lifetime issues with temporaries
  drogon::Task<Expected<epoch_frame::DataFrame>>
  getAggregatesAsync(std::string ticker, std::string from_date,
                     std::string to_date, bool is_eod,
                     std::optional<bool> adjusted = true) const;

  // Struct-based async overload for cleaner API and batch operations
  drogon::Task<Expected<epoch_frame::DataFrame>>
  getAggregatesAsync(AggsOptions opts) const {
    return getAggregatesAsync(std::move(opts.ticker), std::move(opts.from_date),
                              std::move(opts.to_date), opts.is_eod, opts.adjusted);
  }
};

} // namespace data_sdk::polygon
