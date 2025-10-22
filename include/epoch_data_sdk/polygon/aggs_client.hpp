#pragma once

#include <optional>
#include <string>

#include <epoch_frame/dataframe.h>

#include "base_client.hpp"

namespace data_sdk::polygon {

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
};

} // namespace data_sdk::polygon
