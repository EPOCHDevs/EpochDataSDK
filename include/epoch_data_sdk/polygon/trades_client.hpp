#pragma once

#include <optional>
#include <string>

#include <epoch_frame/dataframe.h>

#include "error.hpp"
#include "options.hpp"

namespace data_sdk::polygon {

template <typename T> using Expected = std::expected<T, HttpError>;

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

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::polygon
