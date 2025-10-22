#pragma once

#include <optional>
#include <string>

#include <epoch_frame/dataframe.h>

#include "error.hpp"
#include "options.hpp"

namespace data_sdk::polygon {

template <typename T> using Expected = std::expected<T, HttpError>;

// QuotesClient - Handles historical quote (NBBO) data
// Supports stocks, forex historical quotes
class QuotesClient {
public:
  explicit QuotesClient(Options options);
  ~QuotesClient();

  // Prevent copying
  QuotesClient(const QuotesClient&) = delete;
  QuotesClient& operator=(const QuotesClient&) = delete;

  // Allow moving
  QuotesClient(QuotesClient&&) = default;
  QuotesClient& operator=(QuotesClient&&) = default;

  // Get historical quotes for a ticker within a date range
  // from_date/to_date used as timestamp.gte/lte filters
  // Data is always returned in ascending chronological order (order=asc, sort=timestamp)
  Expected<epoch_frame::DataFrame>
  getQuotes(const std::string &ticker, const std::string &from_date,
            const std::string &to_date, std::optional<int> limit = std::nullopt) const;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::polygon
