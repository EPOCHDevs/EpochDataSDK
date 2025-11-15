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

// Options for quotes data requests
struct QuotesOptions {
  std::string ticker;
  std::string from_date;  // "YYYY-MM-DD"
  std::string to_date;    // "YYYY-MM-DD"
  std::optional<int> limit = std::nullopt;
};

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

  // Struct-based overload
  Expected<epoch_frame::DataFrame>
  getQuotes(const QuotesOptions &opts) const {
    return getQuotes(opts.ticker, opts.from_date, opts.to_date, opts.limit);
  }

  // Async variant - pass-by-value to avoid coroutine lifetime issues
  drogon::Task<Expected<epoch_frame::DataFrame>>
  getQuotesAsync(std::string ticker, std::string from_date,
                 std::string to_date, std::optional<int> limit = std::nullopt) const;

  // Struct-based async overload
  drogon::Task<Expected<epoch_frame::DataFrame>>
  getQuotesAsync(QuotesOptions opts) const {
    return getQuotesAsync(std::move(opts.ticker), std::move(opts.from_date),
                          std::move(opts.to_date), opts.limit);
  }

  // Get metadata describing the DataFrame structure returned by getQuotes()
  static data_sdk::DataFrameMetadata getMetadata();

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::polygon
