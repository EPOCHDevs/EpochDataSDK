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

// Options for short interest data requests
struct ShortInterestOptions {
  std::string ticker;
  std::string date_from;  // "YYYY-MM-DD"
  std::string date_to;    // "YYYY-MM-DD"
  std::optional<int> limit = std::nullopt;
};

// ShortInterestClient - Handles short interest data for stocks
class ShortInterestClient {
public:
  explicit ShortInterestClient(Options options);
  ~ShortInterestClient();

  // Prevent copying
  ShortInterestClient(const ShortInterestClient&) = delete;
  ShortInterestClient& operator=(const ShortInterestClient&) = delete;

  // Allow moving
  ShortInterestClient(ShortInterestClient&&) = default;
  ShortInterestClient& operator=(ShortInterestClient&&) = default;

  // Get short interest data for a ticker within a settlement date range
  // date_from/date_to: Date strings in YYYY-MM-DD format
  // limit: Maximum number of results (optional)
  Expected<epoch_frame::DataFrame>
  getShortInterest(const std::string &ticker,
                   const std::string &date_from,
                   const std::string &date_to,
                   std::optional<int> limit = std::nullopt) const;

  // Struct-based overload
  Expected<epoch_frame::DataFrame>
  getShortInterest(const ShortInterestOptions &opts) const {
    return getShortInterest(opts.ticker, opts.date_from, opts.date_to, opts.limit);
  }

  // Async variant - pass-by-value to avoid coroutine lifetime issues
  drogon::Task<Expected<epoch_frame::DataFrame>>
  getShortInterestAsync(std::string ticker, std::string date_from,
                        std::string date_to, std::optional<int> limit = std::nullopt) const;

  // Struct-based async overload
  drogon::Task<Expected<epoch_frame::DataFrame>>
  getShortInterestAsync(ShortInterestOptions opts) const {
    return getShortInterestAsync(std::move(opts.ticker), std::move(opts.date_from),
                                 std::move(opts.date_to), opts.limit);
  }

  // Get metadata describing the DataFrame structure returned by getShortInterest()
  static data_sdk::DataFrameMetadata getMetadata();

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::polygon
