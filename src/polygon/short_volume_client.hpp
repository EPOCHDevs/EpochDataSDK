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

// Options for short volume data requests
struct ShortVolumeOptions {
  std::string ticker;
  std::string date_from;  // "YYYY-MM-DD"
  std::string date_to;    // "YYYY-MM-DD"
  std::optional<int> limit = std::nullopt;
};

// ShortVolumeClient - Handles short volume data for stocks
class ShortVolumeClient {
public:
  explicit ShortVolumeClient(Options options);
  ~ShortVolumeClient();

  // Prevent copying
  ShortVolumeClient(const ShortVolumeClient&) = delete;
  ShortVolumeClient& operator=(const ShortVolumeClient&) = delete;

  // Allow moving
  ShortVolumeClient(ShortVolumeClient&&) = default;
  ShortVolumeClient& operator=(ShortVolumeClient&&) = default;

  // Get short volume data for a ticker within a date range
  // date_from/date_to: Date strings in YYYY-MM-DD format
  // limit: Maximum number of results (optional)
  Expected<epoch_frame::DataFrame>
  getShortVolume(const std::string &ticker,
                 const std::string &date_from,
                 const std::string &date_to,
                 std::optional<int> limit = std::nullopt) const;

  // Struct-based overload
  Expected<epoch_frame::DataFrame>
  getShortVolume(const ShortVolumeOptions &opts) const {
    return getShortVolume(opts.ticker, opts.date_from, opts.date_to, opts.limit);
  }

  // Async variant - pass-by-value to avoid coroutine lifetime issues
  drogon::Task<Expected<epoch_frame::DataFrame>>
  getShortVolumeAsync(std::string ticker, std::string date_from,
                      std::string date_to, std::optional<int> limit = std::nullopt) const;

  // Struct-based async overload
  drogon::Task<Expected<epoch_frame::DataFrame>>
  getShortVolumeAsync(ShortVolumeOptions opts) const {
    return getShortVolumeAsync(std::move(opts.ticker), std::move(opts.date_from),
                               std::move(opts.date_to), opts.limit);
  }

  // Get metadata describing the DataFrame structure returned by getShortVolume()
  static data_sdk::DataFrameMetadata getMetadata();

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::polygon
