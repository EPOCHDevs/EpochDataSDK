#pragma once

#include <optional>
#include <string>

#include <epoch_frame/dataframe.h>

#include "error.hpp"
#include "options.hpp"

namespace data_sdk::polygon {

template <typename T> using Expected = std::expected<T, HttpError>;

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

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::polygon
