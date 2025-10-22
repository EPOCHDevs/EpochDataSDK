#pragma once

#include <optional>
#include <string>

#include <epoch_frame/dataframe.h>

#include "error.hpp"
#include "options.hpp"

namespace data_sdk::polygon {

template <typename T> using Expected = std::expected<T, HttpError>;

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

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::polygon
