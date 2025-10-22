#pragma once

#include <optional>
#include <string>

#include <epoch_frame/dataframe.h>

#include "error.hpp"
#include "options.hpp"

namespace data_sdk::polygon {

template <typename T> using Expected = std::expected<T, HttpError>;

// SplitsClient - Handles stock split data
// CRITICAL for backtesting: adjusts historical prices for splits
class SplitsClient {
public:
  explicit SplitsClient(Options options);
  ~SplitsClient();

  // Prevent copying
  SplitsClient(const SplitsClient&) = delete;
  SplitsClient& operator=(const SplitsClient&) = delete;

  // Allow moving
  SplitsClient(SplitsClient&&) = default;
  SplitsClient& operator=(SplitsClient&&) = default;

  // Get historical stock splits
  // Essential for adjusting historical prices
  Expected<epoch_frame::DataFrame>
  getSplits(std::optional<std::string> ticker = std::nullopt,
            std::optional<std::string> execution_date = std::nullopt,
            std::optional<std::string> execution_date_gte = std::nullopt,
            std::optional<std::string> execution_date_lte = std::nullopt,
            std::optional<bool> reverse_split = std::nullopt,
            std::optional<int> limit = std::nullopt,
            std::optional<std::string> sort = std::string("execution_date"),
            std::optional<std::string> order = std::string("desc")) const;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::polygon
