#pragma once

#include <memory>
#include <optional>
#include <string>

#include <epoch_frame/dataframe.h>

#include "error.hpp"
#include "options.hpp"

namespace data_sdk::polygon {

template <typename T> using Expected = std::expected<T, HttpError>;

// EconomyClient - Handles U.S. economic data from Polygon's Fed endpoints
// Provides treasury yields, inflation data, and inflation expectations
// Optimized for backtesting with simple date range queries
class EconomyClient {
public:
  explicit EconomyClient(Options options);
  ~EconomyClient();

  // Prevent copying
  EconomyClient(const EconomyClient&) = delete;
  EconomyClient& operator=(const EconomyClient&) = delete;

  // Allow moving
  EconomyClient(EconomyClient&&) = default;
  EconomyClient& operator=(EconomyClient&&) = default;

  // Get U.S. Treasury yields for various maturities (1-month to 30-year)
  // Essential for calculating risk-free rates in backtesting
  // from_date/to_date: Date strings in YYYY-MM-DD format
  // Returns: DataFrame indexed by date with yield columns
  Expected<epoch_frame::DataFrame>
  getTreasuryYields(const std::string &from_date,
                    const std::string &to_date,
                    std::optional<int> limit = std::nullopt) const;

  // Get inflation data (CPI and PCE indexes)
  // Critical for real returns calculation in backtesting
  // from_date/to_date: Date strings in YYYY-MM-DD format
  // Returns: DataFrame indexed by date with inflation metrics
  Expected<epoch_frame::DataFrame>
  getInflation(const std::string &from_date,
               const std::string &to_date,
               std::optional<int> limit = std::nullopt) const;

  // Get inflation expectations from markets and Cleveland Fed models
  // Useful for forward-looking analysis in backtesting
  // from_date/to_date: Date strings in YYYY-MM-DD format
  // Returns: DataFrame indexed by date with expectation metrics
  Expected<epoch_frame::DataFrame>
  getInflationExpectations(const std::string &from_date,
                           const std::string &to_date,
                           std::optional<int> limit = std::nullopt) const;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::polygon
