#pragma once

#include <memory>
#include <optional>
#include <string>

#include <drogon/drogon.h>
#include <epoch_frame/dataframe.h>

#include "error.hpp"
#include "options.hpp"

namespace data_sdk::polygon {

template <typename T> using Expected = std::expected<T, HttpError>;

// Options for treasury yields data requests
struct TreasuryYieldsOptions {
  std::string from_date;  // "YYYY-MM-DD"
  std::string to_date;    // "YYYY-MM-DD"
  std::optional<int> limit = std::nullopt;
};

// Options for inflation data requests
struct InflationOptions {
  std::string from_date;  // "YYYY-MM-DD"
  std::string to_date;    // "YYYY-MM-DD"
  std::optional<int> limit = std::nullopt;
};

// Options for inflation expectations data requests
struct InflationExpectationsOptions {
  std::string from_date;  // "YYYY-MM-DD"
  std::string to_date;    // "YYYY-MM-DD"
  std::optional<int> limit = std::nullopt;
};

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

  // Struct-based overload
  Expected<epoch_frame::DataFrame>
  getTreasuryYields(const TreasuryYieldsOptions &opts) const {
    return getTreasuryYields(opts.from_date, opts.to_date, opts.limit);
  }

  // Async variant - pass-by-value to avoid coroutine lifetime issues
  drogon::Task<Expected<epoch_frame::DataFrame>>
  getTreasuryYieldsAsync(std::string from_date, std::string to_date,
                         std::optional<int> limit = std::nullopt) const;

  // Struct-based async overload
  drogon::Task<Expected<epoch_frame::DataFrame>>
  getTreasuryYieldsAsync(TreasuryYieldsOptions opts) const {
    return getTreasuryYieldsAsync(std::move(opts.from_date), std::move(opts.to_date), opts.limit);
  }

  // Get inflation data (CPI and PCE indexes)
  // Critical for real returns calculation in backtesting
  // from_date/to_date: Date strings in YYYY-MM-DD format
  // Returns: DataFrame indexed by date with inflation metrics
  Expected<epoch_frame::DataFrame>
  getInflation(const std::string &from_date,
               const std::string &to_date,
               std::optional<int> limit = std::nullopt) const;

  // Struct-based overload
  Expected<epoch_frame::DataFrame>
  getInflation(const InflationOptions &opts) const {
    return getInflation(opts.from_date, opts.to_date, opts.limit);
  }

  // Async variant - pass-by-value to avoid coroutine lifetime issues
  drogon::Task<Expected<epoch_frame::DataFrame>>
  getInflationAsync(std::string from_date, std::string to_date,
                    std::optional<int> limit = std::nullopt) const;

  // Struct-based async overload
  drogon::Task<Expected<epoch_frame::DataFrame>>
  getInflationAsync(InflationOptions opts) const {
    return getInflationAsync(std::move(opts.from_date), std::move(opts.to_date), opts.limit);
  }

  // Get inflation expectations from markets and Cleveland Fed models
  // Useful for forward-looking analysis in backtesting
  // from_date/to_date: Date strings in YYYY-MM-DD format
  // Returns: DataFrame indexed by date with expectation metrics
  Expected<epoch_frame::DataFrame>
  getInflationExpectations(const std::string &from_date,
                           const std::string &to_date,
                           std::optional<int> limit = std::nullopt) const;

  // Struct-based overload
  Expected<epoch_frame::DataFrame>
  getInflationExpectations(const InflationExpectationsOptions &opts) const {
    return getInflationExpectations(opts.from_date, opts.to_date, opts.limit);
  }

  // Async variant - pass-by-value to avoid coroutine lifetime issues
  drogon::Task<Expected<epoch_frame::DataFrame>>
  getInflationExpectationsAsync(std::string from_date, std::string to_date,
                                 std::optional<int> limit = std::nullopt) const;

  // Struct-based async overload
  drogon::Task<Expected<epoch_frame::DataFrame>>
  getInflationExpectationsAsync(InflationExpectationsOptions opts) const {
    return getInflationExpectationsAsync(std::move(opts.from_date), std::move(opts.to_date), opts.limit);
  }

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::polygon
