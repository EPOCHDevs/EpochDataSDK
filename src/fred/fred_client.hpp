#pragma once

#include <memory>
#include <string>

#include <drogon/drogon.h>
#include <epoch_frame/dataframe.h>
#include <epoch_data_sdk/common/metadata.hpp>

#include "error.hpp"
#include "options.hpp"

namespace data_sdk::fred {

template <typename T> using Expected = std::expected<T, HttpError>;

// FredClient - Simple FRED economic data client
// Returns latest/current revised values only (no revision history)
// DataFrame schema: [observation_date, value]
//
// Use this client when you:
// - Want current economic data
// - Don't need revision history
// - Don't need point-in-time backtesting
//
// For revision tracking and point-in-time data, use AlfredClient instead
class FredClient {
public:
  explicit FredClient(Options options);
  ~FredClient();

  // Prevent copying
  FredClient(const FredClient&) = delete;
  FredClient& operator=(const FredClient&) = delete;

  // Allow moving
  FredClient(FredClient&&) noexcept;
  FredClient& operator=(FredClient&&) noexcept;

  // Core series fetching method
  Expected<epoch_frame::DataFrame>
  getSeries(const std::string &series_id,
            const std::string &from,
            const std::string &to) const;

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getSeriesAsync(std::string series_id,
                 std::string from,
                 std::string to) const;

  // Convenience methods for common economic series

  // Consumer Price Index for All Urban Consumers (CPI-U)
  Expected<epoch_frame::DataFrame>
  getCPI(const std::string &from, const std::string &to) const {
    return getSeries("CPIAUCSL", from, to);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getCPIAsync(std::string from, std::string to) const {
    return getSeriesAsync("CPIAUCSL", std::move(from), std::move(to));
  }

  // Federal Funds Effective Rate
  Expected<epoch_frame::DataFrame>
  getFedFunds(const std::string &from, const std::string &to) const {
    return getSeries("DFF", from, to);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getFedFundsAsync(std::string from, std::string to) const {
    return getSeriesAsync("DFF", std::move(from), std::move(to));
  }

  // Real Gross Domestic Product
  Expected<epoch_frame::DataFrame>
  getGDP(const std::string &from, const std::string &to) const {
    return getSeries("GDPC1", from, to);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getGDPAsync(std::string from, std::string to) const {
    return getSeriesAsync("GDPC1", std::move(from), std::move(to));
  }

  // Unemployment Rate
  Expected<epoch_frame::DataFrame>
  getUnemployment(const std::string &from, const std::string &to) const {
    return getSeries("UNRATE", from, to);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getUnemploymentAsync(std::string from, std::string to) const {
    return getSeriesAsync("UNRATE", std::move(from), std::move(to));
  }

  // 10-Year Treasury Constant Maturity Rate
  Expected<epoch_frame::DataFrame>
  getTreasury10Y(const std::string &from, const std::string &to) const {
    return getSeries("DGS10", from, to);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getTreasury10YAsync(std::string from, std::string to) const {
    return getSeriesAsync("DGS10", std::move(from), std::move(to));
  }

  /**
   * Get metadata describing the DataFrame structure returned by FredClient
   * @return DataFrameMetadata with column definitions
   */
  static data_sdk::DataFrameMetadata getMetadata();

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::fred
