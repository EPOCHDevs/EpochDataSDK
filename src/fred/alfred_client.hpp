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

// AlfredClient - ALFRED (Archival FRED) economic data client with revision tracking
// Returns all revisions of data with published_at timestamps for point-in-time backtesting
// DataFrame schema: [published_at, observation_date, value, revision]
//
// Use this client when you:
// - Need point-in-time data for backtesting
// - Want to avoid lookahead bias
// - Need to see how data was revised over time
// - Are building realistic historical simulations
//
// Example: CPI for Jan 2020 will have multiple rows:
//   - published_at=2020-02-13, observation_date=2020-01-01, value=257.9, revision=1
//   - published_at=2020-03-11, observation_date=2020-01-01, value=258.1, revision=2
//   - published_at=2020-04-10, observation_date=2020-01-01, value=258.0, revision=3
//
// For series not available in ALFRED, this client automatically falls back to FRED mode
class AlfredClient {
public:
  explicit AlfredClient(Options options);
  ~AlfredClient();

  // Prevent copying
  AlfredClient(const AlfredClient&) = delete;
  AlfredClient& operator=(const AlfredClient&) = delete;

  // Allow moving
  AlfredClient(AlfredClient&&) noexcept;
  AlfredClient& operator=(AlfredClient&&) noexcept;

  // Core series fetching method
  // Returns all revisions for observation dates in [from, to]
  // Revisions are numbered 1, 2, 3, ... for each observation_date
  Expected<epoch_frame::DataFrame>
  getSeries(const std::string &series_id,
            const std::string &from,
            const std::string &to) const;

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getSeriesAsync(std::string series_id,
                 std::string from,
                 std::string to) const;

  // Convenience methods for common economic series

  // Consumer Price Index for All Urban Consumers (CPI-U) with revisions
  Expected<epoch_frame::DataFrame>
  getCPI(const std::string &from, const std::string &to) const {
    return getSeries("CPIAUCSL", from, to);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getCPIAsync(std::string from, std::string to) const {
    return getSeriesAsync("CPIAUCSL", std::move(from), std::move(to));
  }

  // Federal Funds Effective Rate with revisions
  Expected<epoch_frame::DataFrame>
  getFedFunds(const std::string &from, const std::string &to) const {
    return getSeries("DFF", from, to);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getFedFundsAsync(std::string from, std::string to) const {
    return getSeriesAsync("DFF", std::move(from), std::move(to));
  }

  // Real Gross Domestic Product with revisions
  Expected<epoch_frame::DataFrame>
  getGDP(const std::string &from, const std::string &to) const {
    return getSeries("GDPC1", from, to);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getGDPAsync(std::string from, std::string to) const {
    return getSeriesAsync("GDPC1", std::move(from), std::move(to));
  }

  // Unemployment Rate with revisions
  Expected<epoch_frame::DataFrame>
  getUnemployment(const std::string &from, const std::string &to) const {
    return getSeries("UNRATE", from, to);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getUnemploymentAsync(std::string from, std::string to) const {
    return getSeriesAsync("UNRATE", std::move(from), std::move(to));
  }

  // 10-Year Treasury Constant Maturity Rate with revisions
  Expected<epoch_frame::DataFrame>
  getTreasury10Y(const std::string &from, const std::string &to) const {
    return getSeries("DGS10", from, to);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getTreasury10YAsync(std::string from, std::string to) const {
    return getSeriesAsync("DGS10", std::move(from), std::move(to));
  }

  /**
   * Get metadata describing the DataFrame structure returned by AlfredClient
   * @return DataFrameMetadata with column definitions including revision tracking
   */
  static data_sdk::DataFrameMetadata getMetadata();

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::fred
