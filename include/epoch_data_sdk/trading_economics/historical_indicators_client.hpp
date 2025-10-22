#pragma once

#include <optional>
#include <string>

#include <epoch_frame/dataframe.h>
#include <expected>

#include "error.hpp"
#include "options.hpp"

namespace data_sdk::trading_economics {

template <typename T> using Expected = std::expected<T, HttpError>;

// HistoricalIndicatorsClient - Handles historical economic indicator data
// Critical for backtesting: GDP, inflation, unemployment, interest rates, etc.
class HistoricalIndicatorsClient {
public:
  explicit HistoricalIndicatorsClient(Options options);
  ~HistoricalIndicatorsClient();

  // Prevent copying
  HistoricalIndicatorsClient(const HistoricalIndicatorsClient&) = delete;
  HistoricalIndicatorsClient& operator=(const HistoricalIndicatorsClient&) = delete;

  // Allow moving
  HistoricalIndicatorsClient(HistoricalIndicatorsClient&&) = default;
  HistoricalIndicatorsClient& operator=(HistoricalIndicatorsClient&&) = default;

  // Get historical data for specific country and indicator
  // Parameters:
  //   country: Single country name (e.g., "United States") or comma-separated list
  //   indicator: Single indicator (e.g., "GDP Growth Rate") or comma-separated list
  //   from_date: Start date in YYYY-MM-DD format (optional, defaults to 15 years ago)
  //   to_date: End date in YYYY-MM-DD format (optional)
  // Returns: DataFrame with columns: Country, Category, DateTime, Value, etc.
  Expected<epoch_frame::DataFrame>
  getHistoricalData(const std::string& country,
                    const std::string& indicator,
                    std::optional<std::string> from_date = std::nullopt,
                    std::optional<std::string> to_date = std::nullopt) const;

  // Get historical credit ratings for countries
  // Parameters:
  //   country: Single country name or comma-separated list
  //   rating: Optional rating filter (e.g., "S&P", "Moody's", "Fitch")
  //   from_date: Start date in YYYY-MM-DD format (optional)
  //   to_date: End date in YYYY-MM-DD format (optional)
  // Returns: DataFrame with columns: Country, Date, Agency, Rating, Outlook
  Expected<epoch_frame::DataFrame>
  getHistoricalRatings(const std::string& country,
                       std::optional<std::string> rating = std::nullopt,
                       std::optional<std::string> from_date = std::nullopt,
                       std::optional<std::string> to_date = std::nullopt) const;

  // Get historical data by Trading Economics ticker symbol
  // Parameters:
  //   ticker: Trading Economics ticker (e.g., "USURTOT" for US unemployment)
  //   start_date: Start date in YYYY-MM-DD format (optional)
  // Returns: DataFrame with historical values for the ticker
  Expected<epoch_frame::DataFrame>
  getHistoricalByTicker(const std::string& ticker,
                        std::optional<std::string> start_date = std::nullopt) const;

  // Get latest historical data updates across all indicators
  // Useful for checking what data has been recently updated
  // Returns: DataFrame with recently updated indicators
  Expected<epoch_frame::DataFrame>
  getHistoricalLatest() const;

  // Get historical data updates/changes
  // Returns: DataFrame with indicators that have been updated
  Expected<epoch_frame::DataFrame>
  getHistoricalUpdates() const;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::trading_economics
