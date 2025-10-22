#pragma once

#include <optional>
#include <string>

#include <epoch_frame/dataframe.h>
#include <expected>

#include "error.hpp"
#include "options.hpp"

namespace data_sdk::trading_economics {

template <typename T> using Expected = std::expected<T, HttpError>;

// ForecastsClient - Handles economic forecast data
// Important for analyzing consensus expectations and forecast vs actual deviations
class ForecastsClient {
public:
  explicit ForecastsClient(Options options);
  ~ForecastsClient();

  // Prevent copying
  ForecastsClient(const ForecastsClient&) = delete;
  ForecastsClient& operator=(const ForecastsClient&) = delete;

  // Allow moving
  ForecastsClient(ForecastsClient&&) = default;
  ForecastsClient& operator=(ForecastsClient&&) = default;

  // Get forecast data by country and/or indicator
  // At least one of country or indicator must be provided
  // Parameters:
  //   country: Single country name or comma-separated list (optional)
  //   indicator: Single indicator or comma-separated list (optional)
  // Returns: DataFrame with columns: Country, Category, LatestValue, Q1, Q2, Q3, Q4, YearEnd, etc.
  Expected<epoch_frame::DataFrame>
  getForecastData(std::optional<std::string> country = std::nullopt,
                  std::optional<std::string> indicator = std::nullopt) const;

  // Get forecast data by Trading Economics ticker
  // Parameters:
  //   ticker: Single ticker or comma-separated list (e.g., "USURTOT")
  // Returns: DataFrame with forecast values for the ticker(s)
  Expected<epoch_frame::DataFrame>
  getForecastByTicker(const std::string& ticker) const;

  // Get forecast updates/revisions
  // Parameters:
  //   country: Country filter (optional, can be list)
  //   init_date: Filter updates from this date in YYYY-MM-DD format (optional)
  // Returns: DataFrame with recently updated forecasts
  Expected<epoch_frame::DataFrame>
  getForecastUpdates(std::optional<std::string> country = std::nullopt,
                     std::optional<std::string> init_date = std::nullopt) const;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::trading_economics
