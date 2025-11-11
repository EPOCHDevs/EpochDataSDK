#pragma once

#include <optional>
#include <string>

#include <epoch_frame/dataframe.h>
#include <expected>

#include "error.hpp"
#include "options.hpp"

namespace data_sdk::trading_economics {

template <typename T> using Expected = std::expected<T, HttpError>;

// FederalReserveClient - Handles US Federal Reserve regional economic data
// Provides granular state and county-level economic indicators
// Unique dataset for analyzing regional economic divergence
class FederalReserveClient {
public:
  explicit FederalReserveClient(Options options);
  ~FederalReserveClient();

  // Prevent copying
  FederalReserveClient(const FederalReserveClient&) = delete;
  FederalReserveClient& operator=(const FederalReserveClient&) = delete;

  // Allow moving
  FederalReserveClient(FederalReserveClient&&) = default;
  FederalReserveClient& operator=(FederalReserveClient&&) = default;

  // Get list of all US states or counties for a specific state
  // Parameters:
  //   county: State name to get counties for (optional)
  //           e.g., "arkansas" returns all counties in Arkansas
  //           If not provided, returns all US states
  // Returns: DataFrame with states or counties list
  Expected<epoch_frame::DataFrame>
  getFedRStates(std::optional<std::string> county = std::nullopt) const;

  // Get Federal Reserve snapshot data
  // Parameters:
  //   symbol: FED symbol or comma-separated list (optional, e.g., "ALLMARGATTN")
  //   url: Specific URL path (optional)
  //   country: Country filter (optional, typically "united states")
  //   state: State name or comma-separated list (optional)
  //   county: County name or comma-separated list (optional, e.g., "pike county, ar")
  //   page_number: Page number for pagination (optional)
  // Note: At least one parameter must be provided
  // Returns: DataFrame with snapshot data
  Expected<epoch_frame::DataFrame>
  getFedRSnaps(std::optional<std::string> symbol = std::nullopt,
               std::optional<std::string> url = std::nullopt,
               std::optional<std::string> country = std::nullopt,
               std::optional<std::string> state = std::nullopt,
               std::optional<std::string> county = std::nullopt,
               std::optional<int> page_number = std::nullopt) const;

  // Get county-level data
  // Parameters:
  //   state: State name (optional, e.g., "Nevada")
  //   county: County name (optional, e.g., "Pike County, AR")
  // Note: Provide either state OR county
  // Returns: DataFrame with county indicators or list of counties
  Expected<epoch_frame::DataFrame>
  getFedRCounty(std::optional<std::string> state = std::nullopt,
                std::optional<std::string> county = std::nullopt) const;

  // Get historical Federal Reserve data
  // Parameters:
  //   symbol: FED symbol or comma-separated list (required, e.g., "racedisparity005007")
  //   from_date: Start date in YYYY-MM-DD format (optional)
  //   to_date: End date in YYYY-MM-DD format (optional)
  // Returns: DataFrame with historical time series data
  Expected<epoch_frame::DataFrame>
  getFedRHistorical(const std::string& symbol,
                    std::optional<std::string> from_date = std::nullopt,
                    std::optional<std::string> to_date = std::nullopt) const;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::trading_economics
