#pragma once

#include <optional>
#include <string>

#include <epoch_frame/dataframe.h>
#include <expected>

#include "error.hpp"
#include "options.hpp"

namespace data_sdk::trading_economics {

template <typename T> using Expected = std::expected<T, HttpError>;

// IndicatorsClient - Handles current economic indicator values and metadata
// Useful for getting latest values, discovering available indicators, and metadata
class IndicatorsClient {
public:
  explicit IndicatorsClient(Options options);
  ~IndicatorsClient();

  // Prevent copying
  IndicatorsClient(const IndicatorsClient&) = delete;
  IndicatorsClient& operator=(const IndicatorsClient&) = delete;

  // Allow moving
  IndicatorsClient(IndicatorsClient&&) = default;
  IndicatorsClient& operator=(IndicatorsClient&&) = default;

  // Get indicator data by country and/or indicator
  // Without parameters, returns all indicators
  // Parameters:
  //   country: Single country or comma-separated list (optional)
  //   indicators: Single indicator or comma-separated list (optional)
  //   calendar: If "1", returns only indicators that have calendar events (optional)
  // Returns: DataFrame with columns: Country, Category, LatestValue, LatestValueDate, etc.
  Expected<epoch_frame::DataFrame>
  getIndicatorData(std::optional<std::string> country = std::nullopt,
                   std::optional<std::string> indicators = std::nullopt,
                   std::optional<std::string> calendar = std::nullopt) const;

  // Get credit ratings by country
  // Parameters:
  //   country: Single country or comma-separated list (optional)
  //   rating: Rating agency filter (optional)
  // Returns: DataFrame with current credit ratings
  Expected<epoch_frame::DataFrame>
  getRatings(std::optional<std::string> country = std::nullopt,
             std::optional<std::string> rating = std::nullopt) const;

  // Get list of discontinued indicators
  // Parameters:
  //   country: Single country or comma-separated list (optional)
  // Returns: DataFrame with discontinued indicators
  Expected<epoch_frame::DataFrame>
  getDiscontinuedIndicator(std::optional<std::string> country = std::nullopt) const;

  // Get indicators by category group
  // Parameters:
  //   country: Single country or comma-separated list (required)
  //   category_group: Category group (required, e.g., "gdp", "labour", "markets")
  // Returns: DataFrame with indicators in the specified category group
  Expected<epoch_frame::DataFrame>
  getIndicatorByCategoryGroup(const std::string& country,
                              const std::string& category_group) const;

  // Get indicator by Trading Economics ticker
  // Parameters:
  //   ticker: Single ticker or comma-separated list (e.g., "USURTOT")
  // Returns: DataFrame with indicator details
  Expected<epoch_frame::DataFrame>
  getIndicatorByTicker(const std::string& ticker) const;

  // Get latest indicator updates
  // Parameters:
  //   country: Country filter (optional, can be list)
  //   init_date: Filter updates from this date in YYYY-MM-DD format (optional)
  //   time: Filter updates from this time in HH:MM format (optional, requires init_date)
  // Returns: DataFrame with recently updated indicators
  Expected<epoch_frame::DataFrame>
  getLatestUpdates(std::optional<std::string> country = std::nullopt,
                   std::optional<std::string> init_date = std::nullopt,
                   std::optional<std::string> time = std::nullopt) const;

  // Get peer indicators (similar indicators across countries or within a category)
  // Parameters:
  //   country: Country name (optional)
  //   category: Category name (optional)
  //   ticker: Ticker symbol (optional, e.g., "CPI YOY")
  // Returns: DataFrame with peer indicators
  Expected<epoch_frame::DataFrame>
  getPeers(std::optional<std::string> country = std::nullopt,
           std::optional<std::string> category = std::nullopt,
           std::optional<std::string> ticker = std::nullopt) const;

  // Get list of all available countries
  // Returns: DataFrame with all countries available in the API
  Expected<epoch_frame::DataFrame>
  getAllCountries() const;

  // Get indicator changes/revisions
  // Parameters:
  //   start_date: Filter changes from this date in YYYY-MM-DD format (optional)
  // Returns: DataFrame with indicator changes
  Expected<epoch_frame::DataFrame>
  getIndicatorChanges(std::optional<std::string> start_date = std::nullopt) const;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::trading_economics
