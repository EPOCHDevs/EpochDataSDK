#pragma once

#include <optional>
#include <string>

#include <epoch_frame/dataframe.h>
#include <expected>

#include "error.hpp"
#include "options.hpp"

namespace data_sdk::trading_economics {

template <typename T> using Expected = std::expected<T, HttpError>;

// EurostatClient - Handles European statistics data
// Provides comprehensive European economic and social indicators
// Important for European market analysis and research
class EurostatClient {
public:
  explicit EurostatClient(Options options);
  ~EurostatClient();

  // Prevent copying
  EurostatClient(const EurostatClient&) = delete;
  EurostatClient& operator=(const EurostatClient&) = delete;

  // Allow moving
  EurostatClient(EurostatClient&&) = default;
  EurostatClient& operator=(EurostatClient&&) = default;

  // Get Eurostat data by country, category, category_group, or symbol
  // Parameters:
  //   country: Country name (optional)
  //   category: Category name (optional, e.g., "People at risk of income poverty after social transfers")
  //   category_group: Category group (optional, e.g., "Poverty")
  //   lists: Get available lists ("categories" or "countries") (optional)
  //   symbol: Eurostat symbol/ID (optional, e.g., "51640")
  // Note: At least one parameter must be provided
  // Returns: DataFrame with Eurostat data
  Expected<epoch_frame::DataFrame>
  getEurostatData(std::optional<std::string> country = std::nullopt,
                  std::optional<std::string> category = std::nullopt,
                  std::optional<std::string> category_group = std::nullopt,
                  std::optional<std::string> lists = std::nullopt,
                  std::optional<std::string> symbol = std::nullopt) const;

  // Get list of available Eurostat countries
  // Returns: DataFrame with available countries
  Expected<epoch_frame::DataFrame>
  getEurostatCountries() const;

  // Get list of available Eurostat categories and category groups
  // Returns: DataFrame with categories and category groups
  Expected<epoch_frame::DataFrame>
  getEurostatCategoryGroups() const;

  // Get historical Eurostat data
  // Parameters:
  //   ID: Eurostat ID or comma-separated list (required, e.g., "24804")
  //   from_date: Start date in YYYY-MM-DD format (optional)
  //   to_date: End date in YYYY-MM-DD format (optional)
  // Returns: DataFrame with historical Eurostat data
  Expected<epoch_frame::DataFrame>
  getHistoricalEurostat(const std::string& ID,
                        std::optional<std::string> from_date = std::nullopt,
                        std::optional<std::string> to_date = std::nullopt) const;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::trading_economics
