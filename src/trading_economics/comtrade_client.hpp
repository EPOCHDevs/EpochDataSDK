#pragma once

#include <optional>
#include <string>

#include <epoch_frame/dataframe.h>
#include <expected>

#include "error.hpp"
#include "options.hpp"

namespace data_sdk::trading_economics {

template <typename T> using Expected = std::expected<T, HttpError>;

// ComtradeClient - Handles UN Comtrade international trade data
// Provides detailed import/export statistics between countries
// Useful for analyzing trade flows and economic relationships
class ComtradeClient {
public:
  explicit ComtradeClient(Options options);
  ~ComtradeClient();

  // Prevent copying
  ComtradeClient(const ComtradeClient&) = delete;
  ComtradeClient& operator=(const ComtradeClient&) = delete;

  // Allow moving
  ComtradeClient(ComtradeClient&&) = default;
  ComtradeClient& operator=(ComtradeClient&&) = default;

  // Get list of available Comtrade categories
  // Returns: DataFrame with all trade categories
  Expected<epoch_frame::DataFrame>
  getCmtCategories() const;

  // Get Comtrade country data
  // Parameters:
  //   country: Country name or comma-separated list (optional)
  //   page_number: Page number for pagination (optional)
  // Returns: DataFrame with country trade data
  Expected<epoch_frame::DataFrame>
  getCmtCountry(std::optional<std::string> country = std::nullopt,
                std::optional<int> page_number = std::nullopt) const;

  // Get historical Comtrade data by symbol
  // Parameters:
  //   symbol: Comtrade symbol (required, e.g., "PRTESP24031")
  // Returns: DataFrame with historical trade data
  Expected<epoch_frame::DataFrame>
  getCmtHistorical(const std::string& symbol) const;

  // Get Comtrade data between two countries
  // Parameters:
  //   country1: First country name (required)
  //   country2: Second country name (required)
  //   page_number: Page number for pagination (optional)
  // Returns: DataFrame with bilateral trade data
  Expected<epoch_frame::DataFrame>
  getCmtTwoCountries(const std::string& country1,
                     const std::string& country2,
                     std::optional<int> page_number = std::nullopt) const;

  // Get latest Comtrade updates
  // Returns: DataFrame with recent updates
  Expected<epoch_frame::DataFrame>
  getCmtUpdates() const;

  // Get Comtrade data by country, type, and category
  // Parameters:
  //   country: Country name (required)
  //   type: Trade type - "import" or "export" (required)
  //   category: Trade category (optional, e.g., "live animals", "Swine, live")
  // Returns: DataFrame with country trade by category
  Expected<epoch_frame::DataFrame>
  getCmtCountryByCategory(const std::string& country,
                          const std::string& type,
                          std::optional<std::string> category = std::nullopt) const;

  // Get total imports or exports for a country
  // Parameters:
  //   country: Country name (required)
  //   type: Trade type - "import" or "export" (required)
  // Returns: DataFrame with total trade values
  Expected<epoch_frame::DataFrame>
  getCmtTotalByType(const std::string& country,
                    const std::string& type) const;

  // Get trade data between countries filtered by type
  // Parameters:
  //   country1: First country name (required)
  //   country2: Second country name (optional)
  //   type: Trade type - "import" or "export" (required)
  // Returns: DataFrame with filtered bilateral trade
  Expected<epoch_frame::DataFrame>
  getCmtCountryFilterByType(const std::string& country1,
                            std::optional<std::string> country2,
                            const std::string& type) const;

  // Get snapshot of trade data filtered by type
  // Parameters:
  //   country: Country name (required)
  //   type: Trade type - "import" or "export" (required)
  // Returns: DataFrame with trade snapshot
  Expected<epoch_frame::DataFrame>
  getCmtSnapshotByType(const std::string& country,
                       const std::string& type) const;

  // Get last Comtrade updates by country or date
  // Parameters:
  //   country: Country name (optional, e.g., "portugal")
  //   start_date: Filter updates from this date in YYYY-MM-DD format (optional)
  // Returns: DataFrame with last updates
  Expected<epoch_frame::DataFrame>
  getCmtLastUpdates(std::optional<std::string> country = std::nullopt,
                    std::optional<std::string> start_date = std::nullopt) const;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::trading_economics
