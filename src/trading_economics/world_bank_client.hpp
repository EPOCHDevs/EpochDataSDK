#pragma once

#include <optional>
#include <string>

#include <epoch_frame/dataframe.h>
#include <expected>

#include "error.hpp"
#include "options.hpp"

namespace data_sdk::trading_economics {

template <typename T> using Expected = std::expected<T, HttpError>;

// WorldBankClient - Handles World Bank development indicators
// Provides long-term global development data
// Useful for macro research and long-term economic analysis
class WorldBankClient {
public:
  explicit WorldBankClient(Options options);
  ~WorldBankClient();

  // Prevent copying
  WorldBankClient(const WorldBankClient&) = delete;
  WorldBankClient& operator=(const WorldBankClient&) = delete;

  // Allow moving
  WorldBankClient(WorldBankClient&&) = default;
  WorldBankClient& operator=(WorldBankClient&&) = default;

  // Get World Bank categories
  // Parameters:
  //   category: Category name or comma-separated list (optional, e.g., "education", "agriculture")
  //   page_number: Page number for pagination (optional)
  // Returns: DataFrame with categories or indicators in a category
  Expected<epoch_frame::DataFrame>
  getWBCategories(std::optional<std::string> category = std::nullopt,
                  std::optional<int> page_number = std::nullopt) const;

  // Get World Bank indicator details
  // Parameters:
  //   series_code: World Bank series code (optional, e.g., "usa.fr.inr.rinr")
  //   url: URL path to indicator (optional, e.g., "/united-states/real-interest-rate-percent-wb-data.html")
  // Note: Provide either series_code OR url
  // Returns: DataFrame with indicator details
  Expected<epoch_frame::DataFrame>
  getWBIndicator(std::optional<std::string> series_code = std::nullopt,
                 std::optional<std::string> url = std::nullopt) const;

  // Get World Bank indicators available for a country
  // Parameters:
  //   country: Country name (required, e.g., "portugal")
  //   page_number: Page number for pagination (optional)
  // Returns: DataFrame with available indicators for the country
  Expected<epoch_frame::DataFrame>
  getWBCountry(const std::string& country,
               std::optional<int> page_number = std::nullopt) const;

  // Get historical World Bank data
  // Parameters:
  //   series_code: World Bank series code (required, e.g., "usa.fr.inr.rinr")
  // Returns: DataFrame with historical time series data
  Expected<epoch_frame::DataFrame>
  getWBHistorical(const std::string& series_code) const;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::trading_economics
