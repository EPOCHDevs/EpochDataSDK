#pragma once

#include <optional>
#include <string>

#include <epoch_frame/dataframe.h>
#include <expected>

#include "error.hpp"
#include "options.hpp"

namespace data_sdk::trading_economics {

template <typename T> using Expected = std::expected<T, HttpError>;

// MarketsClient - Handles financial markets data
// Supports: commodities, currencies, bonds, indexes, crypto
// Critical for backtesting multi-asset strategies
class MarketsClient {
public:
  explicit MarketsClient(Options options);
  ~MarketsClient();

  // Prevent copying
  MarketsClient(const MarketsClient&) = delete;
  MarketsClient& operator=(const MarketsClient&) = delete;

  // Allow moving
  MarketsClient(MarketsClient&&) = default;
  MarketsClient& operator=(MarketsClient&&) = default;

  // Get latest market data by asset class
  // Parameters:
  //   markets_field: Asset class ("commodities", "currency", "index", "bond", "crypto")
  //   type: Bond type filter (optional, only for bonds: "2Y", "5Y", "10Y", "15Y", "20Y", "30Y")
  // Returns: DataFrame with latest market values
  Expected<epoch_frame::DataFrame>
  getMarketsData(const std::string& markets_field,
                 std::optional<std::string> type = std::nullopt) const;

  // Get currency cross rates
  // Parameters:
  //   cross: Base currency (e.g., "EUR", "USD")
  // Returns: DataFrame with currency cross rates
  Expected<epoch_frame::DataFrame>
  getCurrencyCross(const std::string& cross) const;

  // Get markets data by specific symbols
  // Parameters:
  //   symbols: Single symbol or comma-separated list (e.g., "aapl:us", "indu:ind")
  // Returns: DataFrame with market data for the symbols
  Expected<epoch_frame::DataFrame>
  getMarketsBySymbol(const std::string& symbols) const;

  // Get intraday market data
  // Parameters:
  //   symbols: Single symbol or comma-separated list
  //   from_date: Start date in YYYY-MM-DD format (optional)
  //   to_date: End date in YYYY-MM-DD format (optional)
  // Returns: DataFrame with intraday OHLC data
  Expected<epoch_frame::DataFrame>
  getMarketsIntraday(const std::string& symbols,
                     std::optional<std::string> from_date = std::nullopt,
                     std::optional<std::string> to_date = std::nullopt) const;

  // Get peer markets (similar markets)
  // Parameters:
  //   symbols: Single symbol or comma-separated list
  // Returns: DataFrame with peer markets
  Expected<epoch_frame::DataFrame>
  getMarketsPeers(const std::string& symbols) const;

  // Get index components
  // Parameters:
  //   symbols: Index symbol or comma-separated list (e.g., "psi20:ind")
  // Returns: DataFrame with index components
  Expected<epoch_frame::DataFrame>
  getMarketsComponents(const std::string& symbols) const;

  // Search markets by country and/or category
  // Parameters:
  //   country: Country name (required)
  //   category: Category filter (optional, e.g., "index", "markets")
  //   page: Page number for pagination (optional)
  // Returns: DataFrame with search results
  Expected<epoch_frame::DataFrame>
  getMarketsSearch(const std::string& country,
                   std::optional<std::string> category = std::nullopt,
                   std::optional<int> page = std::nullopt) const;

  // Get market forecasts
  // Parameters:
  //   category: Category filter (optional, e.g., "bond", "index")
  //   symbol: Symbol filter (optional, single or comma-separated list)
  // Returns: DataFrame with market forecasts
  Expected<epoch_frame::DataFrame>
  getMarketsForecasts(std::optional<std::string> category = std::nullopt,
                      std::optional<std::string> symbol = std::nullopt) const;

  // Get aggregate intraday data by interval
  // Parameters:
  //   symbol: Single symbol or comma-separated list
  //   interval: Interval ("1m", "5m", "10m", "15m", "30m", "1h", "2h", "4h")
  //   from_date: Start date in YYYY-MM-DD format (required)
  //   to_date: End date in YYYY-MM-DD format (required)
  // Returns: DataFrame with aggregated OHLCV data
  Expected<epoch_frame::DataFrame>
  getMarketsIntradayByInterval(const std::string& symbol,
                                const std::string& interval,
                                const std::string& from_date,
                                const std::string& to_date) const;

  // Get stock descriptions
  // Parameters:
  //   symbol: Symbol or comma-separated list (optional)
  //   country: Country or comma-separated list (optional)
  // Note: Must provide either symbol OR country, not both
  // Returns: DataFrame with stock descriptions
  Expected<epoch_frame::DataFrame>
  getMarketsStockDescriptions(std::optional<std::string> symbol = std::nullopt,
                              std::optional<std::string> country = std::nullopt) const;

  // Get market symbology (convert between different symbol formats)
  // Parameters:
  //   symbol: Trading Economics symbol (optional, e.g., "AAPL:US")
  //   ticker: Ticker symbol (optional, e.g., "aapl")
  //   isin: ISIN code (optional, e.g., "US0378331005")
  //   country: Country name (optional)
  // Note: Provide exactly ONE parameter
  // Returns: DataFrame with symbol mappings
  Expected<epoch_frame::DataFrame>
  getMarketsSymbology(std::optional<std::string> symbol = std::nullopt,
                      std::optional<std::string> ticker = std::nullopt,
                      std::optional<std::string> isin = std::nullopt,
                      std::optional<std::string> country = std::nullopt) const;

  // Get stocks list by country
  // Parameters:
  //   country: Country name or comma-separated list (required)
  // Returns: DataFrame with stocks available for the country
  Expected<epoch_frame::DataFrame>
  getStocksByCountry(const std::string& country) const;

  // Get historical market data
  // Parameters:
  //   symbol: Single symbol or comma-separated list (required)
  //   from_date: Start date in YYYY-MM-DD format (optional)
  //   to_date: End date in YYYY-MM-DD format (optional)
  // Returns: DataFrame with historical OHLC data
  Expected<epoch_frame::DataFrame>
  getMarketsHistorical(const std::string& symbol,
                       std::optional<std::string> from_date = std::nullopt,
                       std::optional<std::string> to_date = std::nullopt) const;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::trading_economics
