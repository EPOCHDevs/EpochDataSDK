#pragma once

#include <memory>
#include <optional>
#include <string>

#include <epoch_frame/dataframe.h>

#include "error.hpp"
#include "options.hpp"

namespace data_sdk::fred {

template <typename T> using Expected = std::expected<T, HttpError>;

// SeriesClient - Handles FRED economic data series
// Provides time series data with ALFRED revision tracking for backtesting
// Supports point-in-time data retrieval for realistic historical analysis
class SeriesClient {
public:
  explicit SeriesClient(Options options);
  ~SeriesClient();

  // Prevent copying
  SeriesClient(const SeriesClient&) = delete;
  SeriesClient& operator=(const SeriesClient&) = delete;

  // Allow moving
  SeriesClient(SeriesClient&&) = default;
  SeriesClient& operator=(SeriesClient&&) = default;

  // Convenience methods for common economic series

  // Consumer Price Index for All Urban Consumers (CPI-U)
  Expected<epoch_frame::DataFrame>
  getCPI(const std::string &from, const std::string &to) const {
    return getSeries("CPIAUCSL", from, to);
  }

  // Effective Federal Funds Rate
  Expected<epoch_frame::DataFrame>
  getFedFunds(const std::string &from, const std::string &to) const {
    return getSeries("DFF", from, to);
  }

  // Real Gross Domestic Product
  Expected<epoch_frame::DataFrame>
  getGDP(const std::string &from, const std::string &to) const {
    return getSeries("GDPC1", from, to);
  }

  // Unemployment Rate
  Expected<epoch_frame::DataFrame>
  getUnemployment(const std::string &from, const std::string &to) const {
    return getSeries("UNRATE", from, to);
  }

  // 10-Year Treasury Constant Maturity Rate
  Expected<epoch_frame::DataFrame>
  getTreasury10Y(const std::string &from, const std::string &to) const {
    return getSeries("DGS10", from, to);
  }

  // Core CPI (CPI less Food and Energy)
  Expected<epoch_frame::DataFrame>
  getCoreCPI(const std::string &from, const std::string &to) const {
    return getSeries("CPILFESL", from, to);
  }

  // Personal Consumption Expenditures Price Index (Fed's preferred inflation measure)
  Expected<epoch_frame::DataFrame>
  getPCE(const std::string &from, const std::string &to) const {
    return getSeries("PCEPI", from, to);
  }

  // Core PCE (PCE less Food and Energy)
  Expected<epoch_frame::DataFrame>
  getCorePCE(const std::string &from, const std::string &to) const {
    return getSeries("PCEPILFE", from, to);
  }

  // 2-Year Treasury Constant Maturity Rate
  Expected<epoch_frame::DataFrame>
  getTreasury2Y(const std::string &from, const std::string &to) const {
    return getSeries("DGS2", from, to);
  }

  // 5-Year Treasury Constant Maturity Rate
  Expected<epoch_frame::DataFrame>
  getTreasury5Y(const std::string &from, const std::string &to) const {
    return getSeries("DGS5", from, to);
  }

  // 30-Year Treasury Constant Maturity Rate
  Expected<epoch_frame::DataFrame>
  getTreasury30Y(const std::string &from, const std::string &to) const {
    return getSeries("DGS30", from, to);
  }

  // 3-Month Treasury Bill Rate
  Expected<epoch_frame::DataFrame>
  getTreasury3M(const std::string &from, const std::string &to) const {
    return getSeries("DTB3", from, to);
  }

  // Initial Jobless Claims (weekly)
  Expected<epoch_frame::DataFrame>
  getInitialClaims(const std::string &from, const std::string &to) const {
    return getSeries("ICSA", from, to);
  }

  // Nonfarm Payrolls
  Expected<epoch_frame::DataFrame>
  getNonfarmPayrolls(const std::string &from, const std::string &to) const {
    return getSeries("PAYEMS", from, to);
  }

  // Industrial Production Index
  Expected<epoch_frame::DataFrame>
  getIndustrialProduction(const std::string &from, const std::string &to) const {
    return getSeries("INDPRO", from, to);
  }

  // Consumer Sentiment Index (University of Michigan)
  Expected<epoch_frame::DataFrame>
  getConsumerSentiment(const std::string &from, const std::string &to) const {
    return getSeries("UMCSENT", from, to);
  }

  // Retail Sales
  Expected<epoch_frame::DataFrame>
  getRetailSales(const std::string &from, const std::string &to) const {
    return getSeries("RSXFS", from, to);
  }

  // Housing Starts
  Expected<epoch_frame::DataFrame>
  getHousingStarts(const std::string &from, const std::string &to) const {
    return getSeries("HOUST", from, to);
  }

  // M2 Money Supply
  Expected<epoch_frame::DataFrame>
  getM2MoneySupply(const std::string &from, const std::string &to) const {
    return getSeries("M2SL", from, to);
  }

  // S&P 500 Index
  Expected<epoch_frame::DataFrame>
  getSP500(const std::string &from, const std::string &to) const {
    return getSeries("SP500", from, to);
  }

  // VIX Volatility Index
  Expected<epoch_frame::DataFrame>
  getVIX(const std::string &from, const std::string &to) const {
    return getSeries("VIXCLS", from, to);
  }

private:
  class Impl;
  std::unique_ptr<Impl> impl_;

  // Get series observations for any FRED series ID
  // series_id: FRED series identifier (e.g., "CPIAUCSL", "DFF")
  // from/to: Observation date range in YYYY-MM-DD format
  // published_from/published_to: Optional ALFRED realtime period filters
  //   - Use these to get data as it existed on specific historical dates
  //   - Essential for point-in-time backtesting
  // Returns: DataFrame indexed by observation date with columns:
  //   - "value": The series value
  //   - "published_at": When data was published (if ALFRED filters used)
  Expected<epoch_frame::DataFrame>
  getSeries(const std::string &series_id,
            const std::string &from,
            const std::string &to,
            std::optional<std::string> published_from = std::nullopt,
            std::optional<std::string> published_to = std::nullopt) const;
};

} // namespace data_sdk::fred
