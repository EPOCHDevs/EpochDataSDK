#pragma once

#include <memory>
#include <optional>
#include <string>

#include <drogon/drogon.h>
#include <epoch_frame/dataframe.h>

#include "error.hpp"
#include "options.hpp"

namespace data_sdk::fred {

template <typename T> using Expected = std::expected<T, HttpError>;

// Options for FRED series requests
struct SeriesOptions {
  std::string series_id;
  std::string from;
  std::string to;
  bool use_alfred = true;
};

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
  getCPI(const std::string &from, const std::string &to, bool use_alfred = true) const {
    return getSeries("CPIAUCSL", from, to, use_alfred);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getCPIAsync(std::string from, std::string to, bool use_alfred = true) const {
    return getSeriesAsync("CPIAUCSL", std::move(from), std::move(to), use_alfred);
  }

  // Effective Federal Funds Rate
  Expected<epoch_frame::DataFrame>
  getFedFunds(const std::string &from, const std::string &to, bool use_alfred = true) const {
    return getSeries("DFF", from, to, use_alfred);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getFedFundsAsync(std::string from, std::string to, bool use_alfred = true) const {
    return getSeriesAsync("DFF", std::move(from), std::move(to), use_alfred);
  }

  // Real Gross Domestic Product
  Expected<epoch_frame::DataFrame>
  getGDP(const std::string &from, const std::string &to, bool use_alfred = true) const {
    return getSeries("GDPC1", from, to, use_alfred);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getGDPAsync(std::string from, std::string to, bool use_alfred = true) const {
    return getSeriesAsync("GDPC1", std::move(from), std::move(to), use_alfred);
  }

  // Unemployment Rate
  Expected<epoch_frame::DataFrame>
  getUnemployment(const std::string &from, const std::string &to, bool use_alfred = true) const {
    return getSeries("UNRATE", from, to, use_alfred);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getUnemploymentAsync(std::string from, std::string to, bool use_alfred = true) const {
    return getSeriesAsync("UNRATE", std::move(from), std::move(to), use_alfred);
  }

  // 10-Year Treasury Constant Maturity Rate
  Expected<epoch_frame::DataFrame>
  getTreasury10Y(const std::string &from, const std::string &to, bool use_alfred = true) const {
    return getSeries("DGS10", from, to, use_alfred);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getTreasury10YAsync(std::string from, std::string to, bool use_alfred = true) const {
    return getSeriesAsync("DGS10", std::move(from), std::move(to), use_alfred);
  }

  // Core CPI (CPI less Food and Energy)
  Expected<epoch_frame::DataFrame>
  getCoreCPI(const std::string &from, const std::string &to, bool use_alfred = true) const {
    return getSeries("CPILFESL", from, to, use_alfred);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getCoreCPIAsync(std::string from, std::string to, bool use_alfred = true) const {
    return getSeriesAsync("CPILFESL", std::move(from), std::move(to), use_alfred);
  }

  // Personal Consumption Expenditures Price Index (Fed's preferred inflation measure)
  Expected<epoch_frame::DataFrame>
  getPCE(const std::string &from, const std::string &to, bool use_alfred = true) const {
    return getSeries("PCEPI", from, to, use_alfred);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getPCEAsync(std::string from, std::string to, bool use_alfred = true) const {
    return getSeriesAsync("PCEPI", std::move(from), std::move(to), use_alfred);
  }

  // Core PCE (PCE less Food and Energy)
  Expected<epoch_frame::DataFrame>
  getCorePCE(const std::string &from, const std::string &to, bool use_alfred = true) const {
    return getSeries("PCEPILFE", from, to, use_alfred);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getCorePCEAsync(std::string from, std::string to, bool use_alfred = true) const {
    return getSeriesAsync("PCEPILFE", std::move(from), std::move(to), use_alfred);
  }

  // 2-Year Treasury Constant Maturity Rate
  Expected<epoch_frame::DataFrame>
  getTreasury2Y(const std::string &from, const std::string &to, bool use_alfred = true) const {
    return getSeries("DGS2", from, to, use_alfred);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getTreasury2YAsync(std::string from, std::string to, bool use_alfred = true) const {
    return getSeriesAsync("DGS2", std::move(from), std::move(to), use_alfred);
  }

  // 5-Year Treasury Constant Maturity Rate
  Expected<epoch_frame::DataFrame>
  getTreasury5Y(const std::string &from, const std::string &to, bool use_alfred = true) const {
    return getSeries("DGS5", from, to, use_alfred);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getTreasury5YAsync(std::string from, std::string to, bool use_alfred = true) const {
    return getSeriesAsync("DGS5", std::move(from), std::move(to), use_alfred);
  }

  // 30-Year Treasury Constant Maturity Rate
  Expected<epoch_frame::DataFrame>
  getTreasury30Y(const std::string &from, const std::string &to, bool use_alfred = true) const {
    return getSeries("DGS30", from, to, use_alfred);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getTreasury30YAsync(std::string from, std::string to, bool use_alfred = true) const {
    return getSeriesAsync("DGS30", std::move(from), std::move(to), use_alfred);
  }

  // 3-Month Treasury Bill Rate
  Expected<epoch_frame::DataFrame>
  getTreasury3M(const std::string &from, const std::string &to, bool use_alfred = true) const {
    return getSeries("DTB3", from, to, use_alfred);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getTreasury3MAsync(std::string from, std::string to, bool use_alfred = true) const {
    return getSeriesAsync("DTB3", std::move(from), std::move(to), use_alfred);
  }

  // Initial Jobless Claims (weekly)
  Expected<epoch_frame::DataFrame>
  getInitialClaims(const std::string &from, const std::string &to, bool use_alfred = true) const {
    return getSeries("ICSA", from, to, use_alfred);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getInitialClaimsAsync(std::string from, std::string to, bool use_alfred = true) const {
    return getSeriesAsync("ICSA", std::move(from), std::move(to), use_alfred);
  }

  // Nonfarm Payrolls
  Expected<epoch_frame::DataFrame>
  getNonfarmPayrolls(const std::string &from, const std::string &to, bool use_alfred = true) const {
    return getSeries("PAYEMS", from, to, use_alfred);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getNonfarmPayrollsAsync(std::string from, std::string to, bool use_alfred = true) const {
    return getSeriesAsync("PAYEMS", std::move(from), std::move(to), use_alfred);
  }

  // Industrial Production Index
  Expected<epoch_frame::DataFrame>
  getIndustrialProduction(const std::string &from, const std::string &to, bool use_alfred = true) const {
    return getSeries("INDPRO", from, to, use_alfred);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getIndustrialProductionAsync(std::string from, std::string to, bool use_alfred = true) const {
    return getSeriesAsync("INDPRO", std::move(from), std::move(to), use_alfred);
  }

  // Consumer Sentiment Index (University of Michigan)
  Expected<epoch_frame::DataFrame>
  getConsumerSentiment(const std::string &from, const std::string &to, bool use_alfred = true) const {
    return getSeries("UMCSENT", from, to, use_alfred);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getConsumerSentimentAsync(std::string from, std::string to, bool use_alfred = true) const {
    return getSeriesAsync("UMCSENT", std::move(from), std::move(to), use_alfred);
  }

  // Retail Sales
  Expected<epoch_frame::DataFrame>
  getRetailSales(const std::string &from, const std::string &to, bool use_alfred = true) const {
    return getSeries("RSXFS", from, to, use_alfred);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getRetailSalesAsync(std::string from, std::string to, bool use_alfred = true) const {
    return getSeriesAsync("RSXFS", std::move(from), std::move(to), use_alfred);
  }

  // Housing Starts
  Expected<epoch_frame::DataFrame>
  getHousingStarts(const std::string &from, const std::string &to, bool use_alfred = true) const {
    return getSeries("HOUST", from, to, use_alfred);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getHousingStartsAsync(std::string from, std::string to, bool use_alfred = true) const {
    return getSeriesAsync("HOUST", std::move(from), std::move(to), use_alfred);
  }

  // M2 Money Supply
  Expected<epoch_frame::DataFrame>
  getM2MoneySupply(const std::string &from, const std::string &to, bool use_alfred = true) const {
    return getSeries("M2SL", from, to, use_alfred);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getM2MoneySupplyAsync(std::string from, std::string to, bool use_alfred = true) const {
    return getSeriesAsync("M2SL", std::move(from), std::move(to), use_alfred);
  }

  // S&P 500 Index (not available in ALFRED)
  Expected<epoch_frame::DataFrame>
  getSP500(const std::string &from, const std::string &to, bool use_alfred = false) const {
    return getSeries("SP500", from, to, use_alfred);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getSP500Async(std::string from, std::string to, bool use_alfred = false) const {
    return getSeriesAsync("SP500", std::move(from), std::move(to), use_alfred);
  }

  // VIX Volatility Index (not available in ALFRED)
  Expected<epoch_frame::DataFrame>
  getVIX(const std::string &from, const std::string &to, bool use_alfred = false) const {
    return getSeries("VIXCLS", from, to, use_alfred);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getVIXAsync(std::string from, std::string to, bool use_alfred = false) const {
    return getSeriesAsync("VIXCLS", std::move(from), std::move(to), use_alfred);
  }

  // Get series observations for any FRED series ID
  // series_id: FRED series identifier (e.g., "CPIAUCSL", "DFF")
  // from/to: Date range in YYYY-MM-DD format for backtesting period
  // use_alfred: If true, use ALFRED point-in-time data (default: true)
  //   - When true: Returns data as it was published during the backtest period
  //   - When false: Returns current/revised data (not suitable for backtesting)
  //
  // Returns: DataFrame with structure depending on use_alfred:
  //   WITH ALFRED (use_alfred=true, recommended for backtesting):
  //     - Index: "published_at" (when FRED released the data during backtest period)
  //     - Columns: "observation_date" (economic period the data measures), "value"
  //   WITHOUT ALFRED (use_alfred=false, for current data only):
  //     - Index: "date" (observation date)
  //     - Columns: "value"
  Expected<epoch_frame::DataFrame>
  getSeries(const std::string &series_id,
            const std::string &from,
            const std::string &to,
            bool use_alfred = true) const;

  // Async variant of getSeries - returns a coroutine
  // Use co_await to execute, or pass to batch utilities in common/async_batch.hpp
  // Example: auto df = co_await client.getSeriesAsync("CPIAUCSL", from, to, true);
  // NOTE: Parameters are taken by value to avoid coroutine lifetime issues with temporaries
  drogon::Task<Expected<epoch_frame::DataFrame>>
  getSeriesAsync(std::string series_id,
                 std::string from,
                 std::string to,
                 bool use_alfred = true) const;

  // Struct-based async overload for cleaner API and batch operations
  drogon::Task<Expected<epoch_frame::DataFrame>>
  getSeriesAsync(SeriesOptions opts) const {
    return getSeriesAsync(std::move(opts.series_id), std::move(opts.from),
                          std::move(opts.to), opts.use_alfred);
  }

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::fred
