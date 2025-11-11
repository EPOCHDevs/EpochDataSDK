#pragma once

#include <string>
#include <optional>
#include <glaze/glaze.hpp>
#include <epoch_frame/dataframe.h>

#include "base_client.hpp"
#include "models.hpp"
#include "options.hpp"

namespace data_sdk::sec {

/**
 * @brief Client for SEC Form 13F Institutional Holdings API
 *
 * Track institutional holdings reported by investment managers with $100M+ AUM.
 * Form 13F-HR is filed quarterly (45 days after quarter end) and discloses
 * long positions in US equities and convertible debt.
 *
 * Use cases:
 * - Follow-the-smart-money strategies
 * - Track hedge fund/institutional portfolio changes
 * - Identify concentrated ownership positions
 * - Analyze sector/stock crowding
 *
 * Documentation: https://sec-api.io/docs/form-13-f-filings-institutional-holdings-api
 */
class Form13FClient : public BaseClient {
public:
  explicit Form13FClient(Options options);

  /**
   * @brief Get 13F holdings with custom query
   *
   * @param query_json JSON query string following SEC API query syntax
   * @return ListResponse<Form13FHolding> containing matching holdings
   *
   * Example query:
   * {
   *   "query": "cusip:037833100",
   *   "from": "0",
   *   "size": "100"
   * }
   */
  drogon::Task<Expected<ListResponse<Form13FHolding>>>
    getHoldings(const std::string &query_json);

  /**
   * @brief Get institutions holding a specific ticker
   *
   * @param ticker Stock ticker symbol
   * @param size Number of results to return (default: 100)
   * @return ListResponse<Form13FHolding> containing institutional holders
   */
  drogon::Task<Expected<ListResponse<Form13FHolding>>>
    getHoldersByTicker(const std::string &ticker, int size = 100);

  /**
   * @brief Get institutions holding a specific CUSIP
   *
   * @param cusip CUSIP identifier
   * @param size Number of results to return (default: 100)
   * @return ListResponse<Form13FHolding> containing institutional holders
   */
  drogon::Task<Expected<ListResponse<Form13FHolding>>>
    getHoldersByCUSIP(const std::string &cusip, int size = 100);

  /**
   * @brief Get portfolio holdings for a specific institution
   *
   * @param institution_cik CIK of the investment manager/institution
   * @param size Number of results to return (default: 100)
   * @return ListResponse<Form13FHolding> containing the institution's holdings
   *
   * Example CIKs:
   * - 1067983: Berkshire Hathaway
   * - 1324404: Citadel Advisors
   * - 1649339: Tiger Global Management
   */
  drogon::Task<Expected<ListResponse<Form13FHolding>>>
    getHoldingsByInstitution(const std::string &institution_cik, int size = 100);

  /**
   * @brief Get largest positions (by value) for a ticker
   *
   * @param ticker Stock ticker symbol
   * @param min_value Minimum position value in dollars (default: 10M)
   * @param size Number of results to return (default: 50)
   * @return ListResponse<Form13FHolding> containing large institutional positions
   */
  drogon::Task<Expected<ListResponse<Form13FHolding>>>
    getLargePositions(const std::string &ticker,
                     double min_value = 10000000.0,
                     int size = 50);

  // ========== NEW: DataFrame Methods for Systematic Trading ==========

  /**
   * @brief Get 13F holdings as DataFrame (systematic trading interface)
   *
   * Returns flat DataFrame with chronological ordering for backtesting.
   * Matches the pattern used by Polygon and FRED clients.
   * 13F filings are quarterly (Q1-Q4) but filtered by filing date range.
   *
   * @param ticker Stock ticker symbol (or use cusip in opts)
   * @param from_date Filing date range start (YYYY-MM-DD)
   * @param to_date Filing date range end (YYYY-MM-DD)
   * @param is_eod If true (default), aggregate to daily data with date index (guarantees uniqueness).
   *               If false, keep second-level timestamps (may have duplicate timestamps).
   * @return DataFrame with columns: shares, value, security_type,
   *         investment_discretion (filed_at index)
   *
   * Example:
   *   // Get all AAPL institutional holdings for 2024
   *   auto df = client.getHoldingsDataFrame("AAPL", "2024-01-01", "2024-12-31");
   */
  Expected<epoch_frame::DataFrame>
    getHoldingsDataFrame(const std::string &ticker,
                        const std::string &from_date,
                        const std::string &to_date,
                        bool is_eod = true) const;

  /**
   * @brief Get 13F holdings as DataFrame (struct-based overload)
   *
   * @param opts Form13FOptions with all filter parameters
   * @return DataFrame with institutional holdings data
   */
  Expected<epoch_frame::DataFrame>
    getHoldingsDataFrame(const Form13FOptions &opts) const;

  /**
   * @brief Async variant - get 13F holdings as DataFrame
   *
   * Use co_await or pass to batch utilities in common/async_batch.hpp.
   * Parameters taken by value to avoid coroutine lifetime issues.
   *
   * @param ticker Stock ticker symbol
   * @param from_date Filing date range start (YYYY-MM-DD)
   * @param to_date Filing date range end (YYYY-MM-DD)
   * @param is_eod If true (default), aggregate to daily data
   * @return DataFrame with institutional holdings data
   */
  drogon::Task<Expected<epoch_frame::DataFrame>>
    getHoldingsDataFrameAsync(std::string ticker,
                              std::string from_date,
                              std::string to_date,
                              bool is_eod = true) const;

  /**
   * @brief Async variant - get 13F holdings as DataFrame (struct-based)
   *
   * @param opts Form13FOptions with all filter parameters
   * @return DataFrame with institutional holdings data
   */
  drogon::Task<Expected<epoch_frame::DataFrame>>
    getHoldingsDataFrameAsync(Form13FOptions opts) const;
};

} // namespace data_sdk::sec
