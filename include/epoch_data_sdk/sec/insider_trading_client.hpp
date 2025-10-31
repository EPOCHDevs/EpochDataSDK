#pragma once

#include <string>
#include <optional>
#include <glaze/glaze.hpp>
#include <epoch_frame/dataframe.h>

#include "base_client.hpp"
#include "models.hpp"
#include "options.hpp"
#include "enums.hpp"

namespace data_sdk::sec {

/**
 * @brief Client for SEC Insider Trading API
 *
 * Track insider trading activity from Forms 3, 4, 5, and 144.
 * Forms 3, 4, and 5 disclose transactions made by insiders (officers, directors, 10%+ owners)
 * in their companies' securities.
 *
 * Form 3: Initial beneficial ownership statement
 * Form 4: Stock transaction reports (filed within 2 business days)
 * Form 5: Annual insider trading summary
 * Form 144: Notice of proposed sale of restricted securities
 *
 * Documentation: https://sec-api.io/docs/insider-trading-api
 */
class InsiderTradingClient : public BaseClient {
public:
  explicit InsiderTradingClient(Options options);

  /**
   * @brief Get insider transactions with custom query
   *
   * @param query_json JSON query string following SEC API query syntax
   * @return ListResponse<InsiderTransaction> containing matching transactions
   *
   * Example query:
   * {
   *   "query": "issuerTicker:AAPL AND transactionCode:P",
   *   "from": "0",
   *   "size": "50"
   * }
   */
  drogon::Task<Expected<ListResponse<InsiderTransaction>>>
    getTransactions(const std::string &query_json);

  /**
   * @brief Get insider transactions by ticker
   *
   * @param ticker Stock ticker symbol
   * @param transaction_type Transaction code (optional):
   *                         P = Purchase
   *                         S = Sale
   *                         A = Award/grant
   *                         M = Exercise/conversion
   * @param size Number of results to return (default: 50)
   * @return ListResponse<InsiderTransaction> containing matching transactions
   */
  drogon::Task<Expected<ListResponse<InsiderTransaction>>>
    getTransactionsByTicker(const std::string &ticker,
                           const std::string &transaction_type = "",
                           int size = 50);

  /**
   * @brief Get insider transactions by ticker (enum overload)
   *
   * @param ticker Stock ticker symbol
   * @param transaction_code Transaction code enum
   * @param size Number of results to return (default: 50)
   * @return ListResponse<InsiderTransaction> containing matching transactions
   */
  drogon::Task<Expected<ListResponse<InsiderTransaction>>>
    getTransactionsByTicker(const std::string &ticker,
                           epoch_core::TransactionCode transaction_code,
                           int size = 50) {
    co_return co_await getTransactionsByTicker(ticker, transactionCodeToString(transaction_code), size);
  }

  /**
   * @brief Get insider transactions by issuer CIK
   *
   * @param cik Central Index Key of the issuer
   * @param transaction_type Transaction code (optional)
   * @param size Number of results to return (default: 50)
   * @return ListResponse<InsiderTransaction> containing matching transactions
   */
  drogon::Task<Expected<ListResponse<InsiderTransaction>>>
    getTransactionsByCIK(const std::string &cik,
                        const std::string &transaction_type = "",
                        int size = 50);

  /**
   * @brief Get insider transactions by issuer CIK (enum overload)
   *
   * @param cik Central Index Key of the issuer
   * @param transaction_code Transaction code enum
   * @param size Number of results to return (default: 50)
   * @return ListResponse<InsiderTransaction> containing matching transactions
   */
  drogon::Task<Expected<ListResponse<InsiderTransaction>>>
    getTransactionsByCIK(const std::string &cik,
                        epoch_core::TransactionCode transaction_code,
                        int size = 50) {
    co_return co_await getTransactionsByCIK(cik, transactionCodeToString(transaction_code), size);
  }

  /**
   * @brief Get insider transactions by insider name
   *
   * @param owner_name Name of the insider (officer, director, etc.)
   * @param size Number of results to return (default: 50)
   * @return ListResponse<InsiderTransaction> containing matching transactions
   */
  drogon::Task<Expected<ListResponse<InsiderTransaction>>>
    getTransactionsByInsider(const std::string &owner_name,
                            int size = 50);

  /**
   * @brief Get large insider purchases (for "smart money" strategies)
   *
   * @param ticker Stock ticker symbol (optional, empty for all tickers)
   * @param min_value Minimum transaction value in dollars (default: 100000)
   * @param size Number of results to return (default: 50)
   * @return ListResponse<InsiderTransaction> containing significant buy transactions
   */
  drogon::Task<Expected<ListResponse<InsiderTransaction>>>
    getLargePurchases(const std::string &ticker = "",
                     double min_value = 100000.0,
                     int size = 50);

  // ========== NEW: DataFrame Methods for Systematic Trading ==========

  /**
   * @brief Get insider transactions as DataFrame (systematic trading interface)
   *
   * Returns flat DataFrame with chronological ordering for backtesting.
   * Matches the pattern used by Polygon and FRED clients.
   *
   * @param ticker Stock ticker symbol
   * @param from_date Filing date range start (YYYY-MM-DD)
   * @param to_date Filing date range end (YYYY-MM-DD)
   * @param transaction_code Optional transaction code filter (P, S, A, M, etc.)
   * @return DataFrame with columns: filed_at (index), transaction_date,
   *         owner_name, transaction_code, shares, price, ownership_after
   *
   * Example:
   *   auto df = client.getTransactionsDataFrame("AAPL", "2024-01-01", "2024-12-31");
   */
  Expected<epoch_frame::DataFrame>
    getTransactionsDataFrame(const std::string &ticker,
                            const std::string &from_date,
                            const std::string &to_date,
                            std::optional<epoch_core::TransactionCode> transaction_code = std::nullopt) const;

  /**
   * @brief Get insider transactions as DataFrame (struct-based overload)
   *
   * @param opts InsiderTradingOptions with all filter parameters
   * @return DataFrame with insider transaction data
   */
  Expected<epoch_frame::DataFrame>
    getTransactionsDataFrame(const InsiderTradingOptions &opts) const;

  /**
   * @brief Async variant - get insider transactions as DataFrame
   *
   * Use co_await or pass to batch utilities in common/async_batch.hpp.
   * Parameters taken by value to avoid coroutine lifetime issues.
   *
   * @param ticker Stock ticker symbol
   * @param from_date Filing date range start (YYYY-MM-DD)
   * @param to_date Filing date range end (YYYY-MM-DD)
   * @param transaction_code Optional transaction code filter
   * @return DataFrame with insider transaction data
   */
  drogon::Task<Expected<epoch_frame::DataFrame>>
    getTransactionsDataFrameAsync(std::string ticker,
                                  std::string from_date,
                                  std::string to_date,
                                  std::optional<epoch_core::TransactionCode> transaction_code = std::nullopt) const;

  /**
   * @brief Async variant - get insider transactions as DataFrame (struct-based)
   *
   * @param opts InsiderTradingOptions with all filter parameters
   * @return DataFrame with insider transaction data
   */
  drogon::Task<Expected<epoch_frame::DataFrame>>
    getTransactionsDataFrameAsync(InsiderTradingOptions opts) const;
};

} // namespace data_sdk::sec
