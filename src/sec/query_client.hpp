#pragma once

#include <string>
#include <glaze/glaze.hpp>

#include "base_client.hpp"
#include "models.hpp"
#include "options.hpp"
#include "enums.hpp"

namespace data_sdk::sec {

/**
 * @brief Client for SEC EDGAR Query API
 *
 * Search and filter all 18+ million SEC EDGAR filings using various parameters
 * like ticker, CIK, form type, filing date, etc.
 *
 * Documentation: https://sec-api.io/docs/query-api
 */
class QueryClient : public BaseClient {
public:
  explicit QueryClient(Options options);

  /**
   * @brief Search filings using a JSON query string
   *
   * @param query_json JSON query string following SEC API query syntax
   * @return QueryResponse containing matching filings
   *
   * Example query:
   * {
   *   "query": { "query_string": { "query": "ticker:TSLA AND formType:\"10-K\"" } },
   *   "from": "0",
   *   "size": "10",
   *   "sort": [{ "filedAt": { "order": "desc" } }]
   * }
   */
  drogon::Task<Expected<QueryResponse>> getFilings(const std::string &query_json);

  /**
   * @brief Helper: Search filings by ticker and form type (string)
   *
   * @param ticker Stock ticker symbol
   * @param form_type SEC form type (e.g., "10-K", "10-Q", "8-K")
   * @param size Number of results to return (default: 10)
   * @return QueryResponse containing matching filings
   */
  drogon::Task<Expected<QueryResponse>> getFilingsByTicker(
      const std::string &ticker,
      const std::string &form_type = "",
      int size = 10);

  /**
   * @brief Helper: Search filings by ticker and form type (enum)
   *
   * @param ticker Stock ticker symbol
   * @param form_type SEC form type enum (e.g., epoch_core::FormType::TenK)
   * @param size Number of results to return (default: 10)
   * @return QueryResponse containing matching filings
   */
  drogon::Task<Expected<QueryResponse>> getFilingsByTicker(
      const std::string &ticker,
      epoch_core::FormType form_type,
      int size = 10);

  /**
   * @brief Helper: Search filings by CIK (string)
   *
   * @param cik Central Index Key (CIK) of the company
   * @param form_type SEC form type (optional)
   * @param size Number of results to return (default: 10)
   * @return QueryResponse containing matching filings
   */
  drogon::Task<Expected<QueryResponse>> getFilingsByCIK(
      const std::string &cik,
      const std::string &form_type = "",
      int size = 10);

  /**
   * @brief Helper: Search filings by CIK (enum)
   *
   * @param cik Central Index Key (CIK) of the company
   * @param form_type SEC form type enum
   * @param size Number of results to return (default: 10)
   * @return QueryResponse containing matching filings
   */
  drogon::Task<Expected<QueryResponse>> getFilingsByCIK(
      const std::string &cik,
      epoch_core::FormType form_type,
      int size = 10);
};

} // namespace data_sdk::sec
