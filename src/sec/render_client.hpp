#pragma once

#include <string>

#include "base_client.hpp"
#include "options.hpp"

namespace data_sdk::sec {

/**
 * @brief Client for SEC Filing Download/Render API
 *
 * Download raw SEC filings and exhibits from SEC EDGAR archives.
 *
 * Documentation: https://sec-api.io/docs/sec-filings-render-api
 */
class RenderClient : public BaseClient {
public:
  explicit RenderClient(Options options);

  /**
   * @brief Download a filing or exhibit by URL
   *
   * @param filing_url Full SEC EDGAR URL to the filing
   * @return Raw filing content (HTML, XML, etc.)
   *
   * Example URL:
   * "https://www.sec.gov/Archives/edgar/data/320193/000032019323000006/aapl-20221231.htm"
   */
  drogon::Task<Expected<std::string>>
  getFiling(const std::string &filing_url);

  /**
   * @brief Download a file by URL (alias for getFiling)
   */
  drogon::Task<Expected<std::string>>
  getFile(const std::string &file_url);
};

} // namespace data_sdk::sec
