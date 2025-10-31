#pragma once

#include <string>

#include "base_client.hpp"
#include "options.hpp"

namespace data_sdk::sec {

/**
 * @brief Client for SEC Filing Section Extractor API
 *
 * Extract specific sections/items from 10-K, 10-Q, and 8-K filings.
 *
 * Documentation: https://sec-api.io/docs/sec-filings-item-extraction-api
 */
class ExtractorClient : public BaseClient {
public:
  explicit ExtractorClient(Options options);

  /**
   * @brief Extract a section from a filing
   *
   * @param filing_url URL to the SEC filing
   * @param section Section identifier (e.g., "1A", "7", "1.01")
   * @param return_type "text" or "html" (default: "text")
   * @return Extracted section content
   *
   * Common sections:
   * - 10-K: "1A" (Risk Factors), "7" (MD&A), "8" (Financial Statements)
   * - 10-Q: "part1item1" (Financial Statements), "part1item2" (MD&A)
   * - 8-K: "1.01", "1.02", etc. (various event items)
   */
  drogon::Task<Expected<std::string>>
  getSection(const std::string &filing_url,
            const std::string &section,
            const std::string &return_type = "text");
};

} // namespace data_sdk::sec
