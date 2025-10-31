#pragma once

#include <string>
#include <vector>
#include <glaze/glaze.hpp>

#include "base_client.hpp"
#include "models.hpp"
#include "options.hpp"

namespace data_sdk::sec {

/**
 * @brief Client for SEC CUSIP/CIK/Ticker Mapping API
 *
 * Convert between different company identifiers (CUSIP, CIK, Ticker)
 * and retrieve company metadata.
 *
 * Documentation: https://sec-api.io/docs/mapping-api
 */
class MappingClient : public BaseClient {
public:
  explicit MappingClient(Options options);

  /**
   * @brief Resolve company information by ticker symbol
   *
   * @param ticker Stock ticker symbol (e.g., "AAPL")
   * @return MappingData with CIK, CUSIP, and other company info
   */
  drogon::Task<Expected<MappingData>> resolveByTicker(const std::string &ticker);

  /**
   * @brief Resolve company information by CIK
   *
   * @param cik Central Index Key
   * @return MappingData with ticker, CUSIP, and other company info
   */
  drogon::Task<Expected<MappingData>> resolveByCIK(const std::string &cik);

  /**
   * @brief Resolve company information by CUSIP
   *
   * @param cusip CUSIP identifier
   * @return MappingData with ticker, CIK, and other company info
   */
  drogon::Task<Expected<MappingData>> resolveByCUSIP(const std::string &cusip);

  /**
   * @brief Search companies by name
   *
   * @param name Company name (partial match)
   * @return List of matching companies
   */
  drogon::Task<Expected<std::vector<MappingData>>>
  resolveByName(const std::string &name);

private:
  /**
   * @brief Internal method to resolve by any parameter
   */
  drogon::Task<Expected<MappingData>>
  resolve(const std::string &parameter, const std::string &value);
};

} // namespace data_sdk::sec
