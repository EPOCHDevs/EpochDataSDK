#pragma once

#include <string>
#include <vector>
#include <glaze/glaze.hpp>

namespace data_sdk::sec {

/**
 * @brief Document format file information
 */
struct DocumentFormatFile {
  std::string sequence;
  std::string size;
  std::string documentUrl;
  std::string description;
  std::string type;
};

/**
 * @brief Data file information (XBRL, XML, etc.)
 */
struct DataFile {
  std::string sequence;
  std::string size;
  std::string documentUrl;
  std::string description;
  std::string type;
};

/**
 * @brief Entity information within a filing
 */
struct Entity {
  std::string fiscalYearEnd;
  std::string stateOfIncorporation;
  std::string act;
  std::string cik;
  std::string fileNo;
  std::string irsNo;
  std::string companyName;
  std::string type;
  std::string sic;
  std::string filmNo;
  std::string undefined;  // Sometimes contains industry info
};

/**
 * @brief Total information in query response
 */
struct TotalInfo {
  int value{0};
  std::string relation;
};

/**
 * @brief Query metadata in response
 */
struct QueryInfo {
  int from{0};
  int size{0};
};

/**
 * @brief Represents a single SEC filing result from Query API
 *
 * Complete model matching actual SEC API response structure
 */
struct Filing {
  // Core identification
  std::string id;
  std::string accessionNo;
  std::string cik;
  std::string ticker;

  // Company information
  std::string companyName;
  std::string companyNameLong;
  std::string formType;

  // Filing metadata
  std::string filedAt;
  std::string periodOfReport;
  std::string effectivenessDate;  // For S-1, S-3, S-4, S-8 registration statements
  std::string description;

  // Links
  std::string linkToFilingDetails;
  std::string linkToTxt;
  std::string linkToHtml;
  std::string linkToXbrl;

  // Deprecated field name (for backwards compatibility)
  std::string filingUrl;           // Usually empty, use linkToFilingDetails instead
  std::string documentFormatFilesUrl;  // Usually empty

  // Arrays of nested objects
  std::vector<DocumentFormatFile> documentFormatFiles;
  std::vector<Entity> entities;
  std::vector<std::string> seriesAndClassesContractsInformation;
  std::vector<DataFile> dataFiles;

  // Optional fields that may be present
  std::vector<std::string> items;  // For 8-K item numbers
  std::string sic;
  std::string stateOfIncorporation;
};

/**
 * @brief Query API response
 *
 * Matches actual API structure with nested objects
 */
struct QueryResponse {
  TotalInfo total;
  QueryInfo query;
  std::vector<Filing> filings;
};

/**
 * @brief Mapping API response for CUSIP/CIK/Ticker resolution
 *
 * Complete model with all fields from actual API
 */
struct MappingData {
  // Primary identifiers
  std::string cik;
  std::string ticker;
  std::string name;
  std::string cusip;
  std::string id;

  // Exchange and listing info
  std::string exchange;
  bool isDelisted{false};
  std::string category;

  // Industry classification
  std::string sector;
  std::string industry;
  std::string sic;
  std::string sicSector;
  std::string sicIndustry;

  // Fama-French classification
  std::string famaSector;
  std::string famaIndustry;

  // Geographic and currency
  std::string currency;
  std::string location;
};

/**
 * @brief EDGAR Entity information
 */
struct EdgarEntity {
  std::string cik;
  std::string name;
  std::string ticker;
  std::string exchange;
  std::string sic;
  std::string stateOfIncorporation;
};

/**
 * @brief Insider Trading transaction
 */
struct InsiderTransaction {
  std::string filingUrl;
  std::string filedAt;
  std::string issuerCik;
  std::string issuerName;
  std::string issuerTicker;
  std::string ownerCik;
  std::string ownerName;
  std::string transactionDate;
  std::string transactionCode;
  std::string securityTitle;
  double transactionShares{0.0};
  double transactionPricePerShare{0.0};
  double sharesOwnedFollowingTransaction{0.0};
};

/**
 * @brief Form 13F holding entry
 *
 * IMPORTANT: For backtesting without forward bias, always use filedAt as the
 * as-of date, not periodOfReport. You only knew about these holdings on filedAt.
 */
struct Form13FHolding {
  // Filing metadata (critical for no forward bias)
  std::string filedAt;           // When filed with SEC (ISO 8601) - USE THIS for backtesting
  std::string periodOfReport;    // Quarter end date (YYYY-MM-DD) - historical only
  std::string cik;               // Institution CIK (who filed)

  // Holding details
  std::string nameOfIssuer;
  std::string titleOfClass;
  std::string cusip;
  double value{0.0};
  int shares{0};
  std::string shOrPrn;
  std::string putOrCall;
  std::string investmentDiscretion;
  std::string votingAuthoritySole;
  std::string votingAuthorityShared;
  std::string votingAuthorityNone;
};

/**
 * @brief Generic response for list-based APIs
 *
 * Note: total.relation = "eq" means exact count
 *       total.relation = "gte" means >= 10,000 results (API limit)
 */
template <typename T> struct ListResponse {
  std::vector<T> data;
  TotalInfo total;
};

/**
 * @brief Options for insider trading data requests
 *
 * Follows the systematic trading pattern with date ranges for backtesting.
 * All dates in YYYY-MM-DD format.
 */
struct InsiderTradingOptions {
  std::optional<std::string> ticker;
  std::string from_date;  // Filing date range start (YYYY-MM-DD)
  std::string to_date;    // Filing date range end (YYYY-MM-DD)
  std::optional<std::string> transaction_code;  // P, S, A, M, etc. (can also use enum)
  std::optional<double> min_value;  // Minimum transaction value filter
  std::optional<std::string> owner_name;  // Filter by insider name
  std::optional<int> limit;  // Max results (default: API limit)
  bool is_eod = true;  // Aggregate to daily data (guarantees unique index)
};

/**
 * @brief Options for Form 13F holdings data requests
 *
 * Follows the systematic trading pattern with date ranges for backtesting.
 * All dates in YYYY-MM-DD format. 13F filings are quarterly but filtered by date.
 */
struct Form13FOptions {
  std::optional<std::string> ticker;
  std::optional<std::string> cusip;
  std::string from_date;  // Filing date range start (YYYY-MM-DD)
  std::string to_date;    // Filing date range end (YYYY-MM-DD)
  std::optional<std::string> institution_cik;  // Filter by institution
  std::optional<double> min_value;  // Minimum position value filter
  std::optional<int> limit;  // Max results (default: API limit)
  bool is_eod = true;  // Aggregate to daily data (guarantees unique index)
};

} // namespace data_sdk::sec
