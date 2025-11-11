#pragma once

#include <string>
#include <string_view>
#include <vector>
#include <map>

#include "enums.hpp"

namespace data_sdk::trading_economics {

// =============================================================================
// ENUM TO STRING CONVERSIONS
// Thread-safe, compile-time constant strings using string_view
// =============================================================================

// Country enum to API string
std::string_view countryToString(epoch_core::Country country);

// Frequency enum to API string
std::string_view frequencyToString(epoch_core::Frequency frequency);

// Importance enum to API string ("1", "2", "3")
std::string_view importanceToString(epoch_core::Importance importance);

// MarketType enum to API string
std::string_view marketTypeToString(epoch_core::MarketType type);

// BondMaturity enum to API string ("2Y", "10Y", etc.)
std::string_view bondMaturityToString(epoch_core::BondMaturity maturity);

// TradeType enum to API string ("import", "export")
std::string_view tradeTypeToString(epoch_core::TradeType type);

// CalendarGroup enum to API string
std::string_view calendarGroupToString(epoch_core::CalendarGroup group);

// CategoryGroup enum to API string
std::string_view categoryGroupToString(epoch_core::CategoryGroup group);

// =============================================================================
// STRING TO ENUM CONVERSIONS (for parsing API responses)
// =============================================================================

// Parse country name from API response
// Returns std::nullopt if not recognized
std::optional<epoch_core::Country> stringToCountry(std::string_view str);

// Parse frequency from API response
std::optional<epoch_core::Frequency> stringToFrequency(std::string_view str);

// Parse market type from API response
std::optional<epoch_core::MarketType> stringToMarketType(std::string_view str);

// =============================================================================
// UTILITY FUNCTIONS
// =============================================================================

// Build query parameter string from map
// Example: {"country": "United States", "indicator": "GDP"}
//       -> "country=United%20States&indicator=GDP"
std::string buildQueryString(const std::map<std::string, std::string>& params);

// URL-encode a string for use in query parameters
std::string urlEncode(std::string_view str);

// Join multiple values with comma (for multi-country, multi-indicator queries)
// Example: {Country::UnitedStates, Country::China} -> "United States,China"
std::string joinCountries(const std::vector<epoch_core::Country>& countries);
std::string joinStrings(const std::vector<std::string>& values);

// Split comma-separated string into vector
std::vector<std::string> splitString(std::string_view str, char delimiter = ',');

// Date validation helpers
bool isValidDateFormat(std::string_view date); // Validates YYYY-MM-DD format
bool isValidDateRange(std::string_view from_date, std::string_view to_date);

} // namespace data_sdk::trading_economics
