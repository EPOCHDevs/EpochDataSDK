#include "epoch_data_sdk/trading_economics/common.hpp"

#include <algorithm>
#include <cctype>
#include <iomanip>
#include <regex>
#include <sstream>

#include "epoch_data_sdk/trading_economics/enums.hpp"

namespace data_sdk::trading_economics {

// =============================================================================
// ENUM TO STRING CONVERSIONS
// =============================================================================

std::string_view countryToString(epoch_core::Country country) {
  return CountryWrapper::ToString(country);
}

std::string_view frequencyToString(epoch_core::Frequency frequency) {
  return FrequencyWrapper::ToString(frequency);
}

std::string_view importanceToString(epoch_core::Importance importance) {
  using epoch_core::Importance;
  switch (importance) {
  case Importance::Low:
    return "1";
  case Importance::Medium:
    return "2";
  case Importance::High:
    return "3";
  default:
    return "1";
  }
}

std::string_view marketTypeToString(epoch_core::MarketType type) {
  return MarketTypeWrapper::ToString(type);
}

std::string_view bondMaturityToString(epoch_core::BondMaturity maturity) {
  using epoch_core::BondMaturity;
  switch (maturity) {
  case BondMaturity::OneMonth:
    return "1M";
  case BondMaturity::ThreeMonth:
    return "3M";
  case BondMaturity::SixMonth:
    return "6M";
  case BondMaturity::OneYear:
    return "1Y";
  case BondMaturity::TwoYear:
    return "2Y";
  case BondMaturity::ThreeYear:
    return "3Y";
  case BondMaturity::FiveYear:
    return "5Y";
  case BondMaturity::SevenYear:
    return "7Y";
  case BondMaturity::TenYear:
    return "10Y";
  case BondMaturity::TwentyYear:
    return "20Y";
  case BondMaturity::ThirtyYear:
    return "30Y";
  default:
    return "10Y";
  }
}

std::string_view tradeTypeToString(epoch_core::TradeType type) {
  using epoch_core::TradeType;
  switch (type) {
  case TradeType::Import:
    return "import";
  case TradeType::Export:
    return "export";
  default:
    return "import";
  }
}

std::string_view calendarGroupToString(epoch_core::CalendarGroup group) {
  return CalendarGroupWrapper::ToString(group);
}

std::string_view categoryGroupToString(epoch_core::CategoryGroup group) {
  return CategoryGroupWrapper::ToString(group);
}

// =============================================================================
// STRING TO ENUM CONVERSIONS
// =============================================================================

std::optional<epoch_core::Country> stringToCountry(std::string_view str) {
  auto result = CountryWrapper::FromString(std::string(str));
  if (result.has_value()) {
    return result.value();
  }
  return std::nullopt;
}

std::optional<epoch_core::Frequency> stringToFrequency(std::string_view str) {
  auto result = FrequencyWrapper::FromString(std::string(str));
  if (result.has_value()) {
    return result.value();
  }
  return std::nullopt;
}

std::optional<epoch_core::MarketType> stringToMarketType(std::string_view str) {
  auto result = MarketTypeWrapper::FromString(std::string(str));
  if (result.has_value()) {
    return result.value();
  }
  return std::nullopt;
}

// =============================================================================
// UTILITY FUNCTIONS
// =============================================================================

std::string urlEncode(std::string_view str) {
  std::ostringstream escaped;
  escaped.fill('0');
  escaped << std::hex;

  for (char c : str) {
    // Keep alphanumeric and other safe characters
    if (std::isalnum(static_cast<unsigned char>(c)) || c == '-' || c == '_' ||
        c == '.' || c == '~') {
      escaped << c;
    }
    // Space -> '+'
    else if (c == ' ') {
      escaped << '+';
    }
    // Encode other characters
    else {
      escaped << '%' << std::setw(2)
              << int(static_cast<unsigned char>(c)) << std::setw(0);
    }
  }

  return escaped.str();
}

std::string buildQueryString(const std::map<std::string, std::string>& params) {
  if (params.empty()) {
    return {};
  }

  std::ostringstream oss;
  bool first = true;
  for (const auto& [key, value] : params) {
    if (!first) {
      oss << '&';
    }
    oss << urlEncode(key) << '=' << urlEncode(value);
    first = false;
  }
  return oss.str();
}

std::string joinCountries(const std::vector<epoch_core::Country>& countries) {
  if (countries.empty()) {
    return {};
  }

  std::ostringstream oss;
  bool first = true;
  for (size_t i = 0; i < countries.size(); ++i) {
    if (i > 0) {
      oss << ',';
    }
    oss << countryToString(countries[i]);
  }
  return oss.str();
}

std::string joinStrings(const std::vector<std::string>& values) {
  if (values.empty()) {
    return {};
  }

  std::ostringstream oss;
  bool first = true;
  for (const auto& value : values) {
    if (!first) {
      oss << ',';
    }
    oss << value;
    first = false;
  }
  return oss.str();
}

std::vector<std::string> splitString(std::string_view str, char delimiter) {
  std::vector<std::string> result;
  std::string current;

  for (char c : str) {
    if (c == delimiter) {
      if (!current.empty()) {
        result.push_back(current);
        current.clear();
      }
    } else {
      current += c;
    }
  }

  if (!current.empty()) {
    result.push_back(current);
  }

  return result;
}

bool isValidDateFormat(std::string_view date) {
  // Validate YYYY-MM-DD format using regex
  static const std::regex date_regex(R"(^\d{4}-\d{2}-\d{2}$)");
  return std::regex_match(date.begin(), date.end(), date_regex);
}

bool isValidDateRange(std::string_view from_date, std::string_view to_date) {
  if (!isValidDateFormat(from_date) || !isValidDateFormat(to_date)) {
    return false;
  }

  // Simple string comparison works for YYYY-MM-DD format
  return from_date <= to_date;
}

} // namespace data_sdk::trading_economics
