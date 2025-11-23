#pragma once

#include <string>
#include <unordered_map>
#include <epoch_data_sdk/common/enums.hpp>

namespace data_sdk::fred {

// Maps CrossSectionalDataCategory to FRED series IDs
inline const std::unordered_map<CrossSectionalDataCategory, std::string>&
getCrossSectionalSeriesMap() {
  static const std::unordered_map<CrossSectionalDataCategory, std::string> map = {
    // Inflation Indicators
    {CrossSectionalDataCategory::CPI, "CPIAUCSL"},
    {CrossSectionalDataCategory::CoreCPI, "CPILFESL"},
    {CrossSectionalDataCategory::PCE, "PCEPI"},
    {CrossSectionalDataCategory::CorePCE, "PCEPILFE"},

    // Interest Rates & Monetary Policy
    {CrossSectionalDataCategory::FedFunds, "DFF"},
    {CrossSectionalDataCategory::Treasury3M, "DTB3"},
    {CrossSectionalDataCategory::Treasury2Y, "DGS2"},
    {CrossSectionalDataCategory::Treasury5Y, "DGS5"},
    {CrossSectionalDataCategory::Treasury10Y, "DGS10"},
    {CrossSectionalDataCategory::Treasury30Y, "DGS30"},

    // Employment & Labor Market
    {CrossSectionalDataCategory::Unemployment, "UNRATE"},
    {CrossSectionalDataCategory::NonfarmPayrolls, "PAYEMS"},
    {CrossSectionalDataCategory::InitialClaims, "ICSA"},

    // Economic Growth & Production
    {CrossSectionalDataCategory::GDP, "GDPC1"},
    {CrossSectionalDataCategory::IndustrialProduction, "INDPRO"},
    {CrossSectionalDataCategory::RetailSales, "RSXFS"},
    {CrossSectionalDataCategory::HousingStarts, "HOUST"},

    // Market Sentiment & Money Supply
    {CrossSectionalDataCategory::ConsumerSentiment, "UMCSENT"},
    {CrossSectionalDataCategory::M2, "M2SL"},
    {CrossSectionalDataCategory::SP500, "SP500"},
    {CrossSectionalDataCategory::VIX, "VIXCLS"}
  };
  return map;
}

// Get FRED series ID for a given cross-sectional category
inline std::string getSeriesId(CrossSectionalDataCategory category) {
  const auto& map = getCrossSectionalSeriesMap();
  auto it = map.find(category);
  if (it == map.end()) {
    throw std::runtime_error("Unknown CrossSectionalDataCategory: " +
                           CrossSectionalDataCategoryWrapper::ToString(category));
  }
  return it->second;
}

// Get human-readable name for category
inline std::string getCategoryName(CrossSectionalDataCategory category) {
  return CrossSectionalDataCategoryWrapper::ToString(category);
}

} // namespace data_sdk::fred
