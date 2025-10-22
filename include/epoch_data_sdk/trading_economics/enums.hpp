#pragma once

#include <epoch_core/enum_wrapper.h>

namespace data_sdk::trading_economics {

// =============================================================================
// AUTOMATIC ENUM REFLECTION using CREATE_ENUM
// Usage: CountryWrapper::ToString(Country::UnitedStates) -> "UnitedStates"
//        CountryWrapper::FromString("UnitedStates") -> Country::UnitedStates
// =============================================================================

// Country - Major economies (G20 + key markets)
// Note: Use string overload for full 196-country support
CREATE_ENUM(Country,
  // G7
  UnitedStates, Canada, UnitedKingdom, Germany, France, Italy, Japan,
  // G20 Additional
  Argentina, Australia, Brazil, China, India, Indonesia, Mexico, Russia,
  SaudiArabia, SouthAfrica, SouthKorea, Turkey,
  // EU Major
  Spain, Netherlands, Belgium, Austria, Sweden, Norway, Denmark, Finland,
  Ireland, Portugal, Greece, Poland,
  // Asia-Pacific
  Singapore, HongKong, Taiwan, Thailand, Malaysia, Philippines, Vietnam, NewZealand,
  // Middle East
  UnitedArabEmirates, Qatar, Kuwait, Israel, Egypt,
  // Latin America
  Chile, Colombia, Peru,
  // Africa
  Nigeria, Kenya,
  // Other
  Switzerland, EuroArea
);

// Frequency - Data reporting frequency
CREATE_ENUM(Frequency, Daily, Weekly, Monthly, Quarterly, Yearly, Continuous);

// Importance - Calendar event importance (1=Low, 2=Medium, 3=High)
CREATE_ENUM(Importance, Low = 1, Medium = 2, High = 3);

// MarketType - Asset class categories
CREATE_ENUM(MarketType, Commodities, Currency, Index, Bond, Crypto);

// BondMaturity - Standard treasury/bond maturities
CREATE_ENUM(BondMaturity,
  OneMonth, ThreeMonth, SixMonth, OneYear, TwoYear, ThreeYear,
  FiveYear, SevenYear, TenYear, TwentyYear, ThirtyYear
);

// TradeType - Import/Export classification
CREATE_ENUM(TradeType, Import, Export);

// CalendarGroup - Economic calendar event groups
CREATE_ENUM(CalendarGroup,
  Bonds, Inflation, GDP, Employment, Manufacturing, Services,
  Housing, Consumer, Trade, Government
);

// CategoryGroup - Indicator category groupings
CREATE_ENUM(CategoryGroup,
  GDP, Labour, Prices, Money, Trade, Government, Business,
  Consumer, Housing, Taxes, Markets
);

// OutputFormat - API response format
CREATE_ENUM(OutputFormat, Json, DataFrame, Raw);

} // namespace data_sdk::trading_economics
