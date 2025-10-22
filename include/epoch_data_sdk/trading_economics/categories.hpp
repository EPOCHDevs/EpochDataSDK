#pragma once

#include <string_view>

namespace data_sdk::trading_economics::categories {

// =============================================================================
// COMMON ECONOMIC INDICATOR CATEGORIES
// Use these constants for type-safe indicator queries
// Organized by domain for easy discovery
// =============================================================================

// =============================================================================
// GDP & GROWTH
// =============================================================================
namespace gdp {
  constexpr std::string_view GDP_GROWTH_RATE = "GDP Growth Rate";
  constexpr std::string_view GDP_ANNUAL_GROWTH_RATE = "GDP Annual Growth Rate";
  constexpr std::string_view GDP_CONSTANT_PRICES = "GDP Constant Prices";
  constexpr std::string_view GDP_CURRENT_PRICES = "GDP Current Prices";
  constexpr std::string_view GDP_PER_CAPITA = "GDP per capita";
  constexpr std::string_view GDP_PER_CAPITA_PPP = "GDP per capita PPP";
  constexpr std::string_view REAL_GDP = "Real GDP";
  constexpr std::string_view NOMINAL_GDP = "Nominal GDP";
  constexpr std::string_view GDP_DEFLATOR = "GDP Deflator";
}

// =============================================================================
// INFLATION & PRICES
// =============================================================================
namespace inflation {
  constexpr std::string_view INFLATION_RATE = "Inflation Rate";
  constexpr std::string_view CPI = "Consumer Price Index CPI";
  constexpr std::string_view CORE_INFLATION_RATE = "Core Inflation Rate";
  constexpr std::string_view CORE_CPI = "Core Consumer Prices";
  constexpr std::string_view CPI_HOUSING_UTILITIES = "CPI Housing Utilities";
  constexpr std::string_view CPI_TRANSPORTATION = "CPI Transportation";
  constexpr std::string_view CPI_FOOD_BEVERAGE = "Food Inflation";
  constexpr std::string_view CPI_ENERGY = "CPI Energy";
  constexpr std::string_view INFLATION_RATE_MOM = "Inflation Rate MoM";
  constexpr std::string_view PPI = "Producer Prices";
  constexpr std::string_view PPI_YOY = "Producer Prices Change";
  constexpr std::string_view WHOLESALE_PRICES = "Wholesale Prices";
  constexpr std::string_view IMPORT_PRICES = "Import Prices";
  constexpr std::string_view EXPORT_PRICES = "Export Prices";
  constexpr std::string_view INFLATION_EXPECTATIONS = "Inflation Expectations";
}

// =============================================================================
// EMPLOYMENT & LABOR
// =============================================================================
namespace employment {
  constexpr std::string_view UNEMPLOYMENT_RATE = "Unemployment Rate";
  constexpr std::string_view LABOR_FORCE_PARTICIPATION = "Labour Force Participation Rate";
  constexpr std::string_view EMPLOYMENT_RATE = "Employment Rate";
  constexpr std::string_view NONFARM_PAYROLLS = "Non Farm Payrolls";
  constexpr std::string_view JOBLESS_CLAIMS = "Initial Jobless Claims";
  constexpr std::string_view CONTINUING_JOBLESS_CLAIMS = "Continuing Jobless Claims";
  constexpr std::string_view YOUTH_UNEMPLOYMENT = "Youth Unemployment Rate";
  constexpr std::string_view AVERAGE_WEEKLY_HOURS = "Average Weekly Hours";
  constexpr std::string_view WAGES = "Wages";
  constexpr std::string_view WAGE_GROWTH = "Wage Growth";
  constexpr std::string_view MINIMUM_WAGES = "Minimum Wages";
  constexpr std::string_view JOB_VACANCIES = "Job Vacancies";
}

// =============================================================================
// INTEREST RATES & MONETARY POLICY
// =============================================================================
namespace interest_rates {
  constexpr std::string_view INTEREST_RATE = "Interest Rate";
  constexpr std::string_view CENTRAL_BANK_RATE = "Central Bank Rate";
  constexpr std::string_view INTERBANK_RATE = "Interbank Rate";
  constexpr std::string_view DEPOSIT_INTEREST_RATE = "Deposit Interest Rate";
  constexpr std::string_view LENDING_RATE = "Lending Rate";
  constexpr std::string_view MONEY_SUPPLY_M0 = "Money Supply M0";
  constexpr std::string_view MONEY_SUPPLY_M1 = "Money Supply M1";
  constexpr std::string_view MONEY_SUPPLY_M2 = "Money Supply M2";
  constexpr std::string_view MONEY_SUPPLY_M3 = "Money Supply M3";
  constexpr std::string_view BANK_LENDING_RATE = "Bank Lending Rate";
  constexpr std::string_view CASH_RESERVE_RATIO = "Cash Reserve Ratio";
}

// =============================================================================
// TRADE & BALANCE OF PAYMENTS
// =============================================================================
namespace trade {
  constexpr std::string_view EXPORTS = "Exports";
  constexpr std::string_view IMPORTS = "Imports";
  constexpr std::string_view TRADE_BALANCE = "Balance of Trade";
  constexpr std::string_view CURRENT_ACCOUNT = "Current Account";
  constexpr std::string_view CURRENT_ACCOUNT_TO_GDP = "Current Account to GDP";
  constexpr std::string_view CAPITAL_FLOWS = "Capital Flows";
  constexpr std::string_view FOREIGN_DIRECT_INVESTMENT = "Foreign Direct Investment";
  constexpr std::string_view TERMS_OF_TRADE = "Terms of Trade";
  constexpr std::string_view EXPORT_PRICES = "Export Prices";
  constexpr std::string_view IMPORT_PRICES = "Import Prices";
}

// =============================================================================
// GOVERNMENT & FISCAL POLICY
// =============================================================================
namespace government {
  constexpr std::string_view GOVERNMENT_BUDGET = "Government Budget";
  constexpr std::string_view GOVERNMENT_BUDGET_VALUE = "Government Budget Value";
  constexpr std::string_view GOVERNMENT_DEBT_TO_GDP = "Government Debt to GDP";
  constexpr std::string_view GOVERNMENT_REVENUES = "Government Revenues";
  constexpr std::string_view GOVERNMENT_SPENDING = "Government Spending";
  constexpr std::string_view FISCAL_EXPENDITURE = "Fiscal Expenditure";
  constexpr std::string_view TAX_REVENUE = "Tax Revenue";
  constexpr std::string_view CORPORATE_TAX_RATE = "Corporate Tax Rate";
  constexpr std::string_view PERSONAL_INCOME_TAX_RATE = "Personal Income Tax Rate";
  constexpr std::string_view SOCIAL_SECURITY_RATE = "Social Security Rate";
}

// =============================================================================
// BUSINESS & MANUFACTURING
// =============================================================================
namespace business {
  constexpr std::string_view BUSINESS_CONFIDENCE = "Business Confidence";
  constexpr std::string_view MANUFACTURING_PMI = "Manufacturing PMI";
  constexpr std::string_view SERVICES_PMI = "Services PMI";
  constexpr std::string_view COMPOSITE_PMI = "Composite PMI";
  constexpr std::string_view INDUSTRIAL_PRODUCTION = "Industrial Production";
  constexpr std::string_view INDUSTRIAL_PRODUCTION_MOM = "Industrial Production Mom";
  constexpr std::string_view MANUFACTURING_PRODUCTION = "Manufacturing Production";
  constexpr std::string_view CAPACITY_UTILIZATION = "Capacity Utilization";
  constexpr std::string_view FACTORY_ORDERS = "Factory orders";
  constexpr std::string_view ZEW_ECONOMIC_SENTIMENT = "ZEW Economic Sentiment Index";
  constexpr std::string_view IFOUSINESS_CLIMATE = "Ifo Business Climate";
}

// =============================================================================
// CONSUMER & RETAIL
// =============================================================================
namespace consumer {
  constexpr std::string_view CONSUMER_CONFIDENCE = "Consumer Confidence";
  constexpr std::string_view RETAIL_SALES_MOM = "Retail Sales MoM";
  constexpr std::string_view RETAIL_SALES_YOY = "Retail Sales YoY";
  constexpr std::string_view CONSUMER_SPENDING = "Consumer Spending";
  constexpr std::string_view PERSONAL_SPENDING = "Personal Spending";
  constexpr std::string_view PERSONAL_INCOME = "Personal Income";
  constexpr std::string_view PERSONAL_SAVINGS = "Personal Savings";
  constexpr std::string_view CONSUMER_CREDIT = "Consumer Credit";
  constexpr std::string_view GASOLINE_PRICES = "Gasoline Prices";
}

// =============================================================================
// HOUSING & CONSTRUCTION
// =============================================================================
namespace housing {
  constexpr std::string_view HOUSING_STARTS = "Housing Starts";
  constexpr std::string_view BUILDING_PERMITS = "Building Permits";
  constexpr std::string_view NEW_HOME_SALES = "New Home Sales";
  constexpr std::string_view EXISTING_HOME_SALES = "Existing Home Sales";
  constexpr std::string_view HOUSING_INDEX = "Housing Index";
  constexpr std::string_view HOME_PRICE_INDEX = "Home Price Index";
  constexpr std::string_view MORTGAGE_RATE = "Mortgage Rate";
  constexpr std::string_view CONSTRUCTION_OUTPUT = "Construction Output";
}

// =============================================================================
// STOCK MARKET INDICATORS
// =============================================================================
namespace markets {
  constexpr std::string_view STOCK_MARKET = "Stock Market";
  constexpr std::string_view GOVERNMENT_BOND_10Y = "Government Bond 10Y";
  constexpr std::string_view GOVERNMENT_BOND_YIELD = "Government Bond Yield";
  constexpr std::string_view CURRENCY = "Currency";
  constexpr std::string_view GOLD_RESERVES = "Gold Reserves";
  constexpr std::string_view FOREIGN_EXCHANGE_RESERVES = "Foreign Exchange Reserves";
  constexpr std::string_view CRUDE_OIL_PRODUCTION = "Crude Oil Production";
}

// =============================================================================
// CALENDAR EVENT NAMES (for CalendarClient)
// =============================================================================
namespace calendar_events {
  // US Major Events
  constexpr std::string_view US_NONFARM_PAYROLLS = "Nonfarm Payrolls";
  constexpr std::string_view US_UNEMPLOYMENT_RATE = "Unemployment Rate";
  constexpr std::string_view US_CPI = "Consumer Price Index";
  constexpr std::string_view US_CORE_CPI = "Core Consumer Price Index";
  constexpr std::string_view US_PPI = "Producer Price Index";
  constexpr std::string_view US_RETAIL_SALES = "Retail Sales";
  constexpr std::string_view US_GDP_QOQ = "GDP Growth Rate QoQ";
  constexpr std::string_view US_GDP_ANNUALIZED = "GDP Growth Annualized";
  constexpr std::string_view US_JOBLESS_CLAIMS = "Initial Jobless Claims";
  constexpr std::string_view US_FED_INTEREST_RATE = "Fed Interest Rate Decision";
  constexpr std::string_view US_FOMC_MINUTES = "FOMC Meeting Minutes";
  constexpr std::string_view US_FOMC_STATEMENT = "FOMC Statement";

  // Eurozone Major Events
  constexpr std::string_view ECB_INTEREST_RATE = "ECB Interest Rate Decision";
  constexpr std::string_view EUROZONE_CPI = "Inflation Rate YoY";
  constexpr std::string_view EUROZONE_GDP = "GDP Growth Rate QoQ";
  constexpr std::string_view EUROZONE_UNEMPLOYMENT = "Unemployment Rate";

  // UK Major Events
  constexpr std::string_view BOE_INTEREST_RATE = "BoE Interest Rate Decision";
  constexpr std::string_view UK_CPI = "Inflation Rate YoY";
  constexpr std::string_view UK_GDP = "GDP Growth Rate";
  constexpr std::string_view UK_UNEMPLOYMENT = "Unemployment Rate";

  // China Major Events
  constexpr std::string_view CHINA_GDP = "GDP Growth Rate YoY";
  constexpr std::string_view CHINA_CPI = "Inflation Rate YoY";
  constexpr std::string_view CHINA_PMI = "Manufacturing PMI";
  constexpr std::string_view CHINA_TRADE_BALANCE = "Trade Balance";
}

} // namespace data_sdk::trading_economics::categories
