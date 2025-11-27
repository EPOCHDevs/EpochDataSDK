#pragma once
#include <epoch_core/macros.h>
#include <epoch_core/enum_wrapper.h>
#include <filesystem>

// CREATE_ENUM places enums in epoch_core namespace
CREATE_ENUM(DataCategory,
            // Time series data
            MinuteBars, DailyBars,
            // Corporate actions
            News, Dividends, Splits, TickerEvents,
            // Fundamentals
            BalanceSheets, CashFlowStatements, IncomeStatements, Ratios,
            ShortInterest, ShortVolume,
            // Cross-sectional / Market-wide data
            EconomicIndicator,  // FRED data via series_id
            ReferenceAgg        // Reference aggregates (Stocks, FX, Crypto, Indices)
            );

CREATE_ENUM(BenchmarkKind, None, SPY, QQQ, AGG);

// Dividend types from Polygon API
// CD = Cash Dividend, SC = Stock Dividend (special cash), LT = Long-Term Capital Gain, ST = Short-Term Capital Gain
CREATE_ENUM(DividendType, CD, SC, LT, ST);

// Balance sheet timeframes (quarterly, annual only)
CREATE_ENUM(BalanceSheetTimeframe, quarterly, annual);

// Income statement and cash flow timeframes (includes TTM)
CREATE_ENUM(FinancialsTimeframe, quarterly, annual, trailing_twelve_months);

// Cross-sectional economic indicator categories (FRED data)
CREATE_ENUM(CrossSectionalDataCategory,
            // Inflation Indicators
            CPI, CoreCPI, PCE, CorePCE,
            // Interest Rates & Monetary Policy
            FedFunds, Treasury3M, Treasury2Y, Treasury5Y, Treasury10Y, Treasury30Y,
            // Employment & Labor Market
            Unemployment, NonfarmPayrolls, InitialClaims,
            // Economic Growth & Production
            GDP, IndustrialProduction, RetailSales, HousingStarts,
            // Market Sentiment & Money Supply
            ConsumerSentiment, M2, SP500, VIX
            );

namespace data_sdk {
using epoch_core::DataCategory;
using epoch_core::DataCategoryWrapper;
using epoch_core::BenchmarkKind;
using epoch_core::BenchmarkKindWrapper;
using epoch_core::DividendType;
using epoch_core::DividendTypeWrapper;
using epoch_core::BalanceSheetTimeframe;
using epoch_core::BalanceSheetTimeframeWrapper;
using epoch_core::FinancialsTimeframe;
using epoch_core::FinancialsTimeframeWrapper;
using epoch_core::CrossSectionalDataCategory;
using epoch_core::CrossSectionalDataCategoryWrapper;

// Data providers
enum class DataProvider {
  Polygon,
  FRED,
  SEC,
  TradingEconomics,
  EpochArchive  // Epoch's maintained S3 archive
};

// Helper functions for DataCategory classification
inline bool IsTimeSeriesCategory(DataCategory cat) {
  return cat == DataCategory::MinuteBars || cat == DataCategory::DailyBars;
}

inline bool IsIntraday(DataCategory cat) {
  return cat == DataCategory::MinuteBars;
}

inline bool IsDaily(DataCategory cat) {
  return cat == DataCategory::DailyBars;
}

inline bool IsAuxiliaryCategory(DataCategory cat) {
  return !IsTimeSeriesCategory(cat);
}

// Filesystem path operator for DataCategory
inline std::filesystem::path operator/(std::filesystem::path const &os,
                                       DataCategory const &type) {
  return os / DataCategoryWrapper::ToString(type);
}

} // namespace data_sdk
