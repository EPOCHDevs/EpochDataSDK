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
            ShortInterest, ShortVolume
            // AlternativeData, TickData
            );

CREATE_ENUM(BenchmarkKind, None, SPY, QQQ, AGG);

namespace data_sdk {
using epoch_core::DataCategory;
using epoch_core::DataCategoryWrapper;
using epoch_core::BenchmarkKind;
using epoch_core::BenchmarkKindWrapper;

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
