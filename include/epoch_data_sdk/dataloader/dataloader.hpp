#pragma once
#include <epoch_data_sdk/common/enums.hpp>
#include <epoch_data_sdk/dataloader/fetch_kwargs.hpp>
#include <epoch_data_sdk/events/all.h>
#include <epoch_frame/dataframe.h>
#include <epoch_frame/series.h>
#include <epoch_data_sdk/model/asset/asset.hpp>
#include <drogon/drogon.h>
#include <memory>
#include <optional>

namespace data_sdk {

// Forward declarations
namespace dataloader {
  struct IDataFetcher;
}

// Main DataLoader interface - what users interact with
class IDataLoader {
public:
  using DataMap = asset::AssetHashMap<epoch_frame::DataFrame>;
  using Ptr = std::unique_ptr<IDataLoader>;

  virtual ~IDataLoader() = default;

  // Load data based on configured options
  virtual void LoadData(events::ScopedProgressEmitter& emitter) = 0;

  // Get loaded data (asset -> DataFrame map)
  // Note: If EconomicIndicator/Indices requests are specified, they will be
  // merged as columns into each asset's DataFrame
  virtual DataMap GetStoredData() const = 0;

  // Query methods
  virtual DataCategory GetDataCategory() const = 0;  // Returns primary category
  virtual asset::AssetHashSet GetStrategyAssets() const = 0;
  virtual asset::AssetHashSet GetAssets() const = 0;
  virtual std::optional<epoch_frame::Series> GetBenchmark() const = 0;

  // Advanced: load specific asset/category on-demand (sync)
  virtual std::expected<epoch_frame::DataFrame, std::string>
  LoadAssetBars(const asset::Asset& asset,
                DataCategory category,
                const dataloader::FetchKwargs& kwargs = dataloader::NoKwargs{}) const = 0;

  // Advanced: load specific asset/category on-demand (async)
  virtual drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
  LoadAssetBarsAsync(const asset::Asset& asset,
                     DataCategory category,
                     dataloader::FetchKwargs kwargs = dataloader::NoKwargs{}) const = 0;

  // Load economic indicator data (FRED series)
  // indicator: Economic indicator enum (e.g., CrossSectionalDataCategory::CPI)
  // use_alfred: true = point-in-time data (for backtesting), false = latest values
  virtual std::expected<epoch_frame::DataFrame, std::string>
  LoadEconomicIndicator(CrossSectionalDataCategory indicator,
                        const epoch_frame::Date& fromDate,
                        const epoch_frame::Date& toDate,
                        bool use_alfred = true) const = 0;

  virtual drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
  LoadEconomicIndicatorAsync(CrossSectionalDataCategory indicator,
                             const epoch_frame::Date& fromDate,
                             const epoch_frame::Date& toDate,
                             bool use_alfred = true) const = 0;

  // Load market index data (Polygon indices like SPX, VIX, NDX)
  // ticker: Index ticker (without "I:" prefix)
  // is_eod: true = daily bars, false = minute bars
  virtual std::expected<epoch_frame::DataFrame, std::string>
  LoadIndexData(const std::string& ticker,
                const epoch_frame::Date& fromDate,
                const epoch_frame::Date& toDate,
                bool is_eod = true) const = 0;

  virtual drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
  LoadIndexDataAsync(const std::string& ticker,
                     const epoch_frame::Date& fromDate,
                     const epoch_frame::Date& toDate,
                     bool is_eod = true) const = 0;

  // ============================================================
  // Backward compatibility aliases (deprecated)
  // ============================================================

  // Alias for LoadIndexData (deprecated, use LoadIndexData)
  std::expected<epoch_frame::DataFrame, std::string>
  LoadIndicesData(const std::string& ticker,
                  const epoch_frame::Date& fromDate,
                  const epoch_frame::Date& toDate,
                  bool is_eod = true) const {
    return LoadIndexData(ticker, fromDate, toDate, is_eod);
  }

  // Alias for LoadIndexDataAsync (deprecated, use LoadIndexDataAsync)
  drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
  LoadIndicesDataAsync(const std::string& ticker,
                       const epoch_frame::Date& fromDate,
                       const epoch_frame::Date& toDate,
                       bool is_eod = true) const {
    return LoadIndexDataAsync(ticker, fromDate, toDate, is_eod);
  }
};

} // namespace data_sdk
