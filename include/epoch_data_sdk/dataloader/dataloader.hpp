#pragma once
#include <epoch_data_sdk/common/enums.hpp>
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
  virtual void LoadData() = 0;

  // Get loaded data (asset -> DataFrame map)
  // Note: If crossSectionalCategories are specified in options, they will be
  // merged as columns into each asset's DataFrame
  virtual DataMap GetStoredData() const = 0;

  // Query methods
  virtual DataCategory GetDataCategory() const = 0;
  virtual asset::AssetHashSet GetStrategyAssets() const = 0;
  virtual asset::AssetHashSet GetAssets() const = 0;
  virtual std::optional<epoch_frame::Series> GetBenchmark() const = 0;

  // Advanced: load specific asset/category on-demand (sync)
  virtual std::expected<epoch_frame::DataFrame, std::string>
  LoadAssetBars(const asset::Asset& asset,
                DataCategory category,
                const std::unordered_map<std::string, std::string>& parameters = {}) const = 0;

  // Advanced: load specific asset/category on-demand (async)
  virtual drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
  LoadAssetBarsAsync(const asset::Asset& asset,
                     DataCategory category,
                     std::unordered_map<std::string, std::string> parameters = {}) const = 0;

  // Cross-sectional economic data methods (FRED indicators)
  // Fetch cross-sectional data without asset - category determines the series
  virtual std::expected<epoch_frame::DataFrame, std::string>
  LoadCrossSectionalData(CrossSectionalDataCategory category,
                         const epoch_frame::Date& fromDate,
                         const epoch_frame::Date& toDate) const = 0;

  virtual drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
  LoadCrossSectionalDataAsync(CrossSectionalDataCategory category,
                              const epoch_frame::Date& fromDate,
                              const epoch_frame::Date& toDate) const = 0;
};

} // namespace data_sdk
