#pragma once
#include <epoch_data_sdk/model/builder/asset_builder.hpp>

namespace data_sdk::asset {

/**
 * Common asset constants for testing and benchmarking
 * These are lazily initialized when first accessed
 */
struct AssetConstants {
  static const AssetConstants& instance() {
    static AssetConstants instance;
    return instance;
  }

  const Asset SPY{MakeAsset("SPY", epoch_core::AssetClass::Stocks)};
  const Asset AAPL{MakeAsset("AAPL", epoch_core::AssetClass::Stocks)};
  const Asset MSFT{MakeAsset("MSFT", epoch_core::AssetClass::Stocks)};
  const Asset TSLA{MakeAsset("TSLA", epoch_core::AssetClass::Stocks)};
  const Asset QQQ{MakeAsset("QQQ", epoch_core::AssetClass::Stocks)};
  const Asset AGG{MakeAsset("AGG", epoch_core::AssetClass::Stocks)};
};

} // namespace data_sdk::asset
