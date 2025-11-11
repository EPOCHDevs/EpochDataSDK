#pragma once
#include <epoch_data_sdk/model/builder/asset_builder.hpp>
#include <epoch_data_sdk/model/asset/asset_specification.hpp>
#include <epoch_data_sdk/common/symbol.hpp>

namespace data_sdk::asset {

/**
 * Common asset constants for testing and benchmarking
 * These are lazily initialized when first accessed
 */
struct AssetConstants {
  static const AssetConstants &instance() {
    static AssetConstants instance;
    return instance;
  }

  const Asset FAKEPACA{AssetSpecification{
      {.id = "FAKEPACA-Stocks",
       .name = "FAKEPACA",
       .ticker = data_sdk::Symbol{"FAKEPACA"},
       .asset_class = epoch_core::AssetClass::Stocks,
       .exchange = epoch_core::Exchange::NYSE}}};
  const Asset AAPL{MakeAsset("AAPL", epoch_core::AssetClass::Stocks)};
  const Asset MSFT{MakeAsset("MSFT", epoch_core::AssetClass::Stocks)};
  const Asset TSLA{MakeAsset("TSLA", epoch_core::AssetClass::Stocks)};
  const Asset IBM{MakeAsset("IBM", epoch_core::AssetClass::Stocks)};
  const Asset AMZN{MakeAsset("AMZN", epoch_core::AssetClass::Stocks)};
  const Asset GOOG{MakeAsset("GOOG", epoch_core::AssetClass::Stocks)};
  const Asset GOOGL{MakeAsset("GOOGL", epoch_core::AssetClass::Stocks)};
  const Asset AA{MakeAsset("AA", epoch_core::AssetClass::Stocks)};
  const Asset SPY{MakeAsset("SPY", epoch_core::AssetClass::Stocks)};
  const Asset QQQ{MakeAsset("QQQ", epoch_core::AssetClass::Stocks)};
  const Asset AGG{MakeAsset("AGG", epoch_core::AssetClass::Stocks)};
  const Asset SPX{MakeAsset("^SPX", epoch_core::AssetClass::Indices)};
  const Asset DOGE_USD{
      MakeAsset("^DOGEUSD", epoch_core::AssetClass::Crypto, epoch_core::Exchange::COINBASE)};
  const Asset BTC_USD{
      MakeAsset("^BTCUSD", epoch_core::AssetClass::Crypto, epoch_core::Exchange::COINBASE)};
  const Asset ETH_BTC{
      MakeAsset("^ETHBTC", epoch_core::AssetClass::Crypto, epoch_core::Exchange::COINBASE)};
  const Asset ETH_USD{
      MakeAsset("^ETHUSD", epoch_core::AssetClass::Crypto, epoch_core::Exchange::COINBASE)};
  const Asset LTC_USD{
      MakeAsset("^LTCUSD", epoch_core::AssetClass::Crypto, epoch_core::Exchange::COINBASE)};
  const Asset EUR_USD{
      MakeAsset("^EURUSD", epoch_core::AssetClass::FX, epoch_core::Exchange::FX)};
  const Asset EUR_GBP{
      MakeAsset("^EURGBP", epoch_core::AssetClass::FX, epoch_core::Exchange::FX)};
  const Asset USD_JPY{
      MakeAsset("^USDJPY", epoch_core::AssetClass::FX, epoch_core::Exchange::FX)};
  const Asset GBP_USD{
      MakeAsset("^GBPUSD", epoch_core::AssetClass::FX, epoch_core::Exchange::FX)};
  const Asset GC{
      MakeAsset("GC", epoch_core::AssetClass::Futures, epoch_core::Exchange::COMEX)};
  const Asset ZC{
      MakeAsset("ZC", epoch_core::AssetClass::Futures, epoch_core::Exchange::CBOT)};
  const Asset ES{
      MakeAsset("ES", epoch_core::AssetClass::Futures, epoch_core::Exchange::GBLX)};
};

} // namespace data_sdk::asset
