//
// Created by Claude Code for test coverage
//

#include <catch2/catch_test_macros.hpp>
#include <epoch_data_sdk/model/asset/asset.hpp>
#include <epoch_data_sdk/model/builder/asset_builder.hpp>
#include <epoch_data_sdk/common/symbol.hpp>

using namespace data_sdk;
using namespace data_sdk::asset;
using namespace epoch_core;

TEST_CASE("Asset construction from specification", "[asset]") {
  AssetSpecificationQuery query;
  query.required = "AAPL-Stocks";

  AssetSpecification spec = MakeAssetSpec(query);
  Asset asset(spec);

  REQUIRE(asset.GetSymbolStr() == "AAPL");
  REQUIRE(asset.GetAssetClass() == AssetClass::Stocks);
  REQUIRE(asset.GetID() == "AAPL-Stocks");
}

TEST_CASE("Asset GetData method", "[asset]") {
  auto asset = MakeAsset("AAPL", AssetClass::Stocks);
  AssetData data = asset.GetData();

  REQUIRE(data.ticker == "AAPL");
  REQUIRE_FALSE(data.id.empty());
}

TEST_CASE("Asset ToString method", "[asset]") {
  auto asset = MakeAsset("AAPL", AssetClass::Stocks);
  std::string str = asset.ToString();

  REQUIRE_FALSE(str.empty());
  REQUIRE(str.find("AAPL") != std::string::npos);
}

TEST_CASE("Asset stream output operator", "[asset]") {
  auto asset = MakeAsset("AAPL", AssetClass::Stocks);

  std::ostringstream oss;
  oss << asset;
  std::string output = oss.str();

  REQUIRE_FALSE(output.empty());
  REQUIRE(output.find("AAPL") != std::string::npos);
}

TEST_CASE("Asset GetSymbol and GetSymbolStr", "[asset]") {
  auto asset = MakeAsset("MSFT", AssetClass::Stocks);

  Symbol symbol = asset.GetSymbol();
  std::string symbolStr = asset.GetSymbolStr();

  REQUIRE(symbol.get() == "MSFT");
  REQUIRE(symbolStr == "MSFT");
}

TEST_CASE("Asset GetCurrency method", "[asset]") {
  auto asset = MakeAsset("AAPL", AssetClass::Stocks);
  CountryCurrency currency = asset.GetCurrency();

  // US stocks should have USD currency
  REQUIRE(currency == CountryCurrency::USD);
}

TEST_CASE("Asset GetExchange method", "[asset]") {
  auto asset = MakeAsset("AAPL", AssetClass::Stocks);
  Exchange exchange = asset.GetExchange();

  // AAPL trades on NASDAQ
  REQUIRE(exchange != Exchange::Null);
}

TEST_CASE("Asset GetAssetClass method", "[asset]") {
  SECTION("Stocks") {
    auto asset = MakeAsset("AAPL", AssetClass::Stocks);
    REQUIRE(asset.GetAssetClass() == AssetClass::Stocks);
  }

  SECTION("Crypto") {
    auto asset = MakeAsset("^BTCUSD", AssetClass::Crypto, Exchange::COINBASE);
    REQUIRE(asset.GetAssetClass() == AssetClass::Crypto);
  }

  SECTION("FX") {
    auto asset = MakeAsset("^EURUSD", AssetClass::FX, Exchange::FX);
    REQUIRE(asset.GetAssetClass() == AssetClass::FX);
  }
}

TEST_CASE("Asset type checking methods", "[asset]") {
  SECTION("IsStocks") {
    auto asset = MakeAsset("AAPL", AssetClass::Stocks);
    REQUIRE(asset.IsStocks());
    REQUIRE_FALSE(asset.IsCrypto());
    REQUIRE_FALSE(asset.IsFX());
    REQUIRE_FALSE(asset.IsFuturesContract());
  }

  SECTION("IsCrypto") {
    auto asset = MakeAsset("^BTCUSD", AssetClass::Crypto, Exchange::COINBASE);
    REQUIRE(asset.IsCrypto());
    REQUIRE_FALSE(asset.IsStocks());
    REQUIRE_FALSE(asset.IsFX());
    REQUIRE_FALSE(asset.IsFuturesContract());
  }

  SECTION("IsFX") {
    auto asset = MakeAsset("^EURUSD", AssetClass::FX, Exchange::FX);
    REQUIRE(asset.IsFX());
    REQUIRE_FALSE(asset.IsStocks());
    REQUIRE_FALSE(asset.IsCrypto());
    REQUIRE_FALSE(asset.IsFuturesContract());
  }
}

TEST_CASE("Asset IsContract method", "[asset]") {
  auto stockAsset = MakeAsset("AAPL", AssetClass::Stocks);
  REQUIRE_FALSE(stockAsset.IsContract());

  // Futures would return true, but need a futures contract fixture
}

TEST_CASE("Asset GetMultiplier method", "[asset]") {
  auto asset = MakeAsset("AAPL", AssetClass::Stocks);
  decimal::Decimal multiplier = asset.GetMultiplier();

  // Most stocks have multiplier of 1
  REQUIRE(multiplier == 1.0_dec);
}

TEST_CASE("Asset Quantize method", "[asset]") {
  auto asset = MakeAsset("AAPL", AssetClass::Stocks);

  // Quantize price to asset's tick size
  auto quantized = asset.Quantize(150.123456_dec);

  // Should round to valid price increment
  REQUIRE(quantized <= 150.13_dec);
  REQUIRE(quantized >= 150.12_dec);
}

TEST_CASE("Asset equality operator", "[asset]") {
  auto asset1 = MakeAsset("AAPL", AssetClass::Stocks);
  auto asset2 = MakeAsset("AAPL", AssetClass::Stocks);
  auto asset3 = MakeAsset("MSFT", AssetClass::Stocks);

  REQUIRE(asset1 == asset2);
  REQUIRE_FALSE(asset1 == asset3);
}

TEST_CASE("Asset less-than operator", "[asset]") {
  auto assetAAPL = MakeAsset("AAPL", AssetClass::Stocks);
  auto assetMSFT = MakeAsset("MSFT", AssetClass::Stocks);

  // Assets are ordered by their specification
  // The specific ordering depends on AssetSpecification::operator<
  bool ordered = (assetAAPL < assetMSFT) || (assetMSFT < assetAAPL);
  REQUIRE(ordered);
}

TEST_CASE("Asset comparison with Symbol", "[asset]") {
  auto asset = MakeAsset("AAPL", AssetClass::Stocks);
  Symbol symbol{"AAPL"};

  auto cmp = asset <=> symbol;
  REQUIRE(cmp == std::strong_ordering::equal);
}

TEST_CASE("Asset comparison with string", "[asset]") {
  auto asset = MakeAsset("AAPL", AssetClass::Stocks);
  std::string symbolStr = "AAPL";

  auto cmp = asset <=> symbolStr;
  REQUIRE(cmp == std::strong_ordering::equal);

  std::string different = "MSFT";
  auto cmpDiff = asset <=> different;
  REQUIRE(cmpDiff != std::strong_ordering::equal);
}

TEST_CASE("Asset GetSpec method", "[asset]") {
  auto asset = MakeAsset("AAPL", AssetClass::Stocks);
  const AssetSpecification& spec = asset.GetSpec();

  REQUIRE(spec.GetSymbol().get() == "AAPL");
  REQUIRE(spec.GetAssetClass() == AssetClass::Stocks);
}

TEST_CASE("Asset GetID returns consistent identifier", "[asset]") {
  auto asset = MakeAsset("AAPL", AssetClass::Stocks);
  std::string id1 = asset.GetID();
  std::string id2 = asset.GetID();

  REQUIRE(id1 == id2);
  REQUIRE_FALSE(id1.empty());
}

TEST_CASE("AssetData Hash functor", "[asset]") {
  AssetData data1{"AAPL-Stocks", "AAPL", "AAPL"};
  AssetData data2{"MSFT-Stocks", "MSFT", "MSFT"};
  AssetData data3{"AAPL-Stocks", "AAPL", "AAPL"};

  AssetData::Hash hasher;
  size_t hash1 = hasher(data1);
  size_t hash2 = hasher(data2);
  size_t hash3 = hasher(data3);

  // Same data should produce same hash
  REQUIRE(hash1 == hash3);
  // Different data should (usually) produce different hash
  REQUIRE(hash1 != hash2);
}

TEST_CASE("AssetData equality operator", "[asset]") {
  AssetData data1{"AAPL-Stocks", "AAPL", "AAPL"};
  AssetData data2{"AAPL-Stocks", "AAPL", "AAPL"};
  AssetData data3{"MSFT-Stocks", "MSFT", "MSFT"};

  REQUIRE(data1 == data2);
  REQUIRE_FALSE(data1 == data3);
}

TEST_CASE("Asset with different currencies", "[asset]") {
  auto usdAsset = MakeAsset("AAPL", AssetClass::Stocks);

  REQUIRE(usdAsset.GetCurrency() == CountryCurrency::USD);
}

TEST_CASE("Asset created from MakeAsset helper", "[asset]") {
  Asset asset = MakeAsset("GOOG", AssetClass::Stocks);

  REQUIRE(asset.GetSymbolStr() == "GOOG");
  REQUIRE(asset.GetAssetClass() == AssetClass::Stocks);
  REQUIRE(asset.IsStocks());
}

TEST_CASE("Asset with exchange specified", "[asset]") {
  Asset asset = MakeAsset("IBM", AssetClass::Stocks, Exchange::NYSE);

  REQUIRE(asset.GetSymbolStr() == "IBM");
  REQUIRE(asset.GetExchange() == Exchange::NYSE);
}

TEST_CASE("Crypto asset characteristics", "[asset]") {
  auto btcAsset = MakeAsset("^BTCUSD", AssetClass::Crypto, Exchange::COINBASE);

  REQUIRE(btcAsset.IsCrypto());
  REQUIRE(btcAsset.GetSymbolStr() == "^BTCUSD");
  REQUIRE(btcAsset.GetExchange() == Exchange::COINBASE);
}

TEST_CASE("FX asset characteristics", "[asset]") {
  auto eurUsdAsset = MakeAsset("^EURUSD", AssetClass::FX, Exchange::FX);

  REQUIRE(eurUsdAsset.IsFX());
  REQUIRE(eurUsdAsset.GetSymbolStr() == "^EURUSD");
  REQUIRE(eurUsdAsset.GetExchange() == Exchange::FX);
}

TEST_CASE("Asset multiplier for different asset classes", "[asset]") {
  auto stockAsset = MakeAsset("AAPL", AssetClass::Stocks);
  auto cryptoAsset = MakeAsset("^BTCUSD", AssetClass::Crypto, Exchange::COINBASE);
  auto fxAsset = MakeAsset("^EURUSD", AssetClass::FX, Exchange::FX);

  // All should have valid multipliers (typically 1 for stocks/crypto/fx)
  REQUIRE(stockAsset.GetMultiplier() > 0.0_dec);
  REQUIRE(cryptoAsset.GetMultiplier() > 0.0_dec);
  REQUIRE(fxAsset.GetMultiplier() > 0.0_dec);
}
