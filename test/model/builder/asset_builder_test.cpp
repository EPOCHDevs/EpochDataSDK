//
// Created by Claude Code for test coverage
//

// Include catch FIRST before duckdb macros are defined
#include <catch2/catch_test_macros.hpp>

#include <epoch_data_sdk/model/builder/asset_builder.hpp>
#include <epoch_data_sdk/model/asset/asset_database.hpp>
#include <epoch_data_sdk/common/symbol.hpp>
#include <yaml-cpp/yaml.h>

using namespace data_sdk;
using namespace data_sdk::asset;
using namespace epoch_core;

TEST_CASE("MakeAssetSpec with string ID query", "[asset_builder]") {
  AssetSpecificationQuery query;
  query.required = "AAPL-Stocks";

  AssetSpecification spec = MakeAssetSpec(query);

  REQUIRE(spec.GetID() == "AAPL-Stocks");
  REQUIRE(spec.GetSymbol().get() == "AAPL");
  REQUIRE(spec.GetAssetClass() == AssetClass::Stocks);
}

TEST_CASE("MakeAssetSpec with Symbol and AssetClass pair", "[asset_builder]") {
  AssetSpecificationQuery query{
    .required = std::pair{Symbol{"MSFT"}, AssetClass::Stocks},
    .exchange = Exchange::Null,
    .currency = CountryCurrency::Null
  };

  AssetSpecification spec = MakeAssetSpec(query);

  REQUIRE(spec.GetSymbol().get() == "MSFT");
  REQUIRE(spec.GetAssetClass() == AssetClass::Stocks);
}

TEST_CASE("MakeAssetSpec with explicit exchange", "[asset_builder]") {
  AssetSpecificationQuery query{
    .required = std::pair{Symbol{"AAPL"}, AssetClass::Stocks},
    .exchange = Exchange::NASDAQ,
    .currency = CountryCurrency::Null
  };

  AssetSpecification spec = MakeAssetSpec(query);

  REQUIRE(spec.GetSymbol().get() == "AAPL");
  REQUIRE(spec.GetAssetClass() == AssetClass::Stocks);
  REQUIRE(spec.GetExchange() == Exchange::NASDAQ);
}

TEST_CASE("MakeAssetSpec with explicit currency", "[asset_builder]") {
  AssetSpecificationQuery query{
    .required = std::pair{Symbol{"AAPL"}, AssetClass::Stocks},
    .exchange = Exchange::Null,
    .currency = CountryCurrency::USD
  };

  AssetSpecification spec = MakeAssetSpec(query);

  REQUIRE(spec.GetSymbol().get() == "AAPL");
  REQUIRE(spec.GetCountryCurrency() == CountryCurrency::USD);
}

TEST_CASE("MakeAsset convenience function with query", "[asset_builder]") {
  AssetSpecificationQuery query;
  query.required = "GOOG-Stocks";

  Asset asset = MakeAsset(query);

  REQUIRE(asset.GetID() == "GOOG-Stocks");
  REQUIRE(asset.GetSymbolStr() == "GOOG");
  REQUIRE(asset.GetAssetClass() == AssetClass::Stocks);
}

TEST_CASE("MakeAsset with explicit parameters", "[asset_builder]") {
  Asset asset = MakeAsset("TSLA", AssetClass::Stocks);

  REQUIRE(asset.GetSymbolStr() == "TSLA");
  REQUIRE(asset.GetAssetClass() == AssetClass::Stocks);
}

TEST_CASE("MakeAsset with exchange parameter", "[asset_builder]") {
  Asset asset = MakeAsset("AAPL", AssetClass::Stocks, Exchange::NASDAQ);

  REQUIRE(asset.GetSymbolStr() == "AAPL");
  REQUIRE(asset.GetAssetClass() == AssetClass::Stocks);
  REQUIRE(asset.GetExchange() == Exchange::NASDAQ);
}

TEST_CASE("MakeAsset with exchange and currency parameters", "[asset_builder]") {
  Asset asset = MakeAsset("IBM", AssetClass::Stocks, Exchange::NYSE, CountryCurrency::USD);

  REQUIRE(asset.GetSymbolStr() == "IBM");
  REQUIRE(asset.GetAssetClass() == AssetClass::Stocks);
  REQUIRE(asset.GetExchange() == Exchange::NYSE);
  REQUIRE(asset.GetCurrency() == CountryCurrency::USD);
}

TEST_CASE("MakeAssets batch creation from vector", "[asset_builder]") {
  std::vector<AssetSpecificationQuery> queries = {
    AssetSpecificationQuery{.required = "AAPL-Stocks"},
    AssetSpecificationQuery{.required = "MSFT-Stocks"},
    AssetSpecificationQuery{.required = "GOOG-Stocks"}
  };

  AssetHashSet assets = MakeAssets(queries);

  REQUIRE(assets.size() == 3);

  // Check that all assets are in the set
  bool hasAAPL = false, hasMSFT = false, hasGOOG = false;
  for (const auto& asset : assets) {
    if (asset.GetSymbolStr() == "AAPL") hasAAPL = true;
    if (asset.GetSymbolStr() == "MSFT") hasMSFT = true;
    if (asset.GetSymbolStr() == "GOOG") hasGOOG = true;
  }
  REQUIRE(hasAAPL);
  REQUIRE(hasMSFT);
  REQUIRE(hasGOOG);
}

TEST_CASE("MakeAssets with empty vector", "[asset_builder][edge_case]") {
  std::vector<AssetSpecificationQuery> queries;

  AssetHashSet assets = MakeAssets(queries);

  REQUIRE(assets.empty());
}

TEST_CASE("MakeAssets with single asset", "[asset_builder]") {
  std::vector<AssetSpecificationQuery> queries = {
    AssetSpecificationQuery{.required = "SPY-Stocks"}
  };

  AssetHashSet assets = MakeAssets(queries);

  REQUIRE(assets.size() == 1);
  REQUIRE(assets.begin()->GetSymbolStr() == "SPY");
}

TEST_CASE("MakeAsset with Crypto asset", "[asset_builder]") {
  Asset asset = MakeAsset("^BTCUSD", AssetClass::Crypto, Exchange::COINBASE);

  REQUIRE(asset.GetSymbolStr() == "^BTCUSD");
  REQUIRE(asset.GetAssetClass() == AssetClass::Crypto);
  REQUIRE(asset.GetExchange() == Exchange::COINBASE);
}

TEST_CASE("MakeAsset with FX asset", "[asset_builder]") {
  Asset asset = MakeAsset("^EURUSD", AssetClass::FX, Exchange::FX);

  REQUIRE(asset.GetSymbolStr() == "^EURUSD");
  REQUIRE(asset.GetAssetClass() == AssetClass::FX);
  REQUIRE(asset.GetExchange() == Exchange::FX);
}

TEST_CASE("MakeAsset with Futures asset", "[asset_builder]") {
  Asset asset = MakeAsset("ES", AssetClass::Futures, Exchange::GBLX);

  REQUIRE(asset.GetSymbolStr() == "ES");
  REQUIRE(asset.GetAssetClass() == AssetClass::Futures);
  REQUIRE(asset.GetExchange() == Exchange::GBLX);
}

TEST_CASE("YAML decode scalar string", "[asset_builder][yaml]") {
  YAML::Node node = YAML::Load("AAPL-Stocks");

  AssetSpecificationQuery query;
  REQUIRE(YAML::convert<AssetSpecificationQuery>::decode(node, query));

  REQUIRE(std::holds_alternative<std::string>(query.required));
  REQUIRE(std::get<std::string>(query.required) == "AAPL-Stocks");
}

TEST_CASE("YAML decode with ticker and class", "[asset_builder][yaml]") {
  std::string yaml_str = R"(
ticker: TSLA
class: Stocks
)";
  YAML::Node node = YAML::Load(yaml_str);

  AssetSpecificationQuery query;
  REQUIRE(YAML::convert<AssetSpecificationQuery>::decode(node, query));

  REQUIRE(std::holds_alternative<std::pair<Symbol, AssetClass>>(query.required));
  auto [symbol, assetClass] = std::get<1>(query.required);
  REQUIRE(symbol.get() == "TSLA");
  REQUIRE(assetClass == AssetClass::Stocks);
}

TEST_CASE("YAML decode with exchange", "[asset_builder][yaml]") {
  std::string yaml_str = R"(
ticker: AAPL
class: Stocks
exchange: NASDAQ
)";
  YAML::Node node = YAML::Load(yaml_str);

  AssetSpecificationQuery query;
  REQUIRE(YAML::convert<AssetSpecificationQuery>::decode(node, query));

  auto [symbol, assetClass] = std::get<1>(query.required);
  REQUIRE(symbol.get() == "AAPL");
  REQUIRE(query.exchange == Exchange::NASDAQ);
}

TEST_CASE("YAML decode with currency", "[asset_builder][yaml]") {
  std::string yaml_str = R"(
ticker: IBM
class: Stocks
currency: USD
)";
  YAML::Node node = YAML::Load(yaml_str);

  AssetSpecificationQuery query;
  REQUIRE(YAML::convert<AssetSpecificationQuery>::decode(node, query));

  auto [symbol, assetClass] = std::get<1>(query.required);
  REQUIRE(symbol.get() == "IBM");
  REQUIRE(query.currency == CountryCurrency::USD);
}

TEST_CASE("YAML decode with all fields", "[asset_builder][yaml]") {
  std::string yaml_str = R"(
ticker: MSFT
class: Stocks
exchange: NASDAQ
currency: USD
)";
  YAML::Node node = YAML::Load(yaml_str);

  AssetSpecificationQuery query;
  REQUIRE(YAML::convert<AssetSpecificationQuery>::decode(node, query));

  auto [symbol, assetClass] = std::get<1>(query.required);
  REQUIRE(symbol.get() == "MSFT");
  REQUIRE(assetClass == AssetClass::Stocks);
  REQUIRE(query.exchange == Exchange::NASDAQ);
  REQUIRE(query.currency == CountryCurrency::USD);
}

TEST_CASE("YAML decode without optional fields defaults to Null", "[asset_builder][yaml]") {
  std::string yaml_str = R"(
ticker: AMZN
class: Stocks
)";
  YAML::Node node = YAML::Load(yaml_str);

  AssetSpecificationQuery query;
  REQUIRE(YAML::convert<AssetSpecificationQuery>::decode(node, query));

  REQUIRE(query.exchange == Exchange::Null);
  REQUIRE(query.currency == CountryCurrency::Null);
}

TEST_CASE("MakeAssets with diverse asset classes", "[asset_builder]") {
  std::vector<AssetSpecificationQuery> queries = {
    AssetSpecificationQuery{
      .required = std::pair{Symbol{"AAPL"}, AssetClass::Stocks}
    },
    AssetSpecificationQuery{
      .required = std::pair{Symbol{"^BTCUSD"}, AssetClass::Crypto}
    },
    AssetSpecificationQuery{
      .required = std::pair{Symbol{"GC"}, AssetClass::Futures}
    }
  };

  AssetHashSet assets = MakeAssets(queries);

  REQUIRE(assets.size() == 3);

  // Verify different asset classes
  bool hasStock = false, hasCrypto = false, hasFutures = false;
  for (const auto& asset : assets) {
    if (asset.GetAssetClass() == AssetClass::Stocks) hasStock = true;
    if (asset.GetAssetClass() == AssetClass::Crypto) hasCrypto = true;
    if (asset.GetAssetClass() == AssetClass::Futures) hasFutures = true;
  }
  REQUIRE(hasStock);
  REQUIRE(hasCrypto);
  REQUIRE(hasFutures);
}
