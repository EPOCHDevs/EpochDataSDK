//
// Created by adesola on 5/8/24.
//
#include <epoch_data_sdk/model/builder/asset_builder.hpp>

namespace data_sdk::asset {
AssetSpecification MakeAssetSpec(AssetSpecificationQuery const &query) {
  const auto &db = AssetSpecificationDatabase::GetInstance();
  if (std::holds_alternative<std::string>(query.required)) {
    return db.GetAssetSpecification(std::get<0>(query.required));
  }

  const auto [symbol, assetClass] = std::get<1>(query.required);
  return db.GetAssetSpecification(symbol, assetClass, query.exchange,
                                  query.currency);
}

AssetHashSet MakeAssets(std::vector<AssetSpecificationQuery> const &queries) {
  AssetHashSet result;
  for (AssetSpecificationQuery const &assetDetail : queries) {
    result.insert(MakeAsset(assetDetail));
  }
  return result;
}
} // namespace data_sdk::asset

namespace YAML {
bool convert<data_sdk::asset::AssetSpecificationQuery>::decode(
    Node const &node, data_sdk::asset::AssetSpecificationQuery &query) {
  using namespace data_sdk;
  if (node.IsScalar()) {
    query.required = node.as<std::string>();
    return true;
  }
  query.required = std::pair{std::string{
                                 node["ticker"].as<std::string>()},
                             epoch_core::AssetClassWrapper::FromString(
                                 node["class"].as<std::string>())};
  query.exchange =
      node["exchange"]
          ? ExchangeWrapper::FromString(node["exchange"].as<std::string>())
          : epoch_core::Exchange::Null;
  query.currency = node["currency"] ? CountryCurrencyWrapper::FromString(
                                          node["currency"].as<std::string>())
                                    : epoch_core::CountryCurrency::Null;
  return true;
}
} // namespace YAML