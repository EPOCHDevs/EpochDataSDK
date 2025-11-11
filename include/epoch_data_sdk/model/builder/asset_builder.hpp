#pragma once
//
// Created by dewe on 5/13/23.
//
#include <epoch_data_sdk/model/asset/asset_database.hpp>
#include <epoch_data_sdk/common/symbol.hpp>
#include "yaml-cpp/yaml.h"

namespace data_sdk::asset {
struct AssetSpecificationQuery {
  std::variant<std::string,
               std::pair<data_sdk::Symbol, AssetClass>>
      required;
  Exchange exchange{epoch_core::Exchange::Null};
  CountryCurrency currency{epoch_core::CountryCurrency::Null};
};

AssetSpecification MakeAssetSpec(AssetSpecificationQuery const &query);

inline Asset MakeAsset(AssetSpecificationQuery const &query) {
  return Asset{MakeAssetSpec(query)};
}

AssetHashSet MakeAssets(std::vector<AssetSpecificationQuery> const &queries);

inline Asset MakeAsset(
    std::string const &symbol, AssetClass assetClass,
    epoch_core::Exchange exchange = epoch_core::Exchange::Null,
    epoch_core::CountryCurrency currency = epoch_core::CountryCurrency::Null) {
  return MakeAsset(
      AssetSpecificationQuery{.required = std::pair{symbol, assetClass},
                              .exchange = exchange,
                              .currency = currency});
}
} // namespace data_sdk::asset

namespace YAML {
template <> struct convert<data_sdk::asset::AssetSpecificationQuery> {
  static bool decode(Node const &node,
                     data_sdk::asset::AssetSpecificationQuery &query);
};
} // namespace YAML
