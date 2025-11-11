#pragma once
//
// Created by dewe on 6/17/23.
//
#include <epoch_data_sdk/model/asset/asset.hpp>
#include "fstream"
#include "string"
#include "unordered_map"

namespace data_sdk::asset {
class AssetSpecificationDatabase {

public:
  bool IsInitialized() const noexcept { return !m_storage.empty(); }

  static const AssetSpecificationDatabase &GetInstance();

  AssetSpecification GetAssetSpecification(data_sdk::Symbol const &id) const;

  AssetSpecification
  GetAssetSpecification(data_sdk::Symbol const &symbol,
                        AssetClass assetClass,
                        epoch_core::Exchange const &exchange,
                        epoch_core::CountryCurrency const &currency) const;

  const std::unordered_map<std::string, AssetSpecification> &
  GetAssetSpecifications() const {
    return m_storage;
  }

private:
  AssetSpecificationDatabase(AssetSpecificationDatabase const &) = delete;

  AssetSpecificationDatabase &
  operator=(AssetSpecificationDatabase const &) = delete;

  AssetSpecificationDatabase();

  void
  processAssetSpecifications(const std::vector<AssetSpecificationData> &obj);

  std::unordered_map<std::string, AssetSpecification> m_storage;
  std::unordered_map<
      std::string,
      std::unordered_map<
          AssetClass,
          std::unordered_map<epoch_core::Exchange,
                             std::unordered_map<epoch_core::CountryCurrency,
                                                const AssetSpecification *>>>>
      m_queryStorage;
};
} // namespace data_sdk::asset