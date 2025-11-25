//
// Created by dewe on 6/17/23.
//
#include <epoch_data_sdk/model/asset/asset_database.hpp>
#include <epoch_data_sdk/model/asset/exchanges.hpp>
#include <filesystem>
#include <epoch_data_sdk/model/asset/constants.hpp>
#include <epoch_data_sdk/common/s3_loader.hpp>
#include <epoch_core/macros.h>
#include "spdlog/spdlog.h"

namespace data_sdk::asset {
const AssetSpecificationDatabase &AssetSpecificationDatabase::GetInstance() {
  static AssetSpecificationDatabase spec;
  return spec;
}

AssetSpecificationDatabase::AssetSpecificationDatabase() {
  std::vector<AssetSpecificationData> obj;
  glz::error_ctx ec;

  const auto cache =
      std::filesystem::temp_directory_path() / "asset_specs.json";

  if (std::filesystem::exists(cache)) {
    std::string buffer;
    ec = glz::read_file_json(obj, cache.string(), buffer);
  } else {
    // Try to load from S3 first
    auto result = common::S3Loader::Instance().GetObject(
        AssetDataS3Config::BUCKET_NAME,
        AssetDataS3Config::ASSET_SPECS_KEY);

    AssertFromFormat(result.has_value(),
                     "Failed to retrieve asset spec database from S3: {}",
                     result.error());

    ec = glz::read_json(obj, result.value());
    std::string buffer;
    auto write_ec = glz::write_file_json(obj, cache.string(), buffer);
    if (write_ec) {
      SPDLOG_ERROR("Failed to write asset_spec.json cache: {}",
                   glz::format_error(write_ec, buffer));
    }
  }

  AssertFromFormat(!ec, "Failed to parse asset spec database from S3: {}",
                   glz::format_error(ec));

  SPDLOG_INFO("Successfully loaded asset spec database from S3");
  processAssetSpecifications(obj);
}

void AssetSpecificationDatabase::processAssetSpecifications(
    const std::vector<AssetSpecificationData> &obj) {
  auto addToStorage = [&](AssetSpecification const &spec) {
    auto id = spec.GetID();
    const auto &[it, isInserted] = m_storage.emplace(id, spec);
    AssertFromStream(isInserted, "Found AssetSpec with duplicate id\nCurrent: "
                                     << m_storage.at(id) << ", New: " << spec);

    auto &query = m_queryStorage[spec.GetSymbol().get()][spec.GetAssetClass()]
                                [spec.GetExchange()][spec.GetCountryCurrency()];
    AssertFromStream(query == nullptr, "Duplicate AssetSpecification for Query "
                                       "Parameter\nCurrent: "
                                           << *query << ", New: " << spec);
    query = &it->second;
  };

  for (auto const &assetSpecData : obj) {
    AssetSpecification spec(assetSpecData);

    if (assetSpecData.asset_class == AssetClass::Crypto ||
        assetSpecData.exchange == epoch_core::Exchange::FX) {
      if (!spec.HasCurrencyPair()) {
        SPDLOG_DEBUG("Invalid Currency Pair for AssetSpec: {}.",
                     spec.GetSymbol().get());
        continue;
      }
    }

    addToStorage(spec);
  }
}

AssetSpecification
AssetSpecificationDatabase::GetAssetSpecification(data_sdk::Symbol const &id) const {
  try {
    return m_storage.at(id.get());
  } catch (const std::out_of_range&) {
    SPDLOG_ERROR("Asset not found in database: id={}", id.get());
    throw std::runtime_error("Asset not found: " + id.get());
  }
}

AssetSpecification AssetSpecificationDatabase::GetAssetSpecification(
    data_sdk::Symbol const &symbol, AssetClass assetClass,
    epoch_core::Exchange const &exchange,
    epoch_core::CountryCurrency const &currency) const {
  auto symbolStr = symbol.get();
  auto sanitized =
      (symbolStr.starts_with(FUTURES_CONTINUATION_PREFIX) ? symbolStr.substr(1)
                                                          : symbolStr);

  try {
    auto slot = m_queryStorage.at(sanitized).at(assetClass);
    auto &currencyMap = exchange != epoch_core::Exchange::Null
                            ? slot.at(exchange)
                            : slot.begin()->second;
    auto newSpec = currency != epoch_core::CountryCurrency::Null
                       ? currencyMap.at(currency)
                       : currencyMap.begin()->second;
    return symbolStr == sanitized
               ? *newSpec
               : newSpec->MakeContractSpecification(data_sdk::Symbol(symbolStr));
  } catch (std::out_of_range const &) {
    ThrowExceptionFromFormat(
        "Failed to Find Asset with query\nSymbol: {}\nAssetClass: {}, "
        "Exchange: {}, Currency: {}",
        symbol.get(), AssetClassWrapper::ToString(assetClass),
        ExchangeWrapper::ToString(exchange), TO_STRING(currency));
  }
}
} // namespace data_sdk::asset