#pragma once
//
// Created by dewe on 1/8/23.
//
#include <epoch_data_sdk/model/asset/asset_specification.hpp>
#include <epoch_data_sdk/model/asset/contract_spec.hpp>
#include <epoch_core/common_utils.h>

namespace data_sdk {

namespace calendar {
class DailyCalendar;
}

namespace asset {

AssetSpecification GetUnderlyingAssetSpec(const AssetSpecification &parent);

struct AssetData {
  std::string id;
  std::string rootId;
  std::string ticker;

  bool operator==(AssetData const &) const = default;

  struct Hash {
    size_t operator()(AssetData const &asset) const {
      std::hash<std::string> hasher;
      return hasher(asset.id) ^ hasher(asset.rootId) ^ hasher(asset.ticker);
    }
  };
};

class Asset {
public:
  explicit Asset(AssetSpecification const &spec);

  Asset(ContractInfo contractInfo, AssetSpecification const &spec,
        AssetSpecification const &parentSpec);

  Asset(AssetSpecification const &spec, AssetSpecification const &parentSpec);

  Asset(AssetSpecification const &spec, ContractInfo const &contractInfo,
        AssetSpecification const &parentSpec);

  AssetData GetData() const;

  std::string ToString() const;

  friend std::ostream &operator<<(std::ostream &ss, Asset const &asset);

  auto operator<=>(data_sdk::Symbol const &other) const {
    return GetSymbol() <=> other;
  }

  std::string GetSymbolStr() const { return m_spec.GetSymbol().get(); }

  data_sdk::Symbol GetSymbol() const { return m_spec.GetSymbol(); }

  epoch_core::CountryCurrency GetCurrency() const noexcept {
    return m_spec.GetCountryCurrency();
  }

  epoch_core::Exchange GetExchange() const;

  AssetClass GetAssetClass() const { return m_spec.GetAssetClass(); }

  std::string GetID() const { return m_spec.GetID(); }

  bool IsCrypto() const { return GetAssetClass() == AssetClass::Crypto; }

  bool IsFX() const { return GetAssetClass() == AssetClass::FX; }

  bool IsStocks() const { return GetAssetClass() == AssetClass::Stocks; }

  bool IsFuturesContract() const {
    return GetAssetClass() == AssetClass::Futures;
  }

  bool IsContract() const { return IsFuturesContract(); }

  decimal::Decimal Quantize(const decimal::Decimal &value) const;

  decimal::Decimal GetMultiplier() const noexcept {
    return m_spec.GetMultiplier();
  }

  bool operator==(Asset const &asset) const { return asset.m_spec == m_spec; }

  bool operator<(Asset const &asset) const { return asset.m_spec < m_spec; }

  inline const AssetSpecification &GetSpec() const { return m_spec; }

  inline Asset GetRoot() const { return Asset{m_parentAssetSpec}; }

  Asset MakeAsset(data_sdk::Symbol const &newSymbol) const;

  Asset MakeContract(data_sdk::Symbol const &contractInfo) const;

  Asset MakeRootFutures() const;

  Asset MakeFuturesContinuation() const;

  Asset MakeContract(ContractInfo const &contractInfo) const;

  inline bool HasContractInfo() const noexcept {
    return m_contractInfo.has_value();
  }

  inline bool IsFuturesContinuation() const {
    return IsFuturesContract() &&
           GetSymbolStr().starts_with(FUTURES_CONTINUATION_PREFIX);
  }

  ContractInfo GetContractInfo() const;

  CurrencyPair GetCurrencyPair() const;

private:
  AssetSpecification m_spec;
  AssetSpecification m_parentAssetSpec;
  std::optional<ContractInfo> m_contractInfo;

  friend struct AssetHash;
};

using Assets = std::vector<Asset>;

struct AssetHash {
  size_t operator()(Asset const &asset) const {
    return AssetSpecification::Hash()(asset.m_spec);
  }
};

template <class T> using AssetHashMap = std::unordered_map<Asset, T, AssetHash>;
using AssetHashSet = std::unordered_set<Asset, AssetHash>;

template <class T>
using AssetDataHashMap = std::unordered_map<AssetData, T, AssetData::Hash>;
using AssetDataHashSet = std::unordered_set<AssetData, AssetData::Hash>;
using AssetDataList = std::vector<AssetData>;

struct AssetHashSetHash {
  size_t operator()(AssetHashSet const &assetHashSet) const {
    return std::accumulate(assetHashSet.begin(), assetHashSet.end(), 0UL,
                           [](size_t result, const asset::Asset &x) {
                             return result ^ AssetHash()(x);
                           });
  }
};

template <class T>
using AssetSetHashMap = std::unordered_map<AssetHashSet, T, AssetHashSetHash>;

template <class T> using AssetClassMap = std::unordered_map<AssetClass, T>;
using AssetClassSet = std::unordered_set<AssetClass>;

data_sdk::SymbolSet ExtractSymbols(AssetHashSet const &assets);

data_sdk::SymbolSet ExtractSymbols(AssetHashSet const &assets, AssetClass assetClass);

AssetHashSet ExtractAssets(AssetHashSet const &assets, AssetClass assetClass);

template <AssetClass assetClass> inline bool IsAssetClass(Asset const &asset) {
  return asset.GetAssetClass() == assetClass;
}

inline auto IsAssetClass(AssetClass assetClass) {
  return [assetClass](Asset const &asset) {
    return asset.GetAssetClass() == assetClass;
  };
}

template <AssetClass assetClass> inline auto IsAssetClassMethod() {
  return [](Asset const &asset) { return asset.GetAssetClass() == assetClass; };
}

template <AssetClass assetClass> bool IsAny(auto const &assets) {
  return std::ranges::any_of(assets, IsAssetClassMethod<assetClass>());
}

template <class T>
inline AssetClassMap<AssetHashMap<T>>
ExtractAssets(AssetHashMap<T> const &assetMap) {
  AssetClassMap<AssetHashMap<T>> result;
  for (const auto &[asset, data] : assetMap) {
    result[asset.GetAssetClass()][asset] = data;
  }
  return result;
}

template <class T>
AssetHashMap<T> ExtractAssets(std::vector<T> const &container) {
  AssetHashMap<T> result;
  for (const T &element : container) {
    result.insert_or_assign(element.GetAsset(), element);
  }
  return result;
}

AssetClassMap<AssetHashSet>
ExtractAssetsByAssetClass(AssetHashSet const &assets);

ExchangeMap<AssetHashSet> ExtractAssetsByExchange(AssetHashSet const &assets);

std::map<epoch_core::CountryCurrency, AssetHashSet>
ExtractAssetsByCountryCurrency(AssetHashSet const &assets);

AssetClassSet ExtractAssetClasses(AssetHashSet const &assets);

std::set<epoch_core::Exchange> GetExchanges(const asset::AssetHashSet &assets);

std::set<epoch_core::CountryCurrency>
GetCountryCurrencies(const asset::AssetHashSet &assets);

AssetHashSet ExtractExchange(AssetHashSet const &assets,
                             epoch_core::Exchange exchange);

AssetClass GetAssetClassFromExchange(epoch_core::Exchange exchange);

std::ostream &operator<<(std::ostream &streamer, AssetHashSet const &assets);
} // namespace data_sdk::asset
} // namespace data_sdk