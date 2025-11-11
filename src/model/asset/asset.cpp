//
// Created by dewe on 1/10/23.
//
#include <epoch_data_sdk/model/asset/asset.hpp>
#include <epoch_data_sdk/model/builder/asset_builder.hpp>
#include <epoch_data_sdk/common/decimal_utils.hpp>
#include "boost/algorithm/string.hpp"
#include <epoch_data_sdk/model/asset/currency_pair.hpp>
#include <epoch_data_sdk/model/asset/constants.hpp>

namespace data_sdk::asset {
Asset::Asset(AssetSpecification const &spec, ContractInfo const &contractInfo,
             AssetSpecification const &parentSpec)
    : m_spec(spec), m_parentAssetSpec(parentSpec),
      m_contractInfo(contractInfo) {}

AssetData Asset::GetData() const {
  return {.id = m_spec.GetID(),
          .rootId = m_parentAssetSpec.GetID(),
          .ticker = m_spec.GetSymbol().get()};
}

Exchange Asset::GetExchange() const {
  auto exchange = m_spec.GetExchange();
  AssertFromStream(exchange != epoch_core::Exchange::Null,
                   "Invalid epoch_core::Exchange");
  return exchange;
}

Asset::Asset(AssetSpecification const &spec,
             AssetSpecification const &parentSpec)
    : m_spec(spec), m_parentAssetSpec(parentSpec) {
  if (spec.GetSymbol() != parentSpec.GetSymbol()) {
    if (IsFuturesContract()) {
      if (not GetSymbolStr().starts_with(FUTURES_CONTINUATION_PREFIX)) {
        m_contractInfo =
            ContractInfo::MakeFuturesContractInfo(spec.GetSymbol().get());
      }
    }
  }
}

Asset::Asset(AssetSpecification const &spec) : Asset(spec, spec) {}

Asset::Asset(ContractInfo contractInfo, AssetSpecification const &spec,
             AssetSpecification const &parentSpec)
    : m_spec(spec), m_parentAssetSpec(parentSpec),
      m_contractInfo(std::move(contractInfo)) {}

std::string Asset::ToString() const {
  std::stringstream ss;
  ss << *this;
  return ss.str();
}

decimal::Decimal Asset::Quantize(const decimal::Decimal &value) const {
  using namespace data_sdk;
  if (value.isspecial()) {
    return 0_dec;
  }

  try {
    return value.quantize(m_spec.GetMinTick());
  } catch (...) {
    SPDLOG_WARN("Could not quantize {} to tickSize {}.", fromDecimal(value),
                fromDecimal(m_spec.GetMinTick()));
  }
  return value;
}

ContractInfo Asset::GetContractInfo() const {
  try {
    return m_contractInfo.value();
  } catch (std::bad_optional_access const &) {
    throw std::runtime_error(
        std::format("Called GetContractInfo() from a nonderivative asset: {}",
                    this->ToString()));
  }
}

CurrencyPair Asset::GetCurrencyPair() const {
  try {
    return m_spec.GetCurrencyPair().value();
  } catch (std::bad_optional_access const &) {
    throw std::runtime_error(
        std::format("Called GetCurrencyPair() from a non currency asset: {}",
                    this->ToString()));
  }
}

Asset Asset::MakeRootFutures() const {
  auto symbol = GetSymbolStr();
  return symbol.starts_with(FUTURES_CONTINUATION_PREFIX)
             ? Asset(MakeContract(symbol.substr(1)))
             : *this;
}

Asset Asset::MakeContract(data_sdk::Symbol const &contractInfo) const {
  return Asset{m_spec.MakeContractSpecification(contractInfo),
               IsFuturesContinuation() ? m_parentAssetSpec : m_spec};
}

Asset Asset::MakeFuturesContinuation() const {
  auto symbol = GetSymbolStr();
  return symbol.starts_with(FUTURES_CONTINUATION_PREFIX)
             ? *this
             : Asset(MakeContract(std::string(FUTURES_CONTINUATION_PREFIX) + symbol));
}

Asset Asset::MakeContract(ContractInfo const &contractInfo) const {
  return Asset{contractInfo,
               m_spec.MakeContractSpecification(contractInfo.GetSymbol()),
               this->m_spec};
}

Asset Asset::MakeAsset(data_sdk::Symbol const &newSymbol) const {
  return Asset{m_spec.MakeAssetSpecification(newSymbol), this->m_spec};
}

std::ostream &operator<<(std::ostream &ss, Asset const &asset) {
  // Define a consistent width for field names
  const int fieldWidth = 15;

  ss << "Asset(\n"
     << std::left << std::setw(fieldWidth)
     << "  std::string: " << asset.m_spec.GetSymbol() << ",\n"
     << std::left << std::setw(fieldWidth)
     << "  Class: " << asset.m_spec.GetAssetClass() << ",\n"
     << std::left << std::setw(fieldWidth)
     << "  epoch_core::Exchange: " << asset.m_spec.GetExchange() << ",\n"
     << std::left << std::setw(fieldWidth)
     << "  Currency: " << asset.m_spec.GetCountryCurrency() << ",\n"
     << std::left << std::setw(fieldWidth)
     << "  Multiplier: " << asset.m_spec.GetMultiplier() << ",\n";

  if (asset.m_contractInfo) {
    ss << std::left << std::setw(fieldWidth)
       << "  ContractInfo: " << asset.m_contractInfo.value() << ",\n";
  }
  ss << ")";
  return ss;
}

SymbolSet ExtractSymbols(AssetHashSet const &assets) {
  SymbolSet result;
  for (const Asset &asset : assets) {
    result.insert(asset.GetSymbol());
  }
  return result;
}

SymbolSet ExtractSymbols(AssetHashSet const &assets, AssetClass assetClass) {
  SymbolSet result;
  for (const Asset &asset : assets) {
    if (asset.GetAssetClass() == assetClass) {
      result.insert(asset.GetSymbol());
    }
  }
  return result;
}

AssetHashSet ExtractAssets(AssetHashSet const &assets, AssetClass assetClass) {
  AssetHashSet result;
  for (const Asset &asset : assets) {
    if (asset.GetAssetClass() == assetClass) {
      result.insert(asset);
    }
  }
  return result;
}

AssetClassMap<AssetHashSet>
ExtractAssetsByAssetClass(AssetHashSet const &assets) {
  AssetClassMap<AssetHashSet> result;
  for (const Asset &asset : assets) {
    result[asset.GetAssetClass()].insert(asset);
  }
  return result;
}

AssetClassSet ExtractAssetClasses(AssetHashSet const &assets) {
  AssetClassSet result;
  for (const Asset &asset : assets) {
    result.insert(asset.GetAssetClass());
  }
  return result;
}

ExchangeMap<AssetHashSet> ExtractAssetsByExchange(AssetHashSet const &assets) {
  ExchangeMap<AssetHashSet> result;
  for (const Asset &asset : assets) {
    result[asset.GetExchange()].insert(asset);
  }
  return result;
}

std::map<epoch_core::CountryCurrency, AssetHashSet>
ExtractAssetsByCountryCurrency(AssetHashSet const &assets) {
  std::map<epoch_core::CountryCurrency, AssetHashSet> result;
  for (const Asset &asset : assets) {
    result[asset.GetCurrency()].insert(asset);
  }
  return result;
}

AssetHashSet ExtractExchange(AssetHashSet const &assets,
                             epoch_core::Exchange exchange) {
  AssetHashSet result;
  std::ranges::copy_if(
      assets, std::inserter(result, result.end()),
      [exchange](auto &&asset) { return asset.GetExchange() == exchange; });
  return result;
}

AssetClass GetAssetClassFromExchange(Exchange exchange) {
  switch (exchange) {
  case epoch_core::Exchange::COINBASE:
    return AssetClass::Crypto;
  case epoch_core::Exchange::NYSE:
  case epoch_core::Exchange::NASDAQ:
    return AssetClass::Stocks;
  case epoch_core::Exchange::NYMEX:
  case Exchange::COMEX:
  case Exchange::CME:
  case Exchange::CBOT:
  case Exchange::ICEUS:
    return AssetClass::Futures;
  case Exchange::FX:
    return AssetClass::FX;
  default:
    break;
  }
  throw std::runtime_error(
      "GetAssetClassFromExchange cannot process ExchangeNull");
}

std::set<epoch_core::Exchange> GetExchanges(const asset::AssetHashSet &assets) {
  std::set<epoch_core::Exchange> exchanges;
  std::ranges::transform(
      assets, std::inserter(exchanges, exchanges.end()),
      [](const asset::Asset &asset) { return asset.GetExchange(); });
  return exchanges;
}

std::set<epoch_core::CountryCurrency>
GetCountryCurrencies(const asset::AssetHashSet &assets) {
  std::set<epoch_core::CountryCurrency> countryCurrencies;
  std::ranges::transform(
      assets, std::inserter(countryCurrencies, countryCurrencies.end()),
      [](const asset::Asset &asset) { return asset.GetCurrency(); });
  return countryCurrencies;
}

std::ostream &operator<<(std::ostream &streamer, AssetHashSet const &assets) {
  streamer << "{";
  for (auto &&asset : assets) {
    streamer << "\t" << asset << "\n";
  }
  streamer << "}";
  return streamer;
}
} // namespace data_sdk::asset
