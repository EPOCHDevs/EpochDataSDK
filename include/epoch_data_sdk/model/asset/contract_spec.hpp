#pragma once
//
// Created by dewe on 12/23/23.
//
#include <epoch_data_sdk/common/symbol.hpp>
#include <epoch_frame/datetime.h>

namespace data_sdk::asset {
struct ContractInfo {
  ContractInfo(epoch_frame::Date const &expiryDate,
               std::string symbol);

  static ContractInfo MakeFuturesContractInfo(data_sdk::Symbol const &contract);

  epoch_frame::Date GetExpirationDate() const { return m_expirationDate; }

  bool operator==(const ContractInfo &info) const {
    return m_symbol == info.m_symbol;
  }

  std::string GetSymbol() const noexcept { return m_symbol; }

  friend std::ostream &operator<<(std::ostream &os, ContractInfo const &info);

  static chrono_year GetFullYear(data_sdk::Symbol const &yearSuffix);
  static chrono_year GetDecade(data_sdk::Symbol const &yearSuffix);

private:
  epoch_frame::Date m_expirationDate;
  std::string m_symbol;
};
} // namespace data_sdk::asset