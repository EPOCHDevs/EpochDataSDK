//
// Created by dewe on 12/23/23.
//
#include <epoch_data_sdk/model/asset/contract_spec.hpp>
#include <epoch_data_sdk/model/asset/constants.hpp>
#include <epoch_data_sdk/common/futures_constants.hpp>
#include <epoch_core/common_utils.h>

using namespace std::chrono_literals;

namespace data_sdk::asset {
chrono_year ContractInfo::GetDecade(data_sdk::Symbol const &yearSuffix) {
  const auto shortYear = std::stoi(yearSuffix.get());
  constexpr int pivot = 50; // assuming 1950 is when we started trading
  return (shortYear < pivot) ? chrono_year{2000} : chrono_year{1900};
}

chrono_year ContractInfo::GetFullYear(data_sdk::Symbol const &yearSuffix) {
  return GetDecade(yearSuffix) + std::chrono::years(std::stoi(yearSuffix.get()));
}

ContractInfo
ContractInfo::MakeFuturesContractInfo(data_sdk::Symbol const &contract) {
  const auto contractStr = contract.get();

  // Extract year suffix (last 2 characters)
  chrono_year year =
      GetFullYear(data_sdk::Symbol(contractStr.substr(contractStr.length() - 2)));

  // Extract month code (3rd character from end)
  const chrono_month month =
      epoch_core::lookup(FuturesConstants::month_mapping, contractStr[contractStr.length() - 3]);

  // Extract symbol (everything except last 3 characters)
  const std::string symbol = contractStr.substr(0, contractStr.length() - 3);

  // TODO: Provide more realistic heuristics
  return ContractInfo{epoch_frame::Date{year, month, std::chrono::day{15}},
                      contractStr};
}

ContractInfo::ContractInfo(epoch_frame::Date const &expiryDate,
                           std::string symbol)
    : m_expirationDate(expiryDate), m_symbol(std::move(symbol)) {}

std::ostream &operator<<(std::ostream &os, ContractInfo const &info) {
  os << "ContractInfo(" << info.m_symbol << ", " << info.m_expirationDate
     << ")";
  return os;
}
} // namespace data_sdk::asset