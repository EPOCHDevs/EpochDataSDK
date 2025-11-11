//
// Created by dewe on 1/10/23.
//
#include <epoch_data_sdk/common/bar_attribute.hpp>
#include <epoch_core/common_utils.h>

namespace data_sdk {
BarAttribute::Type BarAttribute::getType(std::string const &x) {
  m_attribute = x;
  return static_cast<Type>(epoch_core::lookup(
      attribute_map, m_attribute, "Invalid Price : " + m_attribute));
}

std::string BarAttribute::fromType(Type type) {
  return attribute_map_reverse.at(type);
}

BarAttribute &BarAttribute::operator=(std::string const &_ts) {
  *this = BarAttribute(_ts);
  return *this;
}

BarAttribute &BarAttribute::operator=(const char *_ts) {
  *this = BarAttribute(_ts);
  return *this;
}

bool BarAttribute::isValidBarAttribute(const std::string &string) {
  return attribute_map.contains(string);
}

std::map<std::string, BarAttribute::Type> BarAttribute::initAttribute_map() {
  std::map<std::string, Type> map;
  map["c"] = Close;
  map["o"] = Open;
  map["h"] = High;
  map["l"] = Low;
  map["bp"] = Bid;
  map["ap"] = Ask;
  map["bv"] = BidVolume;
  map["av"] = AskVolume;
  map["v"] = Volume;
  map["t"] = TimeStamp;
  map["p"] = Price;
  map["s"] = Contract;
  map["u"] = Underlying;
  map["oi"] = OpenInterest;
  map["delta"] = Delta;
  map["gamma"] = Gamma;
  map["iv"] = ImpliedVolatility;
  map["vega"] = Vega;
  map["theta"] = Theta;
  map["rho"] = Rho;
  return map;
}

std::map<BarAttribute::Type, std::string>
BarAttribute::initattribute_map_reverse() {
  std::map<Type, std::string> map;
  auto items = initAttribute_map();
  std::transform(
      items.begin(), items.end(), std::inserter(map, std::end(map)),
      [](auto const &item) { return std::pair{item.second, item.first}; });
  return map;
}

} // namespace data_sdk