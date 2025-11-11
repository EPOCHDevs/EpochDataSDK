#pragma once

#include <map>
#include <chrono>

namespace data_sdk {

using chrono_month = std::chrono::month;

struct FuturesConstants {
  static const FuturesConstants& instance() {
    static FuturesConstants instance;
    return instance;
  }

  inline static const std::map<char, chrono_month> month_mapping{
      {'F', std::chrono::January},   {'G', std::chrono::February},
      {'H', std::chrono::March},     {'J', std::chrono::April},
      {'K', std::chrono::May},       {'M', std::chrono::June},
      {'N', std::chrono::July},      {'Q', std::chrono::August},
      {'U', std::chrono::September}, {'V', std::chrono::October},
      {'X', std::chrono::November},  {'Z', std::chrono::December}};
};

} // namespace data_sdk
