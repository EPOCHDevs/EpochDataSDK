#pragma once

#include <expected>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "error.hpp"

namespace data_sdk::tradingeconomics {

struct Options {
  std::string api_key;
  std::string base_url = "https://api.tradingeconomics.com";
  std::string user_agent = "EpochStratifyX-TE-SDK/1.0";
  double connect_timeout_sec = 10.0;
  double request_timeout_sec = 10.0;

  // Test hook to override HTTP GET for unit tests
  std::function<std::expected<std::string, HttpError>(
      const std::string & /*path*/,
      const std::vector<std::pair<std::string, std::string>> & /*query*/)>
      http_get_override;

  // Event loop configuration (shared with PolygonClient)
  bool use_drogon_main_loop = false;  // If true, use app().getLoop() instead of creating own
};

} // namespace data_sdk::tradingeconomics
