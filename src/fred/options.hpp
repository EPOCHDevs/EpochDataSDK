#pragma once

#include <string>
#include <functional>
#include <utility>
#include <vector>
#include <expected>

#include "error.hpp"

namespace data_sdk::fred {

struct Options {
  std::string api_key;
  std::string base_url = "https://api.stlouisfed.org";
  std::string user_agent = "EpochStratifyX-FREDSDK/1.0";
  double connect_timeout_sec = 10.0;
  double request_timeout_sec = 10.0;

  // Test hooks (optional)
  // If set, HTTP calls use this hook instead of real network.
  std::function<std::expected<std::string, HttpError>(
      const std::string & /*path*/,
      const std::vector<std::pair<std::string, std::string>> & /*query*/)> http_get_override;

  // Event loop configuration
  bool use_drogon_main_loop = false;  // If true, use app().getLoop() instead of creating own
};

} // namespace data_sdk::fred
