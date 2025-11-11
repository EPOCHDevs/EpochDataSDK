#pragma once

#include <functional>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <expected>

#include "error.hpp"

namespace data_sdk::sec {

template <typename T> using Expected = std::expected<T, HttpError>;

struct Options {
  std::string api_key;
  std::string base_url = "https://api.sec-api.io";

  // Rate limiting options
  bool enable_rate_limiting = true;
  double max_requests_per_second = 10.0;
  std::optional<size_t> rate_limit_burst_capacity = std::nullopt;

  // Event loop configuration
  bool use_drogon_main_loop = false;

  // HTTP retry configuration
  int max_retries = 3;
  double retry_delay_seconds = 0.5;

  // Override for testing
  std::function<Expected<std::string>(
      const std::string &path,
      const std::vector<std::pair<std::string, std::string>> &query)>
      http_get_override;

  std::function<Expected<std::string>(
      const std::string &path,
      const std::string &body,
      const std::vector<std::pair<std::string, std::string>> &query)>
      http_post_override;
};

} // namespace data_sdk::sec
