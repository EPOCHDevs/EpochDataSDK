#pragma once

#include <functional>
#include <optional>
#include <string>
#include <vector>
#include <expected>

namespace data_sdk::trading_economics {

struct HttpError;
template <typename T> using Expected = std::expected<T, HttpError>;

// Options for configuring Trading Economics API clients
struct Options {
  // Trading Economics API key (required)
  std::string api_key;

  // Base URL for Trading Economics API (default: https://api.tradingeconomics.com)
  std::string base_url = "https://api.tradingeconomics.com";

  // Request timeout in seconds (default: 30 seconds)
  double request_timeout_sec = 30.0;

  // User agent string for HTTP requests
  std::string user_agent = "EpochDataSDK-TradingEconomics/1.0";

  // Use Drogon's main event loop (default: false, creates own thread)
  bool use_drogon_main_loop = false;

  // HTTP GET override for testing (optional)
  std::function<Expected<std::string>(
      const std::string&,
      const std::vector<std::pair<std::string, std::string>>&)>
      http_get_override;

  // Maximum number of retries for failed requests (default: 3)
  int max_retries = 3;

  // Enable verbose logging for debugging (default: false)
  bool verbose = false;
};

} // namespace data_sdk::trading_economics
