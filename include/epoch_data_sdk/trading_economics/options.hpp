#pragma once

#include <optional>
#include <string>

namespace data_sdk::trading_economics {

// Options for configuring Trading Economics API clients
struct Options {
  // Trading Economics API key (required)
  std::string api_key;

  // Base URL for Trading Economics API (default: https://api.tradingeconomics.com)
  std::string base_url = "https://api.tradingeconomics.com";

  // Request timeout in milliseconds (default: 30 seconds)
  int timeout_ms = 30000;

  // Maximum number of retries for failed requests (default: 3)
  int max_retries = 3;

  // Enable verbose logging for debugging (default: false)
  bool verbose = false;
};

} // namespace data_sdk::trading_economics
