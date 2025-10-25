#pragma once

#include <string>
#include <functional>
#include <utility>
#include <vector>
#include <expected>
#include <optional>

#include "enums.hpp"
#include "error.hpp"

namespace data_sdk::polygon {

struct Options {
  std::string api_key;
  std::string base_url = "https://api.polygon.io";
  std::string user_agent = "EpochStratifyX-PolygonSDK/1.0";
  double connect_timeout_sec = 10.0;
  double request_timeout_sec = 10.0;

  // Rate limiting configuration
  // Set to false to disable rate limiting (default: true)
  bool enable_rate_limiting = true;
  // Maximum requests per second (default: 100 for Polygon paid tier)
  // Set to 5.0/60.0 (0.083) for free tier (5 requests per minute)
  double max_requests_per_second = 100.0;
  // Burst capacity - allows short bursts above rate limit (default: 2x max_requests_per_second)
  std::optional<size_t> rate_limit_burst_capacity = std::nullopt;

  // WebSocket settings
  std::string websocket_base_url = "wss://socket.polygon.io";
  epoch_core::WebSocketFeed websocket_feed = epoch_core::WebSocketFeed::Stocks;

  // Test hooks (optional)
  // If set, HTTP calls use this hook instead of real network.
  std::function<std::expected<std::string, HttpError>(
      const std::string & /*path*/,
      const std::vector<std::pair<std::string, std::string>> & /*query*/)> http_get_override;

  // WebSocket testing: if true, don't open a real socket.
  bool ws_test_mode = false;
  // If set, capture outbound WS frames here.
  std::function<void(const std::string &)> ws_send_sink;

  // Event loop configuration
  bool use_drogon_main_loop = false;  // If true, use app().getLoop() instead of creating own
};

} // namespace data_sdk::polygon
