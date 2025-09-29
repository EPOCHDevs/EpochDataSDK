#pragma once

#include <string>
#include <functional>
#include <utility>
#include <vector>
#include <expected>

#include "enums.hpp"
#include "error.hpp"

namespace data_sdk::polygon {

struct Options {
  std::string api_key;
  std::string base_url = "https://api.polygon.io";
  std::string user_agent = "EpochStratifyX-PolygonSDK/1.0";
  double connect_timeout_sec = 10.0;
  double request_timeout_sec = 10.0;

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
