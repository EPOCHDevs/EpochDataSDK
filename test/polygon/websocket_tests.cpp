#include "epoch_data_sdk/polygon/options.hpp"
#include "epoch_data_sdk/polygon/websocket_client.hpp"
#include <catch2/catch_all.hpp>

using namespace data_sdk::polygon;

TEST_CASE("websocket auth+subscribe frames in test mode", "[.polygon][ws]") {
  Options opt;
  opt.api_key = "k";
  opt.ws_test_mode = true;
  std::vector<std::string> sent;
  opt.ws_send_sink = [&](const std::string &f) { sent.push_back(f); };

  WebSocketClient ws(opt);
  bool opened = false;
  ws.setOnOpen([&] { opened = true; });
  ws.connect();
  REQUIRE(opened);
  REQUIRE_FALSE(sent.empty());
  REQUIRE(sent.front().find("\"auth\"") != std::string::npos);

  ws.subscribe({"T.SPY", "Q.SPY"});
  REQUIRE(sent.back().find("subscribe") != std::string::npos);
  ws.unsubscribe({"T.SPY"});
  REQUIRE(sent.back().find("unsubscribe") != std::string::npos);
}
