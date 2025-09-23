#include "websocket_client.hpp"

#include <sstream>

namespace data_sdk::polygon {

WebSocketClient::WebSocketClient(Options options)
    : options_(std::move(options)) {}

void WebSocketClient::connect() {
  if (options_.ws_test_mode) {
    if (on_open_)
      on_open_();
    std::ostringstream oss;
    oss << "{\"action\":\"auth\",\"params\":\"" << options_.api_key << "\"}";
    if (options_.ws_send_sink)
      options_.ws_send_sink(oss.str());
    return;
  }
  const std::string url =
      options_.websocket_base_url + "/" +
      epoch_core::WebSocketFeedWrapper::ToString(options_.websocket_feed);
  client_ = drogon::WebSocketClient::newWebSocketClient(url, &loop_);

  client_->setMessageHandler([this](std::string &&message,
                                    const drogon::WebSocketClientPtr &,
                                    const drogon::WebSocketMessageType &type) {
    if (type == drogon::WebSocketMessageType::Text) {
      if (on_message_)
        on_message_(message);
    }
  });

  client_->setConnectionClosedHandler(
      [this](const drogon::WebSocketClientPtr &) {
        if (on_close_)
          on_close_();
      });

  client_->connectToServer(
      drogon::HttpRequest::newHttpRequest(),
      [this](drogon::ReqResult r, const drogon::HttpResponsePtr &resp,
             const drogon::WebSocketClientPtr &wsPtr) {
        if (r != drogon::ReqResult::Ok) {
          if (on_error_)
            on_error_("connect failed");
          return;
        }
        (void)resp;
        conn_ = wsPtr->getConnection();
        if (on_open_)
          on_open_();
        // auth on open
        std::ostringstream oss;
        oss << "{\"action\":\"auth\",\"params\":\"" << options_.api_key
            << "\"}";
        conn_->send(oss.str(), drogon::WebSocketMessageType::Text);
      });
}

void WebSocketClient::close() {
  if (conn_) {
    auto c = conn_;
    conn_.reset();
    c->shutdown(drogon::CloseCode::kNormalClosure, "bye");
  }
}

void WebSocketClient::subscribe(const std::vector<std::string> &channels) {
  std::string joined;
  for (size_t i = 0; i < channels.size(); ++i) {
    if (i)
      joined += ",";
    joined += channels[i];
  }
  std::ostringstream oss;
  oss << "{\"action\":\"subscribe\",\"params\":\"" << joined << "\"}";
  if (options_.ws_test_mode) {
    if (options_.ws_send_sink)
      options_.ws_send_sink(oss.str());
    return;
  }
  conn_->send(oss.str(), drogon::WebSocketMessageType::Text);
}

void WebSocketClient::unsubscribe(const std::vector<std::string> &channels) {
  std::string joined;
  for (size_t i = 0; i < channels.size(); ++i) {
    if (i)
      joined += ",";
    joined += channels[i];
  }
  std::ostringstream oss;
  oss << "{\"action\":\"unsubscribe\",\"params\":\"" << joined << "\"}";
  if (options_.ws_test_mode) {
    if (options_.ws_send_sink)
      options_.ws_send_sink(oss.str());
    return;
  }
  conn_->send(oss.str(), drogon::WebSocketMessageType::Text);
}

} // namespace data_sdk::polygon
