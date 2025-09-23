#pragma once

#include <functional>
#include <string>
#include <vector>

#include <drogon/WebSocketClient.h>

#include "options.hpp"

namespace data_sdk::polygon {

class WebSocketClient final {
public:
  using MessageHandler = std::function<void(const std::string &raw)>;
  using OpenHandler = std::function<void()>;
  using CloseHandler = std::function<void()>;
  using ErrorHandler = std::function<void(const std::string &)>;

  explicit WebSocketClient(Options options);

  void setOnOpen(OpenHandler cb) { on_open_ = std::move(cb); }
  void setOnClose(CloseHandler cb) { on_close_ = std::move(cb); }
  void setOnMessage(MessageHandler cb) { on_message_ = std::move(cb); }
  void setOnError(ErrorHandler cb) { on_error_ = std::move(cb); }

  void connect();
  void close();
  void subscribe(const std::vector<std::string> &channels);
  void unsubscribe(const std::vector<std::string> &channels);

private:
private:
  Options options_;
  trantor::EventLoop loop_;
  drogon::WebSocketClientPtr client_;
  drogon::WebSocketConnectionPtr conn_;

  OpenHandler on_open_;
  CloseHandler on_close_;
  MessageHandler on_message_;
  ErrorHandler on_error_;
};

} // namespace data_sdk::polygon
