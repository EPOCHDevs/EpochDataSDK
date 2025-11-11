#pragma once

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <drogon/drogon.h>
#include <epoch_frame/dataframe.h>
#include <expected>
#include <trantor/net/EventLoopThread.h>

#include "trading_economics/error.hpp"
#include "trading_economics/options.hpp"

namespace data_sdk::trading_economics {

template <typename T> using Expected = std::expected<T, HttpError>;

// BaseClient - Shared HTTP and DataFrame conversion logic for all Trading Economics clients
// Handles:
// - HTTP GET requests with Drogon
// - Event loop management (thread-safe operations)
// - Query string building
// - Error handling and retries
// - JSON to DataFrame conversion
class BaseClient {
public:
  explicit BaseClient(Options options);
  virtual ~BaseClient();

  // Prevent copying (due to unique_ptr members)
  BaseClient(const BaseClient&) = delete;
  BaseClient& operator=(const BaseClient&) = delete;

  // HTTP GET request with query parameters
  // Returns raw JSON string on success, HttpError on failure
  Expected<std::string>
  httpGet(const std::string& path,
          const std::map<std::string, std::string>& params) const;

  // HTTP GET request that directly returns a DataFrame
  // Automatically converts JSON response to DataFrame
  Expected<epoch_frame::DataFrame>
  httpGetDataFrame(const std::string& path,
                   const std::map<std::string, std::string>& params) const;

  // Build URL query string from parameters
  // Example: {"country": "United States", "indicator": "GDP"}
  //       -> "country=United%20States&indicator=GDP"
  static std::string buildQueryString(const std::map<std::string, std::string>& params);

  // Convert JSON string to DataFrame
  // Uses glaze for JSON parsing, then constructs DataFrame with appropriate columns
  static Expected<epoch_frame::DataFrame> jsonToDataFrame(const std::string& json_str);

  // Create an HttpError from status and message
  template <typename T>
  static Expected<T> makeError(int status, std::string_view message,
                               const drogon::HttpResponsePtr& resp = nullptr) {
    std::string details;
    if (resp) {
      details = "Request-ID: " + resp->getHeader("X-Request-Id");
    }
    return std::unexpected(HttpError(status, std::string(message), details));
  }

private:
  Options options_;
  std::unique_ptr<trantor::EventLoopThread> loopThread_;
  drogon::HttpClientPtr httpClient_;
};

} // namespace data_sdk::trading_economics
