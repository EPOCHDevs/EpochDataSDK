#pragma once

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <drogon/drogon.h>
#include <expected>
#include <trantor/net/EventLoopThread.h>

#include "error.hpp"
#include "options.hpp"

namespace data_sdk::fred {

template <typename T> using Expected = std::expected<T, HttpError>;

// BaseClient - Private base class containing shared HTTP functionality
// This class is NOT exposed in the public API (not in include/)
class BaseClient {
protected:
  explicit BaseClient(Options options);
  virtual ~BaseClient();

  // HTTP request methods
  drogon::Task<Expected<std::string>> httpAsyncGet(
      const std::string &path,
      const std::vector<std::pair<std::string, std::string>> &query) const;

  Expected<std::string>
  httpGet(const std::string &path,
          const std::vector<std::pair<std::string, std::string>> &query) const {
    return drogon::sync_wait(httpAsyncGet(path, query));
  }

  Expected<std::string> httpGetWithRetry(
      const std::string &path,
      const std::vector<std::pair<std::string, std::string>> &query,
      int max_retries) const;

  // Helper methods
  static std::string buildQueryString(
      const std::vector<std::pair<std::string, std::string>> &query);

  static std::optional<int> parseIntHeader(const drogon::HttpResponsePtr &resp,
                                           const std::string &key);

  // Error creation helper
  template <typename T>
  static Expected<T> makeError(int status, std::string_view message,
                               const drogon::HttpResponsePtr &resp);

  // Options accessor for derived classes
  const Options &options() const { return options_; }

private:
  Options options_;
  // Dedicated event loop and HTTP client bound to it
  std::unique_ptr<trantor::EventLoopThread> loopThread_;
  drogon::HttpClientPtr httpClient_;
};

} // namespace data_sdk::fred
