#include "fred/base_client.hpp"

#include <chrono>
#include <sstream>
#include <thread>

#include <spdlog/spdlog.h>
#include <epoch_frame/dataframe.h>

#include "../common/event_loop_helper.hpp"
#include "fred/models.hpp"

namespace data_sdk::fred {

namespace {

std::string urlEncode(const std::string &value) {
  return drogon::utils::urlEncode(value);
}

} // namespace

BaseClient::BaseClient(Options options) : options_(std::move(options)) {
  // Use the shared helper to get an event loop
  auto* loop = data_sdk::common::EventLoopHelper::getEventLoop(
      options_.use_drogon_main_loop,
      "FREDBaseClient",
      loopThread_);

  httpClient_ = drogon::HttpClient::newHttpClient(options_.base_url, loop);
}

BaseClient::~BaseClient() {
  data_sdk::common::EventLoopHelper::quitEventLoopThread(loopThread_);
}

std::optional<int>
BaseClient::parseIntHeader(const drogon::HttpResponsePtr &resp,
                          const std::string &key) {
  if (!resp)
    return std::nullopt;
  const auto v = resp->getHeader(key);
  if (v.empty())
    return std::nullopt;
  try {
    return std::stoi(v);
  } catch (const std::invalid_argument& e) {
    SPDLOG_DEBUG("Invalid integer in header '{}': '{}' - {}", key, v, e.what());
    return std::nullopt;
  } catch (const std::out_of_range& e) {
    SPDLOG_WARN("Integer overflow in header '{}': '{}' - {}", key, v, e.what());
    return std::nullopt;
  }
}

std::string BaseClient::buildQueryString(
    const std::vector<std::pair<std::string, std::string>> &query) {
  if (query.empty())
    return {};
  std::ostringstream oss;
  bool first = true;
  for (const auto &[k, v] : query) {
    oss << (first ? '?' : '&') << urlEncode(k) << '=' << urlEncode(v);
    first = false;
  }
  return oss.str();
}

auto BaseClient::httpAsyncGet(
    std::string path,
    std::vector<std::pair<std::string, std::string>> query) const
    -> drogon::Task<Expected<std::string>> {
  // Parameters are passed by value to avoid coroutine lifetime issues

  if (options_.http_get_override)
    co_return options_.http_get_override(path, query);

  auto client = httpClient_
                    ? httpClient_
                    : drogon::HttpClient::newHttpClient(options_.base_url);
  auto req = drogon::HttpRequest::newHttpRequest();
  req->setMethod(drogon::Get);
  req->addHeader("User-Agent", options_.user_agent);
  req->addHeader("Accept", "application/json");

  query.emplace_back("api_key", options_.api_key);
  query.emplace_back("file_type", "json");
  req->setPath(path + buildQueryString(query));

  try {
    auto resp =
        co_await client->sendRequestCoro(req, options_.request_timeout_sec);
    if (!resp) {
      co_return makeError<std::string>(0, "Network error or no response", resp);
    }

    const auto status = static_cast<int>(resp->getStatusCode());
    if (status < 200 || status >= 300) {
      co_return makeError<std::string>(status, resp->getBody(), resp);
    }

    co_return std::string(resp->getBody());
  } catch (const std::exception &e) {
    co_return makeError<std::string>(0, e.what(), nullptr);
  }
}

drogon::Task<Expected<std::string>> BaseClient::httpAsyncGetWithRetry(
    std::string path,
    std::vector<std::pair<std::string, std::string>> query,
    int max_retries) const {
  // Parameters are passed by value to avoid coroutine lifetime issues

  for (int attempt = 0; attempt <= max_retries; ++attempt) {
    auto result = co_await httpAsyncGet(path, query);

    if (result) {
      co_return result; // Success
    }

    // Don't retry client errors (4xx) - these won't succeed on retry
    const int status = result.error().http_status;
    if (status >= 400 && status < 500) {
      SPDLOG_WARN("FRED HTTP client error (won't retry): path={} status={} error={}",
                  path, status, result.error().message);
      co_return result;
    }

    // Log retry attempt for transient errors (5xx, network errors)
    if (attempt < max_retries) {
      const auto delay_ms =
          100 * (1 << attempt); // exponential backoff: 100, 200, 400ms
      SPDLOG_WARN(
          "FRED HTTP retry {}/{} for path={} error={} retrying in {}ms",
          attempt + 1, max_retries, path, result.error().message, delay_ms);

      // Async sleep using drogon timer
      co_await drogon::sleepCoro(
          httpClient_ ? httpClient_->getLoop()
                      : drogon::app().getLoop(),
          std::chrono::milliseconds(delay_ms));
    } else {
      SPDLOG_ERROR("FRED HTTP failed after {} retries for path={} error={}",
                   max_retries, path, result.error().message);
      co_return result; // Return the final error
    }
  }

  // Should never reach here
  co_return makeError<std::string>(0, "Unexpected retry loop exit", nullptr);
}

Expected<std::string> BaseClient::httpGetWithRetry(
    const std::string &path,
    const std::vector<std::pair<std::string, std::string>> &query,
    int max_retries) const {
  for (int attempt = 0; attempt <= max_retries; ++attempt) {
    auto result = httpGet(path, query);
    if (result) {
      return result;
    }

    // Don't retry client errors (4xx) - these won't succeed on retry
    const int status = result.error().http_status;
    if (status >= 400 && status < 500) {
      SPDLOG_WARN("FRED HTTP client error (won't retry): path={} status={} error={}",
                  path, status, result.error().message);
      return result;
    }

    // Log retry attempt for transient errors (5xx, network errors)
    if (attempt < max_retries) {
      const auto delay_ms =
          100 * (1 << attempt); // exponential backoff: 100, 200, 400ms
      SPDLOG_WARN(
          "FRED HTTP retry {}/{} for path={} error={} retrying in {}ms",
          attempt + 1, max_retries, path, result.error().message, delay_ms);
      std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
    } else {
      SPDLOG_ERROR("FRED HTTP failed after {} retries for path={} error={}",
                   max_retries, path, result.error().message);
      return result; // Return the final error
    }
  }
  std::unreachable();
}

template <typename T>
Expected<T> BaseClient::makeError(int status, std::string_view message,
                                  const drogon::HttpResponsePtr &resp) {
  HttpError e;
  e.http_status = status;
  e.message = std::string(message);
  if (resp) {
    e.request_id = resp->getHeader("X-Request-Id");
    if (e.request_id.empty()) {
      e.request_id = resp->getHeader("Request-Id");
    }
    const auto rem = resp->getHeader("X-RateLimit-Remaining");
    const auto lim = resp->getHeader("X-RateLimit-Limit");
    if (!rem.empty()) {
      try {
        e.rate_limit_remaining = std::stoi(rem);
      } catch (const std::exception& ex) {
        SPDLOG_DEBUG("Failed to parse rate limit remaining header '{}': {}", rem, ex.what());
      }
    }
    if (!lim.empty()) {
      try {
        e.rate_limit_limit = std::stoi(lim);
      } catch (const std::exception& ex) {
        SPDLOG_DEBUG("Failed to parse rate limit limit header '{}': {}", lim, ex.what());
      }
    }
  }
  return std::unexpected(std::move(e));
}

// Explicit template instantiations for commonly used types
template Expected<std::string> BaseClient::makeError(int, std::string_view, const drogon::HttpResponsePtr &);
template Expected<epoch_frame::DataFrame> BaseClient::makeError(int, std::string_view, const drogon::HttpResponsePtr &);
template Expected<SeriesObservationsResponse> BaseClient::makeError(int, std::string_view, const drogon::HttpResponsePtr &);

} // namespace data_sdk::fred
